package wrpctcp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	wrpc "wrpc.io/go"
)

// Error types
var (
	ErrConnectionFailed  = errors.New("connection failed")
	ErrConnectionTimeout = errors.New("connection timeout")
	ErrProtocolViolation = errors.New("protocol violation")
	ErrFrameTooLarge     = errors.New("frame too large")
	ErrInvalidLEB128     = errors.New("invalid LEB128 encoding")
	ErrBufferOverflow    = errors.New("buffer overflow")
	ErrWriteFailed       = errors.New("write failed")
	ErrReadFailed        = errors.New("read failed")
	ErrInvalidPath       = errors.New("invalid path")
	ErrInvalidHeader     = errors.New("invalid header")
)

// Header represents metadata that can be passed with requests
type Header map[string][]string

type headerKey struct{}

// HeaderFromContext extracts a Header from a context
func HeaderFromContext(ctx context.Context) (Header, bool) {
	v, ok := ctx.Value(headerKey{}).(Header)
	return v, ok
}

// ContextWithHeader adds a Header to a context
func ContextWithHeader(ctx context.Context, header Header) context.Context {
	return context.WithValue(ctx, headerKey{}, header)
}

// ClientConfig contains configuration options for the TCP client
type ClientConfig struct {
	// Maximum frame size in bytes
	MaxFrameSize int

	// Connection timeout in milliseconds
	ConnectionTimeout time.Duration

	// Read timeout in milliseconds
	ReadTimeout time.Duration

	// Write timeout in milliseconds
	WriteTimeout time.Duration

	// Maximum depth of nested paths
	MaxPathDepth int
}

// DefaultClientConfig returns the default client configuration
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		MaxFrameSize:      10 * 1024 * 1024, // 10MB
		ConnectionTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		MaxPathDepth:      32,
	}
}

// ClientOption is a function that configures a Client
type ClientOption func(*Client)

// WithMaxFrameSize sets the maximum frame size
func WithMaxFrameSize(size int) ClientOption {
	return func(c *Client) {
		c.config.MaxFrameSize = size
	}
}

// WithConnectionTimeout sets the connection timeout
func WithConnectionTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.ConnectionTimeout = timeout
	}
}

// WithReadTimeout sets the read timeout
func WithReadTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.ReadTimeout = timeout
	}
}

// WithWriteTimeout sets the write timeout
func WithWriteTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.WriteTimeout = timeout
	}
}

// WithMaxPathDepth sets the maximum path depth
func WithMaxPathDepth(depth int) ClientOption {
	return func(c *Client) {
		c.config.MaxPathDepth = depth
	}
}

// WithPrefix sets a prefix for this Client
func WithPrefix(prefix string) ClientOption {
	return func(c *Client) {
		c.prefix = prefix
	}
}

// WithGroup sets a queue group for this Client
func WithGroup(group string) ClientOption {
	return func(c *Client) {
		c.group = group
	}
}

// Client is a TCP client that implements the wrpc.Invoker and wrpc.Server interfaces
type Client struct {
	addr             string
	prefix           string
	group            string
	config           ClientConfig
	activeConns      map[net.Conn]struct{}
	activeConnsMutex sync.Mutex
}

// NewClient creates a new TCP client with the given address and options
func NewClient(addr string, opts ...ClientOption) *Client {
	c := &Client{
		addr:        addr,
		config:      DefaultClientConfig(),
		activeConns: make(map[net.Conn]struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Buffer pool for reusing buffers
var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

func putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

// paramWriter implements wrpc.IndexWriteCloser for writing parameters to the server
type paramWriter struct {
	ctx      context.Context
	conn     *net.TCPConn
	path     []uint32
	buffer   *bytes.Buffer
	mutex    sync.Mutex
	parent   *paramWriter
	children map[string]*paramWriter
	refCount *atomic.Int32
}

// Write implements the io.Writer interface
func (w *paramWriter) Write(p []byte) (int, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Check if context is done
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
	}

	// Write the data to the buffer
	n, err := w.buffer.Write(p)
	if err != nil {
		return 0, err
	}

	// If the buffer is large enough, flush it
	if w.buffer.Len() >= 4096 {
		if err := w.flush(); err != nil {
			return 0, err
		}
	}

	return n, nil
}

// WriteByte implements the io.ByteWriter interface
func (w *paramWriter) WriteByte(b byte) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Check if context is done
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
	}

	err := w.buffer.WriteByte(b)
	if err != nil {
		return err
	}

	// If the buffer is large enough, flush it
	if w.buffer.Len() >= 4096 {
		if err := w.flush(); err != nil {
			return err
		}
	}

	return nil
}

// Index implements the wrpc.Index interface
func (w *paramWriter) Index(path ...uint32) (wrpc.IndexWriteCloser, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Create a path key for tracking
	pathKey := pathToString(path)

	// Check if we already have this child
	if child, exists := w.children[pathKey]; exists {
		return child, nil
	}

	// Create a new parameter writer with the extended path
	newPath := make([]uint32, len(w.path)+len(path))
	copy(newPath, w.path)
	copy(newPath[len(w.path):], path)

	// Increment reference count
	w.refCount.Add(1)

	child := &paramWriter{
		ctx:      w.ctx,
		conn:     w.conn,
		path:     newPath,
		buffer:   getBuffer(),
		parent:   w,
		children: make(map[string]*paramWriter),
		refCount: w.refCount,
	}

	// Store the child
	w.children[pathKey] = child

	return child, nil
}

// Close implements the io.Closer interface
func (w *paramWriter) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Flush any remaining data
	if w.buffer.Len() > 0 {
		if err := w.flush(); err != nil {
			return err
		}
	}

	// Send an empty frame to signal the end of the stream
	frame, err := encodeFrame(w.path, nil)
	if err != nil {
		return fmt.Errorf("failed to encode end-of-stream frame: %w", err)
	}

	if _, err := w.conn.Write(frame); err != nil {
		return fmt.Errorf("failed to send end-of-stream frame: %w", err)
	}

	// Return the buffer to the pool
	putBuffer(w.buffer)

	// Remove from parent's children if we have a parent
	if w.parent != nil {
		for k, child := range w.parent.children {
			if child == w {
				delete(w.parent.children, k)
				break
			}
		}
	}

	// Decrement reference count
	refs := w.refCount.Add(-1)
	slog.DebugContext(w.ctx, "closed parameter writer",
		"path", w.path,
		"refs", refs)

	// Don't close the connection here, let the resultReader close it
	if refs == 0 && w.parent == nil && w.conn != nil {
		slog.DebugContext(w.ctx, "last parameter writer closed, but not closing connection")
	}

	return nil
}

// flush writes the buffered data to the connection
func (w *paramWriter) flush() error {
	// Encode the frame
	frame, err := encodeFrame(w.path, w.buffer.Bytes())
	if err != nil {
		return fmt.Errorf("failed to encode frame: %w", err)
	}

	// Write the frame to the connection
	if _, err := w.conn.Write(frame); err != nil {
		return fmt.Errorf("failed to write frame: %w", err)
	}

	// Reset the buffer
	w.buffer.Reset()

	return nil
}

// resultReader implements wrpc.IndexReadCloser for reading results from the server
type resultReader struct {
	ctx      context.Context
	conn     *net.TCPConn
	path     []uint32
	buffer   *bytes.Buffer
	mutex    sync.Mutex
	parent   *resultReader
	children map[string]*resultReader
	refCount *atomic.Int32
}

// Read implements the io.Reader interface
func (r *resultReader) Read(p []byte) (int, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Check if context is done
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
	}

	// If there's data in the buffer, return it
	if r.buffer.Len() > 0 {
		return r.buffer.Read(p)
	}

	// Read a frame from the connection
	if err := r.readNextFrame(); err != nil {
		return 0, err
	}

	// If the buffer is still empty after reading a frame, it means we got an empty frame (EOF)
	if r.buffer.Len() == 0 {
		return 0, io.EOF
	}

	// Read from the buffer
	return r.buffer.Read(p)
}

// ReadByte implements the io.ByteReader interface
func (r *resultReader) ReadByte() (byte, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Check if context is done
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
	}

	// If there's data in the buffer, return it
	if r.buffer.Len() > 0 {
		return r.buffer.ReadByte()
	}

	// Read a frame from the connection
	if err := r.readNextFrame(); err != nil {
		return 0, err
	}

	// If the buffer is still empty after reading a frame, it means we got an empty frame (EOF)
	if r.buffer.Len() == 0 {
		return 0, io.EOF
	}

	// Read from the buffer
	return r.buffer.ReadByte()
}

// pathToString converts a path to a string for use as a map key
func pathToString(path []uint32) string {
	var result string
	for i, p := range path {
		if i > 0 {
			result += "."
		}
		result += fmt.Sprintf("%d", p)
	}
	return result
}

// Index implements the wrpc.Index interface
func (r *resultReader) Index(path ...uint32) (wrpc.IndexReadCloser, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Create a path key for tracking
	pathKey := pathToString(path)

	// Check if we already have this child
	if child, exists := r.children[pathKey]; exists {
		return child, nil
	}

	// Create a new result reader with the extended path
	newPath := make([]uint32, len(r.path)+len(path))
	copy(newPath, r.path)
	copy(newPath[len(r.path):], path)

	// Increment reference count
	r.refCount.Add(1)

	child := &resultReader{
		ctx:      r.ctx,
		conn:     r.conn,
		path:     newPath,
		buffer:   getBuffer(),
		parent:   r,
		children: make(map[string]*resultReader),
		refCount: r.refCount,
	}

	// Store the child
	r.children[pathKey] = child

	return child, nil
}

// Close implements the io.Closer interface
func (r *resultReader) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Return the buffer to the pool
	putBuffer(r.buffer)

	// Remove from parent's children if we have a parent
	if r.parent != nil {
		for k, child := range r.parent.children {
			if child == r {
				delete(r.parent.children, k)
				break
			}
		}
	}

	// Decrement reference count
	refs := r.refCount.Add(-1)
	slog.DebugContext(r.ctx, "closed result reader",
		"path", r.path,
		"refs", refs)

	// If this is the last reference, close the connection
	if refs == 0 && r.parent == nil && r.conn != nil {
		slog.DebugContext(r.ctx, "closing connection, last result reader closed")
		return r.conn.Close()
	}

	return nil
}

// readNextFrame reads the next frame from the connection
func (r *resultReader) readNextFrame() error {
	slog.DebugContext(r.ctx, "reading next frame", "path", r.path)
	// Read the frame header (2 bytes)
	header := make([]byte, 2)
	if _, err := io.ReadFull(r.conn, header); err != nil {
		return fmt.Errorf("failed to read frame header: %w", err)
	}

	// Check the protocol version
	if header[0] != 0 {
		return fmt.Errorf("unsupported protocol version: %d", header[0])
	}

	// Read the frame payload
	payloadSize := int(header[1])
	if payloadSize > 0 {
		payload := make([]byte, payloadSize)
		if _, err := io.ReadFull(r.conn, payload); err != nil {
			return fmt.Errorf("failed to read frame payload: %w", err)
		}

		// Write the payload to the buffer
		if _, err := r.buffer.Write(payload); err != nil {
			return fmt.Errorf("failed to write payload to buffer: %w", err)
		}
	}

	return nil
}

// encodeFrame encodes a frame into a byte buffer
func encodeFrame(path []uint32, data []byte) ([]byte, error) {
	// Calculate the maximum possible size of the encoded frame
	maxSize := 1 + // version
		binary.MaxVarintLen32 + // path length
		len(path)*binary.MaxVarintLen32 + // path elements
		binary.MaxVarintLen64 + // data length
		len(data) // data payload

	buf := make([]byte, 0, maxSize)

	// Write protocol version (0)
	buf = append(buf, 0)

	// Write path length
	buf = appendUleb128(buf, uint64(len(path)))

	// Write path elements
	for _, p := range path {
		buf = appendUleb128(buf, uint64(p))
	}

	// Write data length
	buf = appendUleb128(buf, uint64(len(data)))

	// Write data payload
	buf = append(buf, data...)

	return buf, nil
}

// appendUleb128 appends a LEB128-encoded unsigned integer to a byte slice
func appendUleb128(buf []byte, val uint64) []byte {
	for {
		b := byte(val & 0x7f)
		val >>= 7
		if val != 0 {
			b |= 0x80
		}
		buf = append(buf, b)
		if val == 0 {
			break
		}
	}
	return buf
}

// decodeFrame decodes a frame from a byte buffer
func decodeFrame(buf []byte) (path []uint32, data []byte, n int, err error) {
	if len(buf) == 0 {
		return nil, nil, 0, errors.New("empty buffer")
	}

	// Read protocol version
	if buf[0] != 0 {
		return nil, nil, 0, fmt.Errorf("unsupported protocol version: %d", buf[0])
	}
	pos := 1

	// Read path length
	pathLen, bytesRead := readUleb128(buf[pos:])
	if bytesRead <= 0 {
		return nil, nil, 0, errors.New("failed to read path length")
	}
	pos += bytesRead

	// Read path elements
	path = make([]uint32, pathLen)
	for i := uint64(0); i < pathLen; i++ {
		val, bytesRead := readUleb128(buf[pos:])
		if bytesRead <= 0 {
			return nil, nil, 0, errors.New("failed to read path element")
		}
		path[i] = uint32(val)
		pos += bytesRead
	}

	// Read data length
	dataLen, bytesRead := readUleb128(buf[pos:])
	if bytesRead <= 0 {
		return nil, nil, 0, errors.New("failed to read data length")
	}
	pos += bytesRead

	// Read data payload
	if pos+int(dataLen) > len(buf) {
		return nil, nil, 0, errors.New("buffer too small for data payload")
	}
	data = buf[pos : pos+int(dataLen)]
	pos += int(dataLen)

	return path, data, pos, nil
}

// readUleb128 reads a LEB128-encoded unsigned integer from a byte slice
func readUleb128(buf []byte) (uint64, int) {
	var val uint64
	var shift uint
	var pos int

	for pos < len(buf) {
		b := buf[pos]
		pos++

		val |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 63 {
			return 0, -1 // Overflow
		}
	}

	return val, pos
}

// createInvocationFrame creates a frame for the initial invocation
func createInvocationFrame(instance string, name string, buf []byte, header Header) ([]byte, error) {
	// Calculate the maximum possible size of the encoded frame
	maxSize := 1 + // version
		binary.MaxVarintLen32 + len(instance) + // instance name
		binary.MaxVarintLen32 + len(name) + // function name
		1 + // empty path length
		binary.MaxVarintLen32 + // header count
		binary.MaxVarintLen32 + len(buf) // parameter buffer

	// Add space for headers
	for k, vs := range header {
		maxSize += binary.MaxVarintLen32 + len(k) // key length and data
		maxSize += binary.MaxVarintLen32          // value count
		for _, v := range vs {
			maxSize += binary.MaxVarintLen32 + len(v) // value length and data
		}
	}

	frame := make([]byte, 0, maxSize)

	// Write protocol version (0)
	frame = append(frame, 0)

	// Write instance name length and data
	frame = appendUleb128(frame, uint64(len(instance)))
	frame = append(frame, instance...)

	// Write function name length and data
	frame = appendUleb128(frame, uint64(len(name)))
	frame = append(frame, name...)

	// Write empty path (length 0)
	frame = append(frame, 0)

	// Write headers
	frame = appendUleb128(frame, uint64(len(header)))
	for k, vs := range header {
		// Write key
		frame = appendUleb128(frame, uint64(len(k)))
		frame = append(frame, k...)

		// Write values
		frame = appendUleb128(frame, uint64(len(vs)))
		for _, v := range vs {
			frame = appendUleb128(frame, uint64(len(v)))
			frame = append(frame, v...)
		}
	}

	// Write parameter buffer length and data
	frame = appendUleb128(frame, uint64(len(buf)))
	frame = append(frame, buf...)

	return frame, nil
}

// Invoke implements the wrpc.Invoker interface
func (c *Client) Invoke(ctx context.Context, instance string, name string, buf []byte, paths ...wrpc.SubscribePath) (wrpc.IndexWriteCloser, wrpc.IndexReadCloser, error) {
	// Create a context with timeout for the connection
	connCtx, cancel := context.WithTimeout(ctx, c.config.ConnectionTimeout)
	defer cancel()

	// Resolve the address
	var d net.Dialer
	conn, err := d.DialContext(connCtx, "tcp", c.addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to %s: %w", c.addr, err)
	}

	// Convert to TCP connection
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		conn.Close()
		return nil, nil, errors.New("connection is not a TCP connection")
	}

	// Set read and write deadlines
	if err := tcpConn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout)); err != nil {
		tcpConn.Close()
		return nil, nil, fmt.Errorf("failed to set read deadline: %w", err)
	}

	if err := tcpConn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout)); err != nil {
		tcpConn.Close()
		return nil, nil, fmt.Errorf("failed to set write deadline: %w", err)
	}

	// Validate input parameters
	if len(instance) > math.MaxUint32 {
		tcpConn.Close()
		return nil, nil, errors.New("instance name too long")
	}

	if len(name) > math.MaxUint32 {
		tcpConn.Close()
		return nil, nil, errors.New("function name too long")
	}

	if len(buf) > c.config.MaxFrameSize {
		tcpConn.Close()
		return nil, nil, fmt.Errorf("parameter buffer too large (max: %d bytes)", c.config.MaxFrameSize)
	}

	// Extract headers from context if present
	var header Header
	if h, ok := HeaderFromContext(ctx); ok {
		header = h
	} else {
		header = make(Header)
	}

	// Create the invocation frame
	frame, err := createInvocationFrame(instance, name, buf, header)
	if err != nil {
		tcpConn.Close()
		return nil, nil, fmt.Errorf("failed to create invocation frame: %w", err)
	}

	// Send the invocation frame
	slog.Debug("sending invocation frame",
		"instance", instance,
		"function", name,
		"frame_size", len(frame))

	if _, err := tcpConn.Write(frame); err != nil {
		tcpConn.Close()
		return nil, nil, fmt.Errorf("failed to send invocation frame: %w", err)
	}

	// Don't close the write side of the connection yet
	// This is different from the original implementation which closed the write side immediately

	// Create parameter writer and result reader
	pwRefCount := &atomic.Int32{}
	pwRefCount.Add(1)

	pw := &paramWriter{
		ctx:      ctx,
		conn:     tcpConn,
		buffer:   getBuffer(),
		children: make(map[string]*paramWriter),
		refCount: pwRefCount,
	}

	rrRefCount := &atomic.Int32{}
	rrRefCount.Add(1)

	rr := &resultReader{
		ctx:      ctx,
		conn:     tcpConn,
		buffer:   getBuffer(),
		children: make(map[string]*resultReader),
		refCount: rrRefCount,
	}

	return pw, rr, nil
}

// decodeHeader decodes a header from a byte buffer
func decodeHeader(buf []byte) (Header, int, error) {
	header := make(Header)
	pos := 0

	// Read header count
	headerCount, bytesRead := readUleb128(buf[pos:])
	if bytesRead <= 0 {
		return nil, 0, errors.New("failed to read header count")
	}
	pos += bytesRead

	// Read headers
	for i := uint64(0); i < headerCount; i++ {
		// Read key
		keyLen, bytesRead := readUleb128(buf[pos:])
		if bytesRead <= 0 {
			return nil, 0, errors.New("failed to read header key length")
		}
		pos += bytesRead

		if pos+int(keyLen) > len(buf) {
			return nil, 0, errors.New("buffer too small for header key")
		}
		key := string(buf[pos : pos+int(keyLen)])
		pos += int(keyLen)

		// Read values
		valueCount, bytesRead := readUleb128(buf[pos:])
		if bytesRead <= 0 {
			return nil, 0, errors.New("failed to read header value count")
		}
		pos += bytesRead

		values := make([]string, valueCount)
		for j := uint64(0); j < valueCount; j++ {
			valueLen, bytesRead := readUleb128(buf[pos:])
			if bytesRead <= 0 {
				return nil, 0, errors.New("failed to read header value length")
			}
			pos += bytesRead

			if pos+int(valueLen) > len(buf) {
				return nil, 0, errors.New("buffer too small for header value")
			}
			values[j] = string(buf[pos : pos+int(valueLen)])
			pos += int(valueLen)
		}

		header[key] = values
	}

	return header, pos, nil
}

// handleMessage handles an incoming message from a TCP connection
func (c *Client) handleMessage(conn net.Conn, instance string, name string, f func(context.Context, wrpc.IndexWriteCloser, wrpc.IndexReadCloser), parentCtx context.Context) {
	// Use the parent context instead of creating a new background context
	ctx := parentCtx
	if ctx == nil {
		ctx = context.Background()
	}

	slog.DebugContext(ctx, "received invocation", "instance", instance, "name", name)

	// Convert to TCP connection
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		slog.Error("connection is not a TCP connection")
		conn.Close()
		return
	}

	// Set read and write deadlines
	if err := tcpConn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout)); err != nil {
		slog.Error("failed to set read deadline", "err", err)
		tcpConn.Close()
		return
	}

	if err := tcpConn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout)); err != nil {
		slog.Error("failed to set write deadline", "err", err)
		tcpConn.Close()
		return
	}

	// Read the invocation frame
	buf := make([]byte, 1024)
	n, err := tcpConn.Read(buf)
	if err != nil {
		slog.Error("failed to read invocation frame", "err", err)
		tcpConn.Close()
		return
	}

	// Decode the invocation frame
	if n > 0 {
		// Skip the protocol version, instance name, function name, and path
		pos := 1 // Skip protocol version

		// Skip instance name
		instLen, bytesRead := readUleb128(buf[pos:])
		if bytesRead <= 0 {
			slog.Error("failed to read instance name length")
			tcpConn.Close()
			return
		}
		pos += bytesRead + int(instLen)

		// Skip function name
		funcLen, bytesRead := readUleb128(buf[pos:])
		if bytesRead <= 0 {
			slog.Error("failed to read function name length")
			tcpConn.Close()
			return
		}
		pos += bytesRead + int(funcLen)

		// Skip path
		pathLen, bytesRead := readUleb128(buf[pos:])
		if bytesRead <= 0 {
			slog.Error("failed to read path length")
			tcpConn.Close()
			return
		}
		pos += bytesRead
		for i := uint64(0); i < pathLen; i++ {
			_, bytesRead := readUleb128(buf[pos:])
			if bytesRead <= 0 {
				slog.Error("failed to read path element")
				tcpConn.Close()
				return
			}
			pos += bytesRead
		}

		// Now we're at the headers section
		header, _, err := decodeHeader(buf[pos:])
		if err == nil && len(header) > 0 {
			// Add headers to context
			ctx = ContextWithHeader(ctx, header)
			slog.DebugContext(ctx, "decoded headers", "header", header)
		} else if err != nil {
			slog.Debug("failed to decode headers (might be normal for older clients)", "err", err)
		}
	}

	// Create parameter writer and result reader with reference counting
	pwRefCount := &atomic.Int32{}
	pwRefCount.Add(1)

	pw := &paramWriter{
		ctx:      ctx,
		conn:     tcpConn,
		buffer:   getBuffer(),
		children: make(map[string]*paramWriter),
		refCount: pwRefCount,
	}

	rrRefCount := &atomic.Int32{}
	rrRefCount.Add(1)

	rr := &resultReader{
		ctx:      ctx,
		conn:     tcpConn,
		buffer:   getBuffer(),
		children: make(map[string]*resultReader),
		refCount: rrRefCount,
	}

	// Call the handler function
	f(ctx, pw, rr)

	slog.DebugContext(ctx, "finished serving invocation")
}

// Serve implements the wrpc.Server interface
func (c *Client) Serve(ctx context.Context, instance string, name string, f func(context.Context, wrpc.IndexWriteCloser, wrpc.IndexReadCloser), paths ...wrpc.SubscribePath) (stop func() error, err error) {
	slog.DebugContext(ctx, "serving", "instance", instance, "name", name, "group", c.group)

	// Create a TCP listener
	addr := c.addr
	if addr == "" {
		addr = ":0" // Use any available port if not specified
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	// Update the client's address field with the actual listener address
	c.addr = listener.Addr().String()
	slog.DebugContext(ctx, "server listening", "addr", c.addr)

	// Start accepting connections in a goroutine
	stopCh := make(chan struct{})
	go func(ctx context.Context) {
		for {
			select {
			case <-stopCh:
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						return
					}
					slog.Error("failed to accept connection", "err", err)
					continue
				}

				// Track the connection
				c.activeConnsMutex.Lock()
				c.activeConns[conn] = struct{}{}
				c.activeConnsMutex.Unlock()

				// Handle the connection in a goroutine, passing the context
				go func(conn net.Conn) {
					c.handleMessage(conn, instance, name, f, ctx)

					// Remove the connection from tracking when done
					c.activeConnsMutex.Lock()
					delete(c.activeConns, conn)
					c.activeConnsMutex.Unlock()
				}(conn)
			}
		}
	}(ctx)

	// Return a function to stop the server
	return func() error {
		// Signal the accept loop to stop
		close(stopCh)

		// Close the listener
		err := listener.Close()

		// Close all active connections
		c.activeConnsMutex.Lock()
		for conn := range c.activeConns {
			conn.Close()
		}
		c.activeConns = make(map[net.Conn]struct{})
		c.activeConnsMutex.Unlock()

		return err
	}, nil
}
