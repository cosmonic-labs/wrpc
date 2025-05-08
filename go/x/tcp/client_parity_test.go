package wrpctcp

import (
	"context"
	"io"
	"net"
	"testing"

	wrpc "wrpc.io/go"
)

// TestNestedPaths tests that nested paths work correctly
func TestNestedPaths(t *testing.T) {
	// Use a random port that's guaranteed to be available
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}

	// Get the address and close the listener
	addr := listener.Addr().String()
	listener.Close()

	// Define a handler function that uses nested paths
	handler := func(ctx context.Context, w wrpc.IndexWriteCloser, r wrpc.IndexReadCloser) {
		// Create a nested writer
		nested, err := w.Index(1, 2, 3)
		if err != nil {
			t.Errorf("Failed to create nested writer: %v", err)
			return
		}

		// Write to the nested path
		if _, err := nested.Write([]byte("nested data")); err != nil {
			t.Errorf("Failed to write to nested path: %v", err)
			return
		}

		// Close the nested writer
		if err := nested.Close(); err != nil {
			t.Errorf("Failed to close nested writer: %v", err)
			return
		}

		// Close the main writer
		if err := w.Close(); err != nil {
			t.Errorf("Failed to close main writer: %v", err)
			return
		}
	}

	// Start serving
	ctx := context.Background()
	client := NewClient(addr)
	stop, err := client.Serve(ctx, "test", "hello", handler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer stop()

	// Create a client to connect to the server
	invokeClient := NewClient(listener.Addr().String())

	// Invoke the function
	pw, rr, err := invokeClient.Invoke(context.Background(), "test", "hello", []byte("request"))
	if err != nil {
		t.Fatalf("Failed to invoke function: %v", err)
	}

	// Create a nested reader
	nestedReader, err := rr.Index(1, 2, 3)
	if err != nil {
		t.Fatalf("Failed to create nested reader: %v", err)
	}

	// Read from the nested path
	buf := make([]byte, 1024)
	n, err := nestedReader.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read from nested path: %v", err)
	}

	if string(buf[:n]) != "nested data" {
		t.Fatalf("Expected nested data to be 'nested data', got '%s'", string(buf[:n]))
	}

	// Close the nested reader
	if err := nestedReader.Close(); err != nil {
		t.Fatalf("Failed to close nested reader: %v", err)
	}

	// Close the parameter writer
	if err := pw.Close(); err != nil {
		t.Fatalf("Failed to close parameter writer: %v", err)
	}
}

// TestResourceManagement tests that resources are properly managed and cleaned up
func TestResourceManagement(t *testing.T) {
	// Use a random port that's guaranteed to be available
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}

	// Get the address and close the listener
	addr := listener.Addr().String()
	listener.Close()

	// Define a handler function
	handler := func(ctx context.Context, w wrpc.IndexWriteCloser, r wrpc.IndexReadCloser) {
		// Create multiple nested writers
		for i := 0; i < 5; i++ {
			nested, err := w.Index(uint32(i))
			if err != nil {
				t.Errorf("Failed to create nested writer %d: %v", i, err)
				return
			}

			// Write to the nested path
			if _, err := nested.Write([]byte("data")); err != nil {
				t.Errorf("Failed to write to nested path %d: %v", i, err)
				return
			}

			// Close some of the nested writers
			if i%2 == 0 {
				if err := nested.Close(); err != nil {
					t.Errorf("Failed to close nested writer %d: %v", i, err)
					return
				}
			}
		}

		// Close the main writer
		if err := w.Close(); err != nil {
			t.Errorf("Failed to close main writer: %v", err)
			return
		}
	}

	// Start serving
	ctx := context.Background()
	client := NewClient(addr)
	stop, err := client.Serve(ctx, "test", "hello", handler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer stop()

	// Create a client to connect to the server
	invokeClient := NewClient(listener.Addr().String())

	// Invoke the function
	pw, rr, err := invokeClient.Invoke(context.Background(), "test", "hello", []byte("request"))
	if err != nil {
		t.Fatalf("Failed to invoke function: %v", err)
	}

	// Create multiple nested readers
	for i := 0; i < 5; i++ {
		nestedReader, err := rr.Index(uint32(i))
		if err != nil {
			t.Fatalf("Failed to create nested reader %d: %v", i, err)
		}

		// Read from the nested path
		buf := make([]byte, 1024)
		n, err := nestedReader.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read from nested path %d: %v", i, err)
		}

		if string(buf[:n]) != "data" {
			t.Fatalf("Expected nested data %d to be 'data', got '%s'", i, string(buf[:n]))
		}

		// Close some of the nested readers
		if i%2 == 0 {
			if err := nestedReader.Close(); err != nil {
				t.Fatalf("Failed to close nested reader %d: %v", i, err)
			}
		}
	}

	// Close the parameter writer and result reader
	if err := pw.Close(); err != nil {
		t.Fatalf("Failed to close parameter writer: %v", err)
	}

	if err := rr.Close(); err != nil {
		t.Fatalf("Failed to close result reader: %v", err)
	}
}
