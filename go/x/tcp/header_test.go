package wrpctcp

import (
	"context"
	"io"
	"testing"
)

// TestHeaderContextPropagation tests that headers can be added to and retrieved from a context
func TestHeaderContextPropagation(t *testing.T) {
	// Create a context with headers
	header := Header{
		"Content-Type": []string{"application/json"},
		"X-Request-ID": []string{"123456"},
		"Multi-Value":  []string{"value1", "value2"},
	}
	ctx := context.Background()
	ctxWithHeader := ContextWithHeader(ctx, header)

	// Extract header from context
	extractedHeader, ok := HeaderFromContext(ctxWithHeader)
	if !ok {
		t.Fatal("Expected header in context, but none found")
	}

	// Check header values
	if len(extractedHeader["Content-Type"]) != 1 || extractedHeader["Content-Type"][0] != "application/json" {
		t.Errorf("Expected Content-Type header to be 'application/json', got %v", extractedHeader["Content-Type"])
	}

	if len(extractedHeader["X-Request-ID"]) != 1 || extractedHeader["X-Request-ID"][0] != "123456" {
		t.Errorf("Expected X-Request-ID header to be '123456', got %v", extractedHeader["X-Request-ID"])
	}

	if len(extractedHeader["Multi-Value"]) != 2 ||
		extractedHeader["Multi-Value"][0] != "value1" ||
		extractedHeader["Multi-Value"][1] != "value2" {
		t.Errorf("Expected Multi-Value header to be ['value1', 'value2'], got %v", extractedHeader["Multi-Value"])
	}
}

// TestSimpleHeaderPropagation tests that headers are properly propagated through the context
// without relying on TCP connections
func TestSimpleHeaderPropagation(t *testing.T) {
	// Create a context with headers
	header := Header{
		"Content-Type": []string{"application/json"},
		"X-Request-ID": []string{"123456"},
		"Multi-Value":  []string{"value1", "value2"},
	}
	ctx := context.Background()
	ctxWithHeader := ContextWithHeader(ctx, header)

	// Define a handler function that checks for headers
	var receivedHeader Header
	var handlerCalled bool

	handler := func(ctx context.Context, w io.Writer, r io.Reader) {
		// Extract header from context
		header, ok := HeaderFromContext(ctx)
		if !ok {
			t.Error("Expected header in context, but none found")
			return
		}
		receivedHeader = header
		handlerCalled = true

		// Write a response
		w.Write([]byte("response"))
	}

	// Call the handler directly with the context
	handler(ctxWithHeader, io.Discard, nil)

	// Verify that the handler was called
	if !handlerCalled {
		t.Fatal("Handler was not called")
	}

	// Verify the received header
	if receivedHeader == nil {
		t.Fatal("No header received by handler")
	}

	// Check header values
	if len(receivedHeader["Content-Type"]) != 1 || receivedHeader["Content-Type"][0] != "application/json" {
		t.Errorf("Expected Content-Type header to be 'application/json', got %v", receivedHeader["Content-Type"])
	}

	if len(receivedHeader["X-Request-ID"]) != 1 || receivedHeader["X-Request-ID"][0] != "123456" {
		t.Errorf("Expected X-Request-ID header to be '123456', got %v", receivedHeader["X-Request-ID"])
	}

	if len(receivedHeader["Multi-Value"]) != 2 ||
		receivedHeader["Multi-Value"][0] != "value1" ||
		receivedHeader["Multi-Value"][1] != "value2" {
		t.Errorf("Expected Multi-Value header to be ['value1', 'value2'], got %v", receivedHeader["Multi-Value"])
	}
}
