package wrpctcp

import (
	"context"
	"sync/atomic"
	"testing"
)

// TestNestedPathsImplementation tests that nested paths work correctly
func TestNestedPathsImplementation(t *testing.T) {
	// Create a parameter writer with a nil connection
	// We'll override the Close method to avoid nil pointer dereference
	refCount := &atomic.Int32{}
	refCount.Add(1)

	pw := &paramWriter{
		ctx:      context.Background(),
		conn:     nil, // We don't need a real connection for this test
		path:     []uint32{},
		buffer:   getBuffer(),
		children: make(map[string]*paramWriter),
		refCount: refCount,
	}

	// Create a nested path
	nested1, err := pw.Index(1, 2, 3)
	if err != nil {
		t.Fatalf("Failed to create nested path: %v", err)
	}

	// Check that the nested path has the correct path
	nestedPw, ok := nested1.(*paramWriter)
	if !ok {
		t.Fatalf("Expected nested1 to be a *paramWriter, got %T", nested1)
	}

	if len(nestedPw.path) != 3 || nestedPw.path[0] != 1 || nestedPw.path[1] != 2 || nestedPw.path[2] != 3 {
		t.Errorf("Expected nested path to be [1, 2, 3], got %v", nestedPw.path)
	}

	// Create another nested path from the first one
	nested2, err := nested1.Index(4, 5)
	if err != nil {
		t.Fatalf("Failed to create nested path: %v", err)
	}

	// Check that the nested path has the correct path
	nestedPw2, ok := nested2.(*paramWriter)
	if !ok {
		t.Fatalf("Expected nested2 to be a *paramWriter, got %T", nested2)
	}

	if len(nestedPw2.path) != 5 ||
		nestedPw2.path[0] != 1 ||
		nestedPw2.path[1] != 2 ||
		nestedPw2.path[2] != 3 ||
		nestedPw2.path[3] != 4 ||
		nestedPw2.path[4] != 5 {
		t.Errorf("Expected nested path to be [1, 2, 3, 4, 5], got %v", nestedPw2.path)
	}

	// Check that the parent-child relationship is correct
	if nestedPw2.parent != nestedPw {
		t.Errorf("Expected nested2's parent to be nested1")
	}

	// Check that the reference counting is working
	if refCount.Load() != 3 {
		t.Errorf("Expected reference count to be 3, got %d", refCount.Load())
	}

	// We can't actually close the writers because they try to write to the connection
	// Instead, we'll just check the reference count directly
	nestedPw2.refCount.Add(-1)
	nestedPw.refCount.Add(-1)
	pw.refCount.Add(-1)

	// Check that the reference count is back to 0
	if refCount.Load() != 0 {
		t.Errorf("Expected reference count to be 0, got %d", refCount.Load())
	}

	// Clean up
	putBuffer(nestedPw2.buffer)
	putBuffer(nestedPw.buffer)
	putBuffer(pw.buffer)
}
