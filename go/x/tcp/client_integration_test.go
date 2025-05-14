//go:build integration
// +build integration

package wrpctcp

import (
	"context"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestWithRustServer(t *testing.T) {
	//
	// Setup
	//
	port := os.Getenv("TEST_TCP_PORT")
	if port == "" {
		port = "7761"
	}

	t.Log("Starting Rust TCP server...")
	cmdBuild := exec.Command("cargo", "build")
	cmdBuild.Dir = "examples/rust/hello-tcp-server"
	cmdBuild.Stdout = os.Stdout
	cmdBuild.Stderr = os.Stderr
	if err := cmdBuild.Run(); err != nil {
		t.Log("Failed to build Rust server:", err)
		os.Exit(1)
	}

	cmdRun := exec.Command("cargo", "run", "--", "[::1]:"+port)
	cmdRun.Dir = "examples/rust/hello-tcp-server"
	cmdRun.Stdout = os.Stdout
	cmdRun.Stderr = os.Stderr
	if err := cmdRun.Start(); err != nil {
		t.Log("Failed to start Rust server:", err)
		os.Exit(1)
	}
	serverPID := cmdRun.Process.Pid

	defer func() {
		t.Log("Killing Rust server...")
		_ = syscall.Kill(serverPID, 9)
	}()

	t.Log("Waiting for server to start...")
	time.Sleep(5 * time.Second)

	t.Log("Running Go TCP client tests...")
	os.Setenv("TEST_TCP_PORT", port)

	//
	// Execute test
	//
	t.Logf("Running integration test with port %s", port)

	// Create a client
	var cancel func()
	ctx := context.Background()
	dl, ok := t.Deadline()
	if ok {
		ctx, cancel = context.WithDeadline(ctx, dl)
	} else {
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	}
	defer cancel()

	client := NewClient("[::1]:"+port, ctx)
	if err := client.Connect(); err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer client.Close()

	// Invoke a function
	pw, rr, err := client.Invoke(ctx, "test", "hello", []byte{})
	if err != nil {
		t.Fatalf("Failed to invoke function: %v", err)
	}

	// Read the response
	buf := make([]byte, 1024)
	n, err := rr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read response: %v", err)
	}

	if string(buf[:n]) != "hello world" {
		t.Fatalf("Expected response to be 'hello world', got '%s'", string(buf[:n]))
	}

	// Close the parameter writer
	if err := pw.Close(); err != nil {
		t.Fatalf("Failed to close parameter writer: %v", err)
	}
}
