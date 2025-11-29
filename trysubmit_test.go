package antfarm_test

import (
	"context"
	"testing"
	"time"

	"github.com/yusufziyrek/antfarm"
)

// TestTrySubmit verifies non-blocking submission behavior.
func TestTrySubmit(t *testing.T) {
	// Create a pool with 1 worker and 0 buffer (unbuffered)
	// This ensures that if the worker is busy, TrySubmit should fail immediately.

	started := make(chan struct{})
	block := make(chan struct{})

	handler := func(ctx context.Context, job int) (int, error) {
		if job == 1 {
			close(started)
			<-block // Keep busy
		}
		return job, nil
	}

	pool := antfarm.New(1, handler, antfarm.WithBufferSize[int, int](0))
	pool.Start()
	defer pool.Shutdown()

	// Drain results in background to prevent deadlock
	go func() {
		for range pool.Results() {
		}
	}()

	// 1. Submit a job that will occupy the worker
	if err := pool.Submit(context.Background(), 1); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Wait for worker to pick it up
	<-started

	// 2. TrySubmit should fail because worker is busy and buffer is 0
	err := pool.TrySubmit(context.Background(), 2)
	if err != antfarm.ErrPoolFull {
		t.Errorf("expected ErrPoolFull, got %v", err)
	}

	// 3. Unblock worker
	close(block)

	// We need to wait for the worker to be free again.
	// Since we don't have a direct "worker idle" signal, we can retry TrySubmit until it succeeds,
	// or wait for the result of job 1 (which we are draining).
	// But draining happens in background.
	// We can just retry TrySubmit with a small timeout or loop.
	// Ideally, we should wait for job 1 to finish.
	// Since we are draining results, we can't easily check result of job 1 here unless we change the drain logic.

	// Let's change drain logic to just drain.
	// We can use `Eventually` pattern or just a loop.

	timeout := time.After(1 * time.Second)
	success := false
	for {
		select {
		case <-timeout:
			t.Fatal("timed out waiting for TrySubmit to succeed")
		default:
			if err := pool.TrySubmit(context.Background(), 3); err == nil {
				success = true
			}
		}
		if success {
			break
		}
		time.Sleep(10 * time.Millisecond) // Small sleep for retry loop
	}
}
