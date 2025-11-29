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
	handler := func(ctx context.Context, job int) (int, error) {
		time.Sleep(100 * time.Millisecond) // Simulate work
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

	// Wait a bit to ensure worker picks it up
	time.Sleep(10 * time.Millisecond)

	// 2. TrySubmit should fail because worker is busy and buffer is 0
	err := pool.TrySubmit(context.Background(), 2)
	if err != antfarm.ErrPoolFull {
		t.Errorf("expected ErrPoolFull, got %v", err)
	}

	// 3. Wait for worker to finish
	time.Sleep(150 * time.Millisecond)

	// 4. TrySubmit should succeed now
	if err := pool.TrySubmit(context.Background(), 3); err != nil {
		t.Errorf("expected success, got %v", err)
	}
}
