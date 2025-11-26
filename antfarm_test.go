package antfarm_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yusufziyrek/antfarm"
)

// TestBasicProcessing verifies that a pool processes all submitted jobs.
func TestBasicProcessing(t *testing.T) {
	t.Log("Starting TestBasicProcessing")
	jobCount := 100
	handler := func(ctx context.Context, job int) (int, error) {
		return job * 2, nil
	}

	pool := antfarm.New(10, handler)
	pool.Start()

	// Submit jobs
	go func() {
		for i := 0; i < jobCount; i++ {
			if err := pool.Submit(i); err != nil {
				t.Errorf("failed to submit job %d: %v", i, err)
			}
		}
		pool.Shutdown()
	}()

	receivedCount := 0
	for res := range pool.Results() {
		if res.Err != nil {
			t.Errorf("unexpected error: %v", res.Err)
		}
		receivedCount++
	}
	t.Logf("TestBasicProcessing: Received %d results", receivedCount)

	if receivedCount != jobCount {
		t.Errorf("expected %d results, got %d", jobCount, receivedCount)
	}
}

// TestConcurrency ensures workers run in parallel.
func TestConcurrency(t *testing.T) {
	t.Log("Starting TestConcurrency")
	concurrency := 5
	startGate := make(chan struct{})
	var activeWorkers int32

	handler := func(ctx context.Context, _ int) (int, error) {
		atomic.AddInt32(&activeWorkers, 1)
		defer atomic.AddInt32(&activeWorkers, -1)
		<-startGate
		return 0, nil
	}

	// Use buffered channels to prevent blocking during setup
	pool := antfarm.New(concurrency, handler, antfarm.WithBufferSize[int, int](concurrency))
	pool.Start()

	// Start consuming results immediately in background to prevent deadlock
	go func() {
		for range pool.Results() {
		}
		t.Log("TestConcurrency: Results drained")
	}()

	// Submit jobs
	go func() {
		for i := 0; i < concurrency; i++ {
			pool.Submit(i)
		}
		t.Log("TestConcurrency: Jobs submitted")
	}()

	time.Sleep(100 * time.Millisecond)

	currentActive := atomic.LoadInt32(&activeWorkers)
	t.Logf("TestConcurrency: Active workers: %d", currentActive)
	if currentActive != int32(concurrency) {
		t.Errorf("expected %d active workers, got %d", concurrency, currentActive)
	}

	close(startGate)
	t.Log("TestConcurrency: Gate closed, shutting down")
	pool.Shutdown()
	t.Log("TestConcurrency: Shutdown complete")
}

// TestGracefulShutdown verifies that submitted jobs are completed before shutdown returns.
func TestGracefulShutdown(t *testing.T) {
	t.Log("Starting TestGracefulShutdown")
	var completedJobs int32
	jobCount := 20

	handler := func(ctx context.Context, job int) (int, error) {
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&completedJobs, 1)
		return job, nil
	}

	pool := antfarm.New(5, handler)
	pool.Start()

	// Use a done channel to wait for results to be fully drained
	done := make(chan struct{})
	go func() {
		for range pool.Results() {
		}
		t.Log("TestGracefulShutdown: Results drained")
		close(done)
	}()

	go func() {
		for i := 0; i < jobCount; i++ {
			pool.Submit(i)
		}
		t.Log("TestGracefulShutdown: Jobs submitted, shutting down")
		pool.Shutdown()
	}()

	// Wait for results to be fully drained (which implies Shutdown is complete)
	<-done

	if atomic.LoadInt32(&completedJobs) != int32(jobCount) {
		t.Errorf("expected %d completed jobs, got %d", jobCount, completedJobs)
	}
}

// TestMiddleware verifies middleware execution order and functionality.
func TestMiddleware(t *testing.T) {
	t.Log("Starting TestMiddleware")
	var executionLog []string
	var mu sync.Mutex

	appendLog := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		executionLog = append(executionLog, msg)
	}

	mw1 := func(next antfarm.Handler[int, int]) antfarm.Handler[int, int] {
		return func(ctx context.Context, job int) (int, error) {
			appendLog("mw1 start")
			res, err := next(ctx, job)
			appendLog("mw1 end")
			return res, err
		}
	}

	mw2 := func(next antfarm.Handler[int, int]) antfarm.Handler[int, int] {
		return func(ctx context.Context, job int) (int, error) {
			appendLog("mw2 start")
			res, err := next(ctx, job)
			appendLog("mw2 end")
			return res, err
		}
	}

	handler := func(ctx context.Context, job int) (int, error) {
		appendLog("handler")
		return job, nil
	}

	pool := antfarm.New(1, handler, antfarm.WithMiddleware(mw1, mw2))
	pool.Start()

	// Start consuming results in BG to prevent deadlock
	done := make(chan struct{})
	go func() {
		for range pool.Results() {
		}
		close(done)
	}()

	pool.Submit(1)
	pool.Shutdown()

	// Wait for results to drain
	<-done
	t.Log("TestMiddleware: Finished")

	expected := []string{"mw1 start", "mw2 start", "handler", "mw2 end", "mw1 end"}

	mu.Lock()
	defer mu.Unlock()

	if len(executionLog) != len(expected) {
		t.Fatalf("expected log length %d, got %d", len(expected), len(executionLog))
	}

	for i, v := range expected {
		if executionLog[i] != v {
			t.Errorf("index %d: expected %s, got %s", i, v, executionLog[i])
		}
	}
}

// TestSubmitAfterShutdown verifies that submitting to a closed pool returns an error.
func TestSubmitAfterShutdown(t *testing.T) {
	pool := antfarm.New(1, func(ctx context.Context, i int) (int, error) { return i, nil })
	pool.Start()

	// Consume results in BG
	go func() {
		for range pool.Results() {
		}
	}()

	pool.Shutdown()

	err := pool.Submit(1)
	if !errors.Is(err, antfarm.ErrPoolClosed) {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}
}
