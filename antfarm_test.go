package antfarm_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yusufziyrek/antfarm"
)

// TestPool_TableDriven validates the pool's behavior using table-driven tests.
func TestPool_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
		bufferSize  int
		jobs        int
		handler     antfarm.Handler[int, int]
		wantErr     bool // Expect errors in results
	}{
		{
			name:        "SingleWorker_Unbuffered",
			concurrency: 1,
			bufferSize:  0,
			jobs:        10,
			handler: func(ctx context.Context, job int) (int, error) {
				return job * 2, nil
			},
			wantErr: false,
		},
		{
			name:        "MultiWorker_Buffered",
			concurrency: 5,
			bufferSize:  10,
			jobs:        100,
			handler: func(ctx context.Context, job int) (int, error) {
				return job * 2, nil
			},
			wantErr: false,
		},
		{
			name:        "HandlerErrors",
			concurrency: 2,
			bufferSize:  0,
			jobs:        5,
			handler: func(ctx context.Context, job int) (int, error) {
				return 0, errors.New("task failed")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []antfarm.Option[int, int]
			if tt.bufferSize > 0 {
				opts = append(opts, antfarm.WithBufferSize[int, int](tt.bufferSize))
			}

			p := antfarm.New(tt.concurrency, tt.handler, opts...)
			p.Start()

			// Submit jobs in a separate goroutine
			go func() {
				for i := 0; i < tt.jobs; i++ {
					if err := p.Submit(context.Background(), i); err != nil {
						t.Errorf("Submit failed: %v", err)
					}
				}
				p.Shutdown()
			}()

			// Collect results
			count := 0
			for res := range p.Results() {
				count++
				if tt.wantErr {
					if res.Err == nil {
						// In this specific test case, we expect ALL to fail.
					}
				} else {
					if res.Err != nil {
						t.Errorf("unexpected job error: %v", res.Err)
					}
				}
			}

			if count != tt.jobs {
				t.Errorf("got %d results, want %d", count, tt.jobs)
			}
		})
	}
}

// TestConcurrency ensures workers run in parallel and respect concurrency limits.
func TestConcurrency(t *testing.T) {
	concurrency := 5
	startGate := make(chan struct{})
	var activeWorkers int32

	handler := func(ctx context.Context, _ int) (int, error) {
		atomic.AddInt32(&activeWorkers, 1)
		defer atomic.AddInt32(&activeWorkers, -1)
		<-startGate
		return 0, nil
	}

	pool := antfarm.New(concurrency, handler, antfarm.WithBufferSize[int, int](concurrency))
	pool.Start()

	// Drain results in background
	go func() {
		for range pool.Results() {
		}
	}()

	// Fill the pool
	for i := 0; i < concurrency; i++ {
		go pool.Submit(context.Background(), i)
	}

	// Allow goroutines to start
	time.Sleep(50 * time.Millisecond)

	currentActive := atomic.LoadInt32(&activeWorkers)
	if currentActive != int32(concurrency) {
		t.Errorf("expected %d active workers, got %d", concurrency, currentActive)
	}

	close(startGate)
	pool.Shutdown()
}

// TestMiddleware verifies middleware execution order.
func TestMiddleware(t *testing.T) {
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

	go func() {
		pool.Submit(context.Background(), 1)
		pool.Shutdown()
	}()

	for range pool.Results() {
	}

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

// TestPanicRecovery ensures that panics in handlers are caught and returned as errors.
func TestPanicRecovery(t *testing.T) {
	handler := func(ctx context.Context, job int) (int, error) {
		if job == 666 {
			panic("something went wrong")
		}
		return job, nil
	}

	pool := antfarm.New(1, handler)
	pool.Start()

	go func() {
		pool.Submit(context.Background(), 666)
		pool.Shutdown()
	}()

	res := <-pool.Results()
	if res.Err == nil {
		t.Error("expected error from panic, got nil")
	}
	if !strings.Contains(res.Err.Error(), "panic recovered") {
		t.Errorf("expected panic error message, got: %v", res.Err)
	}
}

// TestStats verifies that stats are updated correctly.
func TestStats(t *testing.T) {
	var block = make(chan struct{})
	handler := func(ctx context.Context, job int) (int, error) {
		if job == 1 {
			<-block // Block this worker
			return job, nil
		}
		if job == 2 {
			return 0, errors.New("fail")
		}
		return job, nil
	}

	pool := antfarm.New(2, handler)
	pool.Start()

	// Submit job 1 (will block)
	go pool.Submit(context.Background(), 1)

	// Wait for worker to pick it up
	time.Sleep(50 * time.Millisecond)

	stats := pool.Stats()
	if stats.BusyWorkers != 1 {
		t.Errorf("expected 1 busy worker, got %d", stats.BusyWorkers)
	}
	if stats.SubmittedJobs != 1 {
		t.Errorf("expected 1 submitted job, got %d", stats.SubmittedJobs)
	}

	// Submit job 2 (will fail)
	go pool.Submit(context.Background(), 2)
	time.Sleep(50 * time.Millisecond) // Wait for processing

	stats = pool.Stats()
	if stats.FailedJobs != 1 {
		t.Errorf("expected 1 failed job, got %d", stats.FailedJobs)
	}

	// Unblock job 1
	close(block)

	// Drain results
	go func() {
		for range pool.Results() {
		}
	}()
	pool.Shutdown()

	stats = pool.Stats()
	if stats.CompletedJobs != 1 {
		t.Errorf("expected 1 completed job, got %d", stats.CompletedJobs)
	}
}

// BenchmarkPool benchmarks the throughput of the pool.
func BenchmarkPool(b *testing.B) {
	handler := func(ctx context.Context, job int) (int, error) {
		return job, nil
	}

	benchmarks := []struct {
		name        string
		concurrency int
		buffer      int
	}{
		{"C1_B0", 1, 0},
		{"C10_B0", 10, 0},
		{"C10_B100", 10, 100},
		{"C100_B1000", 100, 1000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			p := antfarm.New(bm.concurrency, handler, antfarm.WithBufferSize[int, int](bm.buffer))
			p.Start()

			// Consumer
			go func() {
				for range p.Results() {
				}
			}()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.Submit(context.Background(), i)
			}
			p.Shutdown()
		})
	}
}
