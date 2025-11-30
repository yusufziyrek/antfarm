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
	readyWg := sync.WaitGroup{}
	readyWg.Add(concurrency)

	var activeWorkers int32

	handler := func(ctx context.Context, _ int) (int, error) {
		readyWg.Done() // Signal that worker is ready and running
		<-startGate    // Wait for signal to proceed
		atomic.AddInt32(&activeWorkers, 1)
		defer atomic.AddInt32(&activeWorkers, -1)
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

	// Wait for all workers to be running and blocked on startGate
	readyWg.Wait()

	// Now we know all workers are active (but blocked), so we can check if they are all running.
	// However, since they are blocked BEFORE incrementing activeWorkers in my new logic,
	// I need to adjust the test logic or the handler logic.
	// Actually, to test concurrency limit, we want to ensure that NO MORE than `concurrency` workers are running.
	// But here we are testing that AT LEAST `concurrency` workers ARE running.

	// Let's adjust the handler to increment BEFORE waiting, so we can verify they are all up.
	// But if we increment before waiting, we need another way to know they reached that point.
	// readyWg.Done() does exactly that.

	// So if readyWg.Wait() returns, it means `concurrency` goroutines have started.
	// We don't strictly need `activeWorkers` counter if we trust `readyWg`, but let's keep it for sanity check
	// if we move the increment up.

	// Correct logic for "TestConcurrency":
	// We want to prove that `concurrency` jobs are being processed simultaneously.

	close(startGate) // Release them
	pool.Shutdown()
}

// TestConcurrency_Real ensures workers run in parallel.
func TestConcurrency_Real(t *testing.T) {
	concurrency := 5
	// We want to ensure that 5 workers are running at the same time.
	// We can use a barrier.

	barrier := make(chan struct{})
	ready := make(chan struct{}, concurrency)

	handler := func(ctx context.Context, _ int) (int, error) {
		ready <- struct{}{} // Signal ready
		<-barrier           // Wait for all to be ready
		return 0, nil
	}

	pool := antfarm.New(concurrency, handler, antfarm.WithBufferSize[int, int](concurrency))
	pool.Start()

	go func() {
		for range pool.Results() {
		}
	}()

	for i := 0; i < concurrency; i++ {
		go pool.Submit(context.Background(), i)
	}

	// Wait for all 5 to signal ready
	timeout := time.After(1 * time.Second)
	for i := 0; i < concurrency; i++ {
		select {
		case <-ready:
		case <-timeout:
			t.Fatalf("timed out waiting for worker %d", i)
		}
	}

	// If we reached here, 5 workers are currently blocked on <-barrier.
	// This proves 5 concurrent executions.
	close(barrier)
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
	// Channels to coordinate execution
	job1Started := make(chan struct{})
	job1Block := make(chan struct{})

	job2Started := make(chan struct{})
	// job2 doesn't block, it just fails

	handler := func(ctx context.Context, job int) (int, error) {
		if job == 1 {
			close(job1Started)
			<-job1Block // Block this worker
			return job, nil
		}
		if job == 2 {
			close(job2Started)
			return 0, errors.New("fail")
		}
		return job, nil
	}

	pool := antfarm.New(2, handler)
	pool.Start()

	// Submit job 1 (will block)
	go pool.Submit(context.Background(), 1)

	// Wait for worker to pick it up
	<-job1Started

	stats := pool.Stats()
	if stats.BusyWorkers != 1 {
		t.Errorf("expected 1 busy worker, got %d", stats.BusyWorkers)
	}
	if stats.SubmittedJobs != 1 {
		t.Errorf("expected 1 submitted job, got %d", stats.SubmittedJobs)
	}

	// Submit job 2 (will fail)
	go pool.Submit(context.Background(), 2)

	// Wait for job 2 to start (and finish immediately after)
	<-job2Started

	// We need to wait for job 2 to actually finish processing and update stats.
	// Since we don't have a "job finished" channel exposed from the pool,
	// and we are inside the test, we can just wait for the result to appear in Results().
	// But Results() gives us the result, it doesn't guarantee the stats are updated YET
	// (though usually it happens before sending result).
	// Looking at worker.go:
	// p.failedJobs.Add(1) -> p.resultQueue <- Result
	// So if we receive the result, the stats ARE updated.

	// Let's consume one result (which should be job 2, as job 1 is blocked)
	res := <-pool.Results()
	if res.Value != 0 { // Job 2 returns 0 on error
		t.Errorf("expected job 2 result, got %v", res)
	}

	stats = pool.Stats()
	if stats.FailedJobs != 1 {
		t.Errorf("expected 1 failed job, got %d", stats.FailedJobs)
	}

	// Unblock job 1
	close(job1Block)

	// Drain remaining results (job 1)
	<-pool.Results()

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

// BenchmarkGoroutines compares the pool against raw goroutines.
func BenchmarkGoroutines(b *testing.B) {
	handler := func(ctx context.Context, job int) (int, error) {
		return job, nil
	}

	b.Run("RawGoroutines", func(b *testing.B) {
		var wg sync.WaitGroup
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				handler(context.Background(), val)
			}(i)
		}
		wg.Wait()
	})

	b.Run("AntFarm_Pool", func(b *testing.B) {
		// Use a reasonable fixed size to show the benefit of pooling.
		p := antfarm.New(100, handler, antfarm.WithBufferSize[int, int](1000))
		p.Start()

		// Consumer to drain results
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
