package antfarm

import (
	"context"
	"sync/atomic"
)

// Start initializes the worker goroutines and begins processing jobs.
// This method is non-blocking.
func (p *Pool[T, R]) Start() {
	p.wg.Add(p.concurrency)
	for i := 0; i < p.concurrency; i++ {
		go p.worker()
	}
}

// Submit adds a job to the pool for processing.
// It blocks if the job queue is full.
// It returns ErrPoolClosed if the pool is shutting down or closed.
// The provided context is passed to the handler and can be used for cancellation or timeouts.
func (p *Pool[T, R]) Submit(ctx context.Context, job T) error {
	if atomic.LoadInt32(&p.closed) == 1 {
		return ErrPoolClosed
	}

	p.mu.RLock()
	if atomic.LoadInt32(&p.closed) == 1 {
		p.mu.RUnlock()
		return ErrPoolClosed
	}
	p.submitWg.Add(1)
	p.mu.RUnlock()

	defer p.submitWg.Done()

	atomic.AddUint64(&p.submittedJobs, 1)

	select {
	case p.jobQueue <- jobWrapper[T]{ctx: ctx, payload: job}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.quit:
		return ErrPoolClosed
	}
}

// Stats returns a snapshot of the pool's runtime metrics.
func (p *Pool[T, R]) Stats() Stats {
	return Stats{
		Concurrency:   p.concurrency,
		BusyWorkers:   int(atomic.LoadInt32(&p.busyWorkers)),
		SubmittedJobs: atomic.LoadUint64(&p.submittedJobs),
		CompletedJobs: atomic.LoadUint64(&p.completedJobs),
		FailedJobs:    atomic.LoadUint64(&p.failedJobs),
	}
}

// Results returns a read-only channel to consume job results.
// The channel is closed when the pool is fully shut down and all results have been published.
func (p *Pool[T, R]) Results() <-chan Result[R] {
	return p.resultQueue
}

// Shutdown gracefully stops the pool.
// It stops accepting new jobs and waits for:
// 1. All active Submit calls to return.
// 2. All active workers to finish their current jobs.
// 3. The job queue to be drained (if workers are still processing).
// Finally, it closes the results channel.
func (p *Pool[T, R]) Shutdown() {
	p.mu.Lock()
	if atomic.LoadInt32(&p.closed) == 1 {
		p.mu.Unlock()
		return
	}
	atomic.StoreInt32(&p.closed, 1)
	p.mu.Unlock()

	close(p.quit)
	p.submitWg.Wait()
	close(p.jobQueue)
	p.wg.Wait()
	close(p.resultQueue)
}
