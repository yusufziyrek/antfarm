package antfarm

import (
	"context"
)

// Start initializes the worker goroutines and begins processing jobs.
// This method is non-blocking. It is idempotent; calling it multiple times has no effect.
func (p *Pool[T, R]) Start() {
	if !p.started.CompareAndSwap(0, 1) {
		return
	}

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
	if ctx == nil {
		ctx = context.Background()
	}
	if p.closed.Load() == 1 {
		return ErrPoolClosed
	}

	p.mu.RLock()
	if p.closed.Load() == 1 {
		p.mu.RUnlock()
		return ErrPoolClosed
	}
	p.submitWg.Add(1)
	p.mu.RUnlock()

	defer p.submitWg.Done()

	p.submittedJobs.Add(1)

	select {
	case p.jobQueue <- jobWrapper[T]{ctx: ctx, payload: job}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.quit:
		return ErrPoolClosed
	}
}

// TrySubmit adds a job to the pool if the queue is not full.
// It returns ErrPoolFull if the queue is full.
// It returns ErrPoolClosed if the pool is shutting down or closed.
func (p *Pool[T, R]) TrySubmit(ctx context.Context, job T) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p.closed.Load() == 1 {
		return ErrPoolClosed
	}

	p.mu.RLock()
	if p.closed.Load() == 1 {
		p.mu.RUnlock()
		return ErrPoolClosed
	}
	p.submitWg.Add(1)
	p.mu.RUnlock()

	defer p.submitWg.Done()

	p.submittedJobs.Add(1)

	select {
	case p.jobQueue <- jobWrapper[T]{ctx: ctx, payload: job}:
		return nil
	case <-p.quit:
		return ErrPoolClosed
	default:
		return ErrPoolFull
	}
}

// Stats returns a snapshot of the pool's runtime metrics.
func (p *Pool[T, R]) Stats() Stats {
	return Stats{
		Concurrency:   p.concurrency,
		BusyWorkers:   int(p.busyWorkers.Load()),
		SubmittedJobs: p.submittedJobs.Load(),
		CompletedJobs: p.completedJobs.Load(),
		FailedJobs:    p.failedJobs.Load(),
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
	if p.closed.Load() == 1 {
		p.mu.Unlock()
		return
	}
	p.closed.Store(1)
	p.mu.Unlock()

	close(p.quit)
	p.submitWg.Wait()
	close(p.jobQueue)
	p.wg.Wait()
	close(p.resultQueue)
}
