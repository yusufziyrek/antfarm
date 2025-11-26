package antfarm

// Start initializes the workers and starts processing jobs.
// It does not block.
func (p *Pool[T, R]) Start() {
	p.wg.Add(p.concurrency)
	for i := 0; i < p.concurrency; i++ {
		go p.worker()
	}
}

// Submit adds a job to the pool.
// It blocks if the job queue is full.
// Returns ErrPoolClosed if the pool is shutting down or closed.
func (p *Pool[T, R]) Submit(job T) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPoolClosed
	}
	p.mu.RUnlock()

	select {
	case p.jobQueue <- job:
		return nil
	case <-p.quit:
		return ErrPoolClosed
	}
}

// Results returns the read-only channel for results.
func (p *Pool[T, R]) Results() <-chan Result[R] {
	return p.resultQueue
}

// Shutdown gracefully shuts down the pool.
// It closes the job queue and waits for all workers to finish processing currently active jobs.
// It does NOT wait for the job queue to drain if workers are busy, but standard worker pool behavior
// usually implies closing the channel drains it.
// Here, we will close the job queue so workers finish the range loop.
func (p *Pool[T, R]) Shutdown() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	// Closing the channel signals workers to finish after they drain the channel.
	close(p.jobQueue)

	// Signal quit for any blocking operations (like Submit)
	close(p.quit)

	// Wait for all workers to finish
	p.wg.Wait()

	// Close result queue
	close(p.resultQueue)
}
