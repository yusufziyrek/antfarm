package antfarm

import "context"

// worker is the main loop for a worker goroutine.
func (p *Pool[T, R]) worker() {
	defer p.wg.Done()

	// Create a background context for the worker.
	// In a real app, we might want to pass a context to Start() or store it in Pool.
	// For now, we use Background, but the Handler receives it.
	ctx := context.Background()

	for job := range p.jobQueue {
		// Check if we should stop early (optional, depending on requirements)
		select {
		case <-p.quit:
			// If we want to drain the queue, we shouldn't return here.
			// But if 'quit' is for hard stop, we would.
			// Our Shutdown closes jobQueue, so 'range' handles draining.
			// 'quit' is mostly for unblocking Submit.
		default:
		}

		// Execute the handler (with middleware)
		result, err := p.handler(ctx, job)

		// Send result
		// Non-blocking send to avoid deadlocks if no one is listening to results
		// OR blocking if we want strict backpressure.
		// Given "high performance" and "worker pool", usually we want to avoid dropping results.
		// However, if the user doesn't read results, this will block the worker.
		// We should document that Results() must be consumed if R is not struct{}.

		select {
		case p.resultQueue <- Result[R]{Value: result, Err: err}:
		case <-p.quit:
			return
		}
	}
}
