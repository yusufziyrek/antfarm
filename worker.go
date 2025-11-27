package antfarm

import "context"

// worker is the main loop for a worker goroutine.
// It consumes jobs from the jobQueue, processes them, and sends results to the resultQueue.
func (p *Pool[T, R]) worker() {
	defer p.wg.Done()

	for wrapper := range p.jobQueue {
		ctx := wrapper.ctx
		if ctx == nil {
			ctx = context.Background()
		}

		result, err := p.handler(ctx, wrapper.payload)

		// Send result with anti-deadlock logic.
		// If the result queue is full and the pool is shutting down,
		// we abandon the result to ensure the worker terminates.
		select {
		case p.resultQueue <- Result[R]{Value: result, Err: err}:
		default:
			select {
			case p.resultQueue <- Result[R]{Value: result, Err: err}:
			case <-p.quit:
				return
			}
		}
	}
}
