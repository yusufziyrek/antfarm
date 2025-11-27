package antfarm

import (
	"context"
	"fmt"
	"sync/atomic"
)

// worker is the main loop for a worker goroutine.
// It consumes jobs from the jobQueue, processes them, and sends results to the resultQueue.
func (p *Pool[T, R]) worker() {
	defer p.wg.Done()

	for wrapper := range p.jobQueue {
		ctx := wrapper.ctx
		if ctx == nil {
			ctx = context.Background()
		}

		atomic.AddInt32(&p.busyWorkers, 1)

		var result R
		var err error

		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic recovered: %v", r)
				}
			}()
			result, err = p.handler(ctx, wrapper.payload)
		}()

		atomic.AddInt32(&p.busyWorkers, -1)

		if err != nil {
			atomic.AddUint64(&p.failedJobs, 1)
		} else {
			atomic.AddUint64(&p.completedJobs, 1)
		}

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
