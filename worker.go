package antfarm

import (
	"context"
	"fmt"
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

		p.busyWorkers.Add(1)

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

		p.busyWorkers.Add(-1)

		if err != nil {
			p.failedJobs.Add(1)
		} else {
			p.completedJobs.Add(1)
		}

		// Send result.
		// We block here to ensure the result is delivered.
		// The user must drain the Results channel to prevent deadlocks during Shutdown.
		p.resultQueue <- Result[R]{Value: result, Err: err}
	}
}
