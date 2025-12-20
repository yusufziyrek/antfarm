package antfarm

import (
	"fmt"
)

// worker is the main loop for a worker goroutine.
// It consumes jobs from the jobQueue, processes them, and sends results to the resultQueue.
func (p *Pool[T, R]) worker() {
	defer p.wg.Done()

	for wrapper := range p.jobQueue {
		p.executeJob(wrapper)
	}
}

// executeJob processes a single job and handles panic recovery.
func (p *Pool[T, R]) executeJob(wrapper jobWrapper[T]) {
	ctx := wrapper.ctx

	p.busyWorkers.Add(1)

	var result R
	var err error

	defer func() {
		p.busyWorkers.Add(-1)

		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered: %v", r)
		}

		if err != nil {
			p.failedJobs.Add(1)
		} else {
			p.completedJobs.Add(1)
		}

		// Send result.
		// We block here to ensure the result is delivered.
		// The user must drain the Results channel to prevent deadlocks during Shutdown.
		p.resultQueue <- Result[R]{Value: result, Err: err}
	}()

	result, err = p.handler(ctx, wrapper.payload)
}
