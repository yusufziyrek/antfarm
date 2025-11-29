package antfarm

import "errors"

var (
	// ErrPoolClosed is returned when attempting to submit a job to a closed pool.
	ErrPoolClosed = errors.New("antfarm: pool is closed")

	// ErrPoolFull is returned by TrySubmit when the job queue is full.
	ErrPoolFull = errors.New("antfarm: pool is full")
)
