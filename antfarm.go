// Package antfarm provides a high-performance, type-safe, and generic worker pool implementation.
// It supports context propagation, middleware chains, and graceful shutdowns.
package antfarm

import (
	"context"
	"sync"
)

// Handler defines the function signature for processing a single job.
// T is the input type (Task), and R is the output type (Result).
// The context passed to the handler is propagated from the Submit call.
type Handler[T any, R any] func(ctx context.Context, job T) (R, error)

// Middleware is a function that wraps a Handler to add cross-cutting concerns
// such as logging, metrics, or retries.
type Middleware[T any, R any] func(next Handler[T, R]) Handler[T, R]

// Result captures the output of a job execution, including any error that occurred.
type Result[R any] struct {
	Value R
	Err   error
}

// jobWrapper holds the job payload and its associated context.
// It is an internal wrapper to transport context through the job channel.
type jobWrapper[T any] struct {
	ctx     context.Context
	payload T
}

// Pool is a high-performance, type-safe worker pool.
// It manages a fixed number of workers to process jobs concurrently.
type Pool[T any, R any] struct {
	// core
	handler Handler[T, R]

	// channels
	jobQueue    chan jobWrapper[T]
	resultQueue chan Result[R]

	// configuration
	concurrency int
	bufferSize  int

	// lifecycle
	wg       sync.WaitGroup
	submitWg sync.WaitGroup // Waits for active Submit calls to finish
	quit     chan struct{}
	closed   int32 // Atomic flag (0: open, 1: closed)
	mu       sync.RWMutex

	// stats
	busyWorkers   int32
	submittedJobs uint64
	completedJobs uint64
	failedJobs    uint64
}

// Stats contains runtime metrics of the worker pool.
type Stats struct {
	Concurrency   int
	BusyWorkers   int
	SubmittedJobs uint64
	CompletedJobs uint64
	FailedJobs    uint64
}

// New creates a new Pool with the specified concurrency level and job handler.
// It applies any provided functional options to configure the pool.
// If concurrency is less than or equal to 0, it defaults to 1.
func New[T any, R any](concurrency int, handler Handler[T, R], opts ...Option[T, R]) *Pool[T, R] {
	concurrency = max(1, concurrency)

	p := &Pool[T, R]{
		concurrency: concurrency,
		handler:     handler,
		bufferSize:  0, // Unbuffered by default
		quit:        make(chan struct{}),
	}

	// Apply options
	for _, opt := range opts {
		opt(p)
	}

	// Initialize channels after options are applied
	p.jobQueue = make(chan jobWrapper[T], p.bufferSize)
	p.resultQueue = make(chan Result[R], p.bufferSize)

	return p
}
