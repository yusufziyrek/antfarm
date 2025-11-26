package antfarm

import (
	"context"
	"sync"
)

// Handler defines the function signature for processing a single job.
// T is the input type (Task), and R is the output type (Result).
type Handler[T any, R any] func(ctx context.Context, job T) (R, error)

// Middleware is a function that wraps a Handler to add behavior (logging, metrics, etc.).
type Middleware[T any, R any] func(next Handler[T, R]) Handler[T, R]

// Result captures the output of a job execution.
type Result[R any] struct {
	Value R
	Err   error
}

// Pool is a high-performance, type-safe worker pool.
type Pool[T any, R any] struct {
	// core
	handler Handler[T, R]

	// channels
	jobQueue    chan T
	resultQueue chan Result[R]

	// configuration
	concurrency int
	bufferSize  int

	// lifecycle
	wg     sync.WaitGroup
	quit   chan struct{}
	closed bool
	mu     sync.RWMutex // protects closed state
}

// New creates a new Pool with the given concurrency level and handler.
// It applies any provided functional options.
func New[T any, R any](concurrency int, handler Handler[T, R], opts ...Option[T, R]) *Pool[T, R] {
	if concurrency <= 0 {
		concurrency = 1
	}

	p := &Pool[T, R]{
		concurrency: concurrency,
		handler:     handler,
		bufferSize:  0, // Unbuffered by default, can be changed via options
		quit:        make(chan struct{}),
	}

	// Apply options
	for _, opt := range opts {
		opt(p)
	}

	// Initialize channels after options are applied (to respect bufferSize)
	p.jobQueue = make(chan T, p.bufferSize)
	// Result queue is optional, usually users might want to consume it.
	// We'll initialize it with the same buffer size to prevent blocking workers immediately.
	p.resultQueue = make(chan Result[R], p.bufferSize)

	return p
}
