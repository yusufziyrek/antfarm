package antfarm

// Option defines a functional configuration option for the Pool.
type Option[T any, R any] func(*Pool[T, R])

// WithBufferSize sets the capacity of the job and result channels.
// The default buffer size is 0 (unbuffered).
func WithBufferSize[T any, R any](size int) Option[T, R] {
	return func(p *Pool[T, R]) {
		if size < 0 {
			size = 0
		}
		p.bufferSize = size
	}
}

// WithMiddleware adds one or more middleware to the pool's handler.
// Middleware are applied in the order they are provided.
// For example, WithMiddleware(A, B) results in a chain where A wraps B, and B wraps the handler.
func WithMiddleware[T any, R any](mw ...Middleware[T, R]) Option[T, R] {
	return func(p *Pool[T, R]) {
		// Wrap the handler in reverse order so that the first middleware
		// in the slice becomes the outer-most wrapper.
		for i := len(mw) - 1; i >= 0; i-- {
			p.handler = mw[i](p.handler)
		}
	}
}
