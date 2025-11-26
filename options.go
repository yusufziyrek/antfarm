package antfarm

// Option defines a functional configuration option for the Pool.
type Option[T any, R any] func(*Pool[T, R])

// WithBufferSize sets the size of the job and result channels.
// Default is 0 (unbuffered).
func WithBufferSize[T any, R any](size int) Option[T, R] {
	return func(p *Pool[T, R]) {
		if size < 0 {
			size = 0
		}
		p.bufferSize = size
	}
}

// WithMiddleware adds one or more middleware to the pool's handler.
// Middleware is applied in the order provided (first in, outer-most).
func WithMiddleware[T any, R any](mw ...Middleware[T, R]) Option[T, R] {
	return func(p *Pool[T, R]) {
		// We need to wrap the existing handler.
		// To maintain the order: mw[0] wraps mw[1] ... wraps handler.
		// Actually, usually it's: mw[0] -> mw[1] -> handler.
		// So we iterate in reverse if we want mw[0] to be the outer-most.
		// Or we can just wrap iteratively.
		// Let's say mw = [Log, Metrics].
		// We want Log(Metrics(Handler)).

		// If we iterate normal:
		// 1. h = Log(h)
		// 2. h = Metrics(h) -> Metrics(Log(original)) -> This is usually "inside-out" depending on implementation.
		// Standard middleware chaining:
		// final = m1(m2(handler))

		for i := len(mw) - 1; i >= 0; i-- {
			p.handler = mw[i](p.handler)
		}
	}
}
