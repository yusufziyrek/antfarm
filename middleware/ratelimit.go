package middleware

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yusufziyrek/antfarm"
)

var ErrRateLimitExceeded = errors.New("rate limit exceeded")

// RateLimit returns a middleware that limits the number of jobs processed within a given window.
// It uses a simple fixed window algorithm.
// limit: max number of jobs allowed per window.
// window: duration of the window.
func RateLimit[T any, R any](limit int, window time.Duration) antfarm.Middleware[T, R] {
	var mu sync.Mutex
	var count int
	var windowStart = time.Now()

	return func(next antfarm.Handler[T, R]) antfarm.Handler[T, R] {
		return func(ctx context.Context, job T) (R, error) {
			mu.Lock()
			now := time.Now()
			if now.Sub(windowStart) > window {
				// New window
				windowStart = now
				count = 0
			}

			if count >= limit {
				mu.Unlock()
				var zero R
				return zero, ErrRateLimitExceeded
			}

			count++
			mu.Unlock()

			return next(ctx, job)
		}
	}
}
