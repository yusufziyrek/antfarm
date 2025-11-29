package middleware

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yusufziyrek/antfarm"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

type state int

const (
	stateClosed state = iota
	stateOpen
	stateHalfOpen
)

// CircuitBreaker returns a middleware that implements the Circuit Breaker pattern.
// failureThreshold: number of consecutive failures to open the circuit.
// resetTimeout: duration to wait before attempting to close the circuit (Half-Open).
func CircuitBreaker[T any, R any](failureThreshold int, resetTimeout time.Duration) antfarm.Middleware[T, R] {
	var mu sync.Mutex
	var currentState state = stateClosed
	var failures int
	var lastFailureTime time.Time

	return func(next antfarm.Handler[T, R]) antfarm.Handler[T, R] {
		return func(ctx context.Context, job T) (R, error) {
			mu.Lock()
			if currentState == stateOpen {
				if time.Since(lastFailureTime) > resetTimeout {
					currentState = stateHalfOpen
				} else {
					mu.Unlock()
					var zero R
					return zero, ErrCircuitOpen
				}
			}
			mu.Unlock()

			res, err := next(ctx, job)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				failures++
				lastFailureTime = time.Now()
				if failures >= failureThreshold {
					currentState = stateOpen
				}
				return res, err
			}

			// Success
			if currentState == stateHalfOpen {
				currentState = stateClosed
				failures = 0
			} else if currentState == stateClosed {
				failures = 0
			}

			return res, nil
		}
	}
}
