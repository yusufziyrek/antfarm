package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/yusufziyrek/antfarm"
)

// LoggingMiddleware logs the start and end of each job.
func LoggingMiddleware[T any, R any](next antfarm.Handler[T, R]) antfarm.Handler[T, R] {
	return func(ctx context.Context, job T) (R, error) {
		start := time.Now()
		log.Printf("Job started: %v", job)

		res, err := next(ctx, job)

		duration := time.Since(start)
		if err != nil {
			log.Printf("Job failed after %v: %v", duration, err)
		} else {
			log.Printf("Job completed in %v: %v", duration, res)
		}

		return res, err
	}
}

// RetryMiddleware retries the job up to `attempts` times if it fails.
func RetryMiddleware[T any, R any](attempts int) antfarm.Middleware[T, R] {
	return func(next antfarm.Handler[T, R]) antfarm.Handler[T, R] {
		return func(ctx context.Context, job T) (R, error) {
			var err error
			var res R

			for i := 0; i < attempts; i++ {
				res, err = next(ctx, job)
				if err == nil {
					return res, nil
				}
				log.Printf("Attempt %d failed: %v. Retrying...", i+1, err)
				time.Sleep(time.Millisecond * 10) // Backoff
			}
			return res, err
		}
	}
}

func main() {
	// A handler that randomly fails
	flakyHandler := func(ctx context.Context, id int) (string, error) {
		if rand.Float32() < 0.5 {
			return "", fmt.Errorf("random error for job %d", id)
		}
		return fmt.Sprintf("Success %d", id), nil
	}

	// Create pool with Middleware:
	// 1. Retry (Outer) - catches errors from inner layers
	// 2. Logging (Inner) - logs individual attempts
	// Note: Order matters!
	// If we want to log *each* retry attempt, Logging should be *inside* Retry.
	// Structure: Retry( Logging( Handler ) )
	pool := antfarm.New(2, flakyHandler,
		antfarm.WithMiddleware(
			RetryMiddleware[int, string](3),
			LoggingMiddleware[int, string],
		),
	)

	pool.Start()

	go func() {
		for i := 0; i < 5; i++ {
			pool.Submit(context.Background(), i)
		}
		pool.Shutdown()
	}()

	for res := range pool.Results() {
		if res.Err != nil {
			fmt.Printf("Final Failure: %v\n", res.Err)
		} else {
			fmt.Printf("Final Result: %s\n", res.Value)
		}
	}
}
