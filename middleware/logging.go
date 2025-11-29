package middleware

import (
	"context"
	"log"
	"time"

	"github.com/yusufziyrek/antfarm"
)

// Logging returns a middleware that logs the start and end of each job.
// It uses the provided logger. If logger is nil, it uses the standard log package.
func Logging[T any, R any](logger *log.Logger) antfarm.Middleware[T, R] {
	return func(next antfarm.Handler[T, R]) antfarm.Handler[T, R] {
		return func(ctx context.Context, job T) (R, error) {
			start := time.Now()
			if logger != nil {
				logger.Printf("Job started: %v", job)
			} else {
				log.Printf("Job started: %v", job)
			}

			res, err := next(ctx, job)

			duration := time.Since(start)
			if logger != nil {
				if err != nil {
					logger.Printf("Job failed after %v: %v", duration, err)
				} else {
					logger.Printf("Job completed in %v: %v", duration, res)
				}
			} else {
				if err != nil {
					log.Printf("Job failed after %v: %v", duration, err)
				} else {
					log.Printf("Job completed in %v: %v", duration, res)
				}
			}

			return res, err
		}
	}
}
