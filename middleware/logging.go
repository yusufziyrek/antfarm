package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/yusufziyrek/antfarm"
)

// Logging returns a middleware that logs the start and end of each job.
// It uses the provided logger. If logger is nil, it uses the default slog logger.
func Logging[T any, R any](logger *slog.Logger) antfarm.Middleware[T, R] {
	return func(next antfarm.Handler[T, R]) antfarm.Handler[T, R] {
		return func(ctx context.Context, job T) (R, error) {
			l := logger
			if l == nil {
				l = slog.Default()
			}

			start := time.Now()
			l.Info("Job started", "job", job)

			res, err := next(ctx, job)

			duration := time.Since(start)
			if err != nil {
				l.Error("Job failed", "duration", duration, "error", err)
			} else {
				l.Info("Job completed", "duration", duration, "result", res)
			}

			return res, err
		}
	}
}
