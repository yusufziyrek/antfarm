package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/yusufziyrek/antfarm"
	"github.com/yusufziyrek/antfarm/middleware"
)

func main() {
	// A handler that randomly fails
	flakyHandler := func(ctx context.Context, id int) (string, error) {
		if rand.Float32() < 0.5 {
			return "", fmt.Errorf("random error for job %d", id)
		}
		return fmt.Sprintf("Success %d", id), nil
	}

	// Create pool with Middleware:
	// 1. CircuitBreaker (Outer) - protects downstream
	// 2. RateLimit (Middle) - controls throughput
	// 3. Logging (Inner) - logs execution
	pool := antfarm.New(2, flakyHandler,
		antfarm.WithMiddleware(
			middleware.CircuitBreaker[int, string](3, time.Second*2),
			middleware.RateLimit[int, string](10, time.Second),
			middleware.Logging[int, string](nil),
		),
	)

	pool.Start()

	go func() {
		for i := 0; i < 20; i++ {
			pool.Submit(context.Background(), i)
			time.Sleep(100 * time.Millisecond)
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
