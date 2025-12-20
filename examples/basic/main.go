package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/yusufziyrek/antfarm"
)

func main() {
	// Define a handler
	handler := func(ctx context.Context, job int) (int, error) {
		// Simulate work
		time.Sleep(100 * time.Millisecond)
		return job * 2, nil
	}

	// Create a pool with 3 workers and a buffer of 5
	// Note: We must specify types [int, int] for the option generic inference if not clear,
	// but usually Go infers it from the New call.
	pool, err := antfarm.New(3, handler, antfarm.WithBufferSize[int, int](5))
	if err != nil {
		log.Fatal(err)
	}

	// Start the pool
	pool.Start()
	fmt.Println("Pool started")

	// Submit jobs in a separate goroutine
	go func() {
		for i := 0; i < 10; i++ {
			if err := pool.Submit(context.Background(), i); err != nil {
				log.Printf("Failed to submit job %d: %v", i, err)
			} else {
				fmt.Printf("Submitted job %d\n", i)
			}
		}
		// Graceful shutdown
		pool.Shutdown()
		fmt.Println("Pool shutdown initiated")
	}()

	// Consume results
	// The loop will exit when resultQueue is closed (which happens after Shutdown completes)
	for res := range pool.Results() {
		if res.Err != nil {
			log.Printf("Job error: %v", res.Err)
		} else {
			fmt.Printf("Result: %d\n", res.Value)
		}
	}
	fmt.Println("All results processed")
}
