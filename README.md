# 🐜 AntFarm

**AntFarm** is a production-ready, generic worker pool for Go that makes concurrency easy, safe, and efficient. It abstracts away the complexities of goroutine management, context propagation, and graceful shutdowns, allowing you to focus on your business logic.

Designed for modern Go applications, AntFarm offers strict compile-time type safety, zero dependencies, and a flexible middleware system to easily add logging, metrics, or retries to your background jobs.

![Go Version](https://img.shields.io/badge/go-1.25+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/yusufziyrek/antfarm)](https://goreportcard.com/report/github.com/yusufziyrek/antfarm)

## 🚀 Why AntFarm?

*   **Type-Safe & Generic:** Leverage Go Generics (`[T, R]`) for strict type checking at compile time. No more runtime casting or `interface{}` risks.
*   **Production Ready:** Built-in support for `context.Context` propagation, graceful shutdowns, and panic recovery (via middleware).
*   **High Performance:** Minimal overhead using standard `sync` primitives and channels.
*   **Developer Friendly:** Simple, fluent API using the Functional Options pattern.
*   **Extensible:** Easily plug in cross-cutting concerns like logging, tracing, or rate limiting using the Middleware pattern.

## 📦 Installation

```bash
go get github.com/yusufziyrek/antfarm
```

## ⚡ Quick Start

```go
package main

import (
	"context"
	"fmt"
	"github.com/yusufziyrek/antfarm"
)

func main() {
	// 1. Define your handler (the work logic)
	handler := func(ctx context.Context, job int) (int, error) {
		return job * 2, nil
	}

	// 2. Create a pool with 3 workers
	pool := antfarm.New(3, handler)
	pool.Start()

	// 3. Submit jobs
	go func() {
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			// Submit now accepts a context for timeout/cancellation
			pool.Submit(ctx, i)
		}
		pool.Shutdown()
	}()

	// 4. Consume results
	for res := range pool.Results() {
		fmt.Printf("Result: %d\n", res.Value)
	}
}
```

> **⚠️ Note on Backpressure:** AntFarm uses blocking channels to ensure data safety and backpressure. You should consume the `Results()` channel (or drain it) to prevent workers from blocking if the result buffer fills up.

## 🛠 Configuration & Middleware

AntFarm uses the **Functional Options** pattern for configuration.

### Buffered Channels

```go
pool := antfarm.New(10, handler, antfarm.WithBufferSize[int, int](100))
```

### Adding Middleware (Logging, Retries, etc.)

You can wrap your handler with middleware to add functionality without changing the core logic.

```go
// Example: A simple logging middleware
func LoggingMiddleware[T any, R any](next antfarm.Handler[T, R]) antfarm.Handler[T, R] {
    return func(ctx context.Context, job T) (R, error) {
        fmt.Println("Job started")
        res, err := next(ctx, job)
        fmt.Println("Job finished")
        return res, err
    }
}

// Usage
pool := antfarm.New(5, handler, 
    antfarm.WithMiddleware(LoggingMiddleware[int, int]),
)
```

See `examples/middleware/` for more advanced examples like **Retries**.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1.  Fork the Project
2.  Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3.  Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4.  Push to the Branch (`git push origin feature/AmazingFeature`)
5.  Open a Pull Request

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.
