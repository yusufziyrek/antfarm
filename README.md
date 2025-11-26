# 🐜 AntFarm

**AntFarm** is a high-performance, type-safe, generic worker pool library for Go (Golang). It is designed with **SOLID principles** in mind, offering a clean API, strict type safety via Go Generics, and extensibility through a robust Middleware pattern.

![Go Version](https://img.shields.io/badge/go-1.25+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/yusufziyrek/antfarm)](https://goreportcard.com/report/github.com/yusufziyrek/antfarm)

## 🚀 Features

*   **Type-Safe:** Built with Go 1.25+ Generics (`[T, R]`). No more `interface{}` casting.
*   **High Performance:** Minimal overhead, using standard `sync` and channels.
*   **Extensible:** Add Logging, Metrics, Tracing, or Retries using the **Middleware Pattern**.
*   **Graceful Shutdown:** Ensures all active jobs are completed before exiting.
*   **Simple API:** Functional Options pattern for easy configuration.
*   **Zero Dependencies:** Relies only on the Go Standard Library.

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
		for i := 0; i < 5; i++ {
			pool.Submit(i)
		}
		pool.Shutdown()
	}()

	// 4. Consume results
	for res := range pool.Results() {
		fmt.Printf("Result: %d\n", res.Value)
	}
}
```

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
