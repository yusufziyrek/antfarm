package middleware_test

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/yusufziyrek/antfarm/middleware"
)

func TestLogging(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	handler := func(ctx context.Context, job int) (int, error) {
		return job, nil
	}

	wrapped := middleware.Logging[int, int](logger)(handler)
	_, _ = wrapped(context.Background(), 1)

	output := buf.String()
	if !strings.Contains(output, "Job started") {
		t.Error("expected log to contain 'Job started'")
	}
	if !strings.Contains(output, "Job completed") {
		t.Error("expected log to contain 'Job completed'")
	}
}

func TestRateLimit(t *testing.T) {
	limit := 5
	window := time.Second
	handler := func(ctx context.Context, job int) (int, error) {
		return job, nil
	}

	wrapped := middleware.RateLimit[int, int](limit, window)(handler)

	// Consume limit
	for i := 0; i < limit; i++ {
		if _, err := wrapped(context.Background(), i); err != nil {
			t.Errorf("unexpected error on iteration %d: %v", i, err)
		}
	}

	// Exceed limit
	if _, err := wrapped(context.Background(), limit+1); err != middleware.ErrRateLimitExceeded {
		t.Errorf("expected ErrRateLimitExceeded, got %v", err)
	}
}

func TestCircuitBreaker(t *testing.T) {
	threshold := 2
	timeout := 100 * time.Millisecond

	// Fail handler
	failHandler := func(ctx context.Context, job int) (int, error) {
		return 0, errors.New("fail")
	}

	wrapped := middleware.CircuitBreaker[int, int](threshold, timeout)(failHandler)

	// Trigger failures
	for i := 0; i < threshold; i++ {
		wrapped(context.Background(), i)
	}

	// Circuit should be open now
	if _, err := wrapped(context.Background(), 0); err != middleware.ErrCircuitOpen {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}

	// Wait for timeout (Half-Open)
	time.Sleep(timeout + 10*time.Millisecond)

	// Next call should go through (and fail again, re-opening circuit)
	if _, err := wrapped(context.Background(), 0); err == middleware.ErrCircuitOpen {
		t.Error("expected call to go through in Half-Open state")
	}
}
