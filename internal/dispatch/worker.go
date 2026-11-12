// Package dispatch sends pending deliveries and schedules their retries.
package dispatch

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/maxexq/wingpost/internal/core"
)

// Queue is what the worker needs from storage.
type Queue interface {
	// Claim returns up to limit deliveries that are due at now. It counts
	// the attempt and hides each delivery until leaseUntil, so a worker that
	// dies in the middle of an attempt does not lose the delivery.
	Claim(ctx context.Context, now, leaseUntil time.Time, limit int) ([]core.Job, error)
	Complete(ctx context.Context, deliveryID string, now time.Time) error
	Retry(ctx context.Context, deliveryID string, at time.Time, reason string) error
	// Bury marks a delivery as dead. A dead delivery is never claimed again.
	Bury(ctx context.Context, deliveryID string, now time.Time, reason string) error
}

// Sender delivers one job to its endpoint.
type Sender interface {
	Send(ctx context.Context, job core.Job, now time.Time) error
}

// Worker moves due deliveries from the queue to their endpoints.
type Worker struct {
	Queue    Queue
	Sender   Sender
	Schedule core.Schedule
	Now      func() time.Time
	// Lease is how long a claimed delivery stays hidden from other workers.
	// Set it longer than the sender's timeout.
	Lease time.Duration
	Batch int
	Log   *slog.Logger
}

// Tick claims one batch and attempts every delivery in it.
// It returns the number of deliveries it attempted.
func (w *Worker) Tick(ctx context.Context) (int, error) {
	now := w.Now()
	jobs, err := w.Queue.Claim(ctx, now, now.Add(w.Lease), w.Batch)
	if err != nil {
		return 0, fmt.Errorf("claim deliveries: %w", err)
	}
	for _, job := range jobs {
		if err := w.attempt(ctx, job); err != nil {
			return 0, err
		}
	}
	return len(jobs), nil
}

func (w *Worker) attempt(ctx context.Context, job core.Job) error {
	sendErr := w.Sender.Send(ctx, job, w.Now())
	now := w.Now()
	if sendErr == nil {
		w.log("delivered", job, nil)
		return w.Queue.Complete(ctx, job.DeliveryID, now)
	}
	delay, ok := w.Schedule.Next(job.Attempt)
	if !ok {
		w.log("dead", job, sendErr)
		return w.Queue.Bury(ctx, job.DeliveryID, now, sendErr.Error())
	}
	w.log("retry scheduled", job, sendErr)
	return w.Queue.Retry(ctx, job.DeliveryID, now.Add(delay), sendErr.Error())
}

// Run calls Tick until ctx ends. When a batch is full, Run claims the next
// batch at once. Otherwise it waits for interval.
func (w *Worker) Run(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		n, err := w.Tick(ctx)
		if err != nil && ctx.Err() == nil && w.Log != nil {
			w.Log.Error("tick failed", "error", err)
		}
		if err == nil && n == w.Batch {
			timer.Reset(0)
			continue
		}
		timer.Reset(interval)
	}
}

func (w *Worker) log(msg string, job core.Job, err error) {
	if w.Log == nil {
		return
	}
	attrs := []any{"delivery_id", job.DeliveryID, "event_type", job.EventType, "attempt", job.Attempt}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	w.Log.Info(msg, attrs...)
}
