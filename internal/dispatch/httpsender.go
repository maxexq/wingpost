package dispatch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/maxexq/wingpost/internal/core"
)

// HTTPSender delivers a job as a signed HTTP POST.
type HTTPSender struct {
	Client *http.Client
}

// Send posts the event payload to the endpoint URL.
// Any status outside 200-299 is an error.
func (s *HTTPSender) Send(ctx context.Context, job core.Job, now time.Time) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.URL, bytes.NewReader(job.Payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "wingpost")
	req.Header.Set("X-Wingpost-Event", job.EventType)
	req.Header.Set("X-Wingpost-Event-Id", job.EventID)
	req.Header.Set("X-Wingpost-Delivery-Id", job.DeliveryID)
	req.Header.Set("X-Wingpost-Attempt", strconv.Itoa(job.Attempt))
	req.Header.Set("X-Wingpost-Timestamp", strconv.FormatInt(now.Unix(), 10))
	req.Header.Set("X-Wingpost-Signature", core.Sign(job.Secret, now, job.Payload))

	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("endpoint answered %s", resp.Status)
	}
	return nil
}
