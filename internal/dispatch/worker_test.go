package dispatch_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/maxexq/wingpost/internal/core"
	"github.com/maxexq/wingpost/internal/dispatch"
	"github.com/maxexq/wingpost/internal/memstore"
)

// clock is a hand-set clock. Tests move it forward. Nothing sleeps.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// scriptedSender returns the next scripted error for each call.
// When the script is used up, every call succeeds.
type scriptedSender struct {
	script []error
	sent   []core.Job
}

func (s *scriptedSender) Send(_ context.Context, job core.Job, _ time.Time) error {
	s.sent = append(s.sent, job)
	if len(s.script) == 0 {
		return nil
	}
	err := s.script[0]
	s.script = s.script[1:]
	return err
}

var errDown = errors.New("endpoint is down")

type fixture struct {
	store  *memstore.Store
	sender *scriptedSender
	clock  *clock
	worker *dispatch.Worker
}

// newFixture returns a worker over a store that holds one pending delivery.
func newFixture(t *testing.T, script ...error) *fixture {
	t.Helper()
	ctx := context.Background()
	c := &clock{now: time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)}
	store := memstore.New()
	app, _ := store.CreateApp(ctx, "smart-home", core.HashKey("k"))
	_, _ = store.CreateEndpoint(ctx, core.Endpoint{
		AppID: app.ID, URL: "http://example.invalid/hook", Secret: "s", Patterns: []string{"*"},
	})
	if _, _, err := store.AcceptEvent(ctx, core.NewEvent{
		AppID: app.ID, Type: "home.door.opened", Payload: []byte(`{"room":"bathroom"}`),
	}, c.now); err != nil {
		t.Fatal(err)
	}
	sender := &scriptedSender{script: script}
	return &fixture{
		store:  store,
		sender: sender,
		clock:  c,
		worker: &dispatch.Worker{
			Queue:    store,
			Sender:   sender,
			Schedule: core.Schedule{10 * time.Second, time.Minute},
			Now:      c.Now,
			Lease:    30 * time.Second,
			Batch:    10,
		},
	}
}

func (f *fixture) tick(t *testing.T) int {
	t.Helper()
	n, err := f.worker.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (f *fixture) delivery(t *testing.T) memstore.Delivery {
	t.Helper()
	all := f.store.Deliveries()
	if len(all) != 1 {
		t.Fatalf("got %d deliveries, want 1", len(all))
	}
	return all[0]
}

func TestTickDeliversPendingDelivery(t *testing.T) {
	f := newFixture(t)

	if n := f.tick(t); n != 1 {
		t.Fatalf("Tick attempted %d deliveries, want 1", n)
	}

	d := f.delivery(t)
	if d.Status != memstore.Delivered || d.Attempts != 1 {
		t.Errorf("delivery = %+v, want delivered after 1 attempt", d)
	}
	job := f.sender.sent[0]
	if job.EventType != "home.door.opened" || string(job.Payload) != `{"room":"bathroom"}` || job.Attempt != 1 {
		t.Errorf("sent job = %+v", job)
	}
	if n := f.tick(t); n != 0 {
		t.Errorf("second Tick attempted %d deliveries, want 0", n)
	}
}

func TestFailedDeliveryWaitsForItsRetryTime(t *testing.T) {
	f := newFixture(t, errDown)
	start := f.clock.now

	f.tick(t)

	d := f.delivery(t)
	if d.Status != memstore.Pending || d.LastError != errDown.Error() {
		t.Fatalf("delivery = %+v, want pending with the send error", d)
	}
	if want := start.Add(10 * time.Second); !d.NextAttemptAt.Equal(want) {
		t.Fatalf("next attempt at %v, want %v", d.NextAttemptAt, want)
	}

	f.clock.Advance(9 * time.Second)
	if n := f.tick(t); n != 0 {
		t.Fatalf("Tick attempted %d deliveries 1s before the retry time, want 0", n)
	}

	f.clock.Advance(time.Second)
	if n := f.tick(t); n != 1 {
		t.Fatalf("Tick attempted %d deliveries at the retry time, want 1", n)
	}
	if d := f.delivery(t); d.Status != memstore.Delivered || d.Attempts != 2 {
		t.Errorf("delivery = %+v, want delivered after 2 attempts", d)
	}
}

func TestDeliveryIsBuriedWhenScheduleIsUsedUp(t *testing.T) {
	f := newFixture(t, errDown, errDown, errDown)

	f.tick(t) // attempt 1 fails, retry in 10s
	f.clock.Advance(10 * time.Second)
	f.tick(t) // attempt 2 fails, retry in 1m
	f.clock.Advance(time.Minute)
	f.tick(t) // attempt 3 fails, schedule has 2 retries, so bury

	d := f.delivery(t)
	if d.Status != memstore.Dead || d.Attempts != 3 {
		t.Fatalf("delivery = %+v, want dead after 3 attempts", d)
	}
	f.clock.Advance(24 * time.Hour)
	if n := f.tick(t); n != 0 {
		t.Errorf("Tick attempted %d dead deliveries, want 0", n)
	}
}

func TestClaimedDeliveryReturnsAfterLease(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// Another worker claims the delivery, then dies before it reports.
	jobs, err := f.store.Claim(ctx, f.clock.now, f.clock.now.Add(30*time.Second), 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("Claim = %d jobs, %v, want 1 job", len(jobs), err)
	}

	if n := f.tick(t); n != 0 {
		t.Fatalf("Tick attempted %d deliveries during the lease, want 0", n)
	}
	f.clock.Advance(30 * time.Second)
	if n := f.tick(t); n != 1 {
		t.Fatalf("Tick attempted %d deliveries after the lease, want 1", n)
	}
	if d := f.delivery(t); d.Status != memstore.Delivered || d.Attempts != 2 {
		t.Errorf("delivery = %+v, want delivered on attempt 2", d)
	}
}

func TestHTTPSenderSignsTheRequest(t *testing.T) {
	sentAt := time.Unix(1_700_000_000, 0)
	payload := []byte(`{"room":"bathroom"}`)
	var got *http.Request
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sender := &dispatch.HTTPSender{Client: server.Client()}
	err := sender.Send(context.Background(), core.Job{
		DeliveryID: "dlv_1", EventID: "evt_1", EventType: "home.door.opened",
		URL: server.URL, Secret: "s3cret", Payload: payload, Attempt: 2,
	}, sentAt)

	if err != nil {
		t.Fatal(err)
	}
	if string(gotBody) != string(payload) {
		t.Errorf("body = %q, want %q", gotBody, payload)
	}
	want := map[string]string{
		"X-Wingpost-Event":       "home.door.opened",
		"X-Wingpost-Event-Id":    "evt_1",
		"X-Wingpost-Delivery-Id": "dlv_1",
		"X-Wingpost-Attempt":     "2",
		"X-Wingpost-Timestamp":   strconv.FormatInt(sentAt.Unix(), 10),
	}
	for header, value := range want {
		if got.Header.Get(header) != value {
			t.Errorf("%s = %q, want %q", header, got.Header.Get(header), value)
		}
	}
	if !core.Verify("s3cret", got.Header.Get("X-Wingpost-Signature"), sentAt, gotBody) {
		t.Error("the receiver cannot verify X-Wingpost-Signature")
	}
}

func TestHTTPSenderTreatsNon2xxAsFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	sender := &dispatch.HTTPSender{Client: server.Client()}
	err := sender.Send(context.Background(), core.Job{URL: server.URL, Payload: []byte(`{}`)}, time.Unix(0, 0))

	if err == nil {
		t.Fatal("Send returned nil for a 503 answer")
	}
}
