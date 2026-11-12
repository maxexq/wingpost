package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/maxexq/wingpost/internal/core"
)

// These tests need a real database. Set WINGPOST_TEST_DATABASE_URL to run
// them. Each test creates its own app, so the tests do not see each other's
// events. Claim is global, so the tests that call Claim do not run in
// parallel and use a clock far from the other tests.

func openTestStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("WINGPOST_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("WINGPOST_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	store, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// Leftover pending rows from an earlier run would be claimed by this run.
	if _, err := store.db.ExecContext(ctx, `DELETE FROM deliveries`); err != nil {
		t.Fatal(err)
	}
	return store
}

func newApp(t *testing.T, store *Store, patterns ...string) core.App {
	t.Helper()
	ctx := context.Background()
	app, err := store.CreateApp(ctx, t.Name(), core.HashKey(rand.Text()))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range patterns {
		if _, err := store.CreateEndpoint(ctx, core.Endpoint{
			AppID: app.ID, URL: "http://example.invalid/" + p, Secret: "s", Patterns: []string{p},
		}); err != nil {
			t.Fatal(err)
		}
	}
	return app
}

var testNow = time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)

func TestMigrateTwice(t *testing.T) {
	store := openTestStore(t)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

func TestAppByKeyHash(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	hash := core.HashKey(rand.Text())
	created, err := store.CreateApp(ctx, "lookup", hash)
	if err != nil {
		t.Fatal(err)
	}

	found, err := store.AppByKeyHash(ctx, hash)
	if err != nil || found != created {
		t.Errorf("AppByKeyHash = %+v, %v, want %+v", found, err, created)
	}
	if _, err := store.AppByKeyHash(ctx, core.HashKey("no-such-key")); !errors.Is(err, core.ErrNotFound) {
		t.Errorf("AppByKeyHash for an unknown key = %v, want ErrNotFound", err)
	}
}

func TestAcceptEventIsIdempotentPerKey(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	app := newApp(t, store, "home.*", "judge.*")
	ev := core.NewEvent{AppID: app.ID, Type: "home.door.opened", IdempotencyKey: "door-1", Payload: []byte(`{"room":"bathroom"}`)}

	firstID, created, err := store.AcceptEvent(ctx, ev, testNow)
	if err != nil || !created {
		t.Fatalf("first AcceptEvent = %q, %v, %v, want created", firstID, created, err)
	}
	secondID, created, err := store.AcceptEvent(ctx, ev, testNow)
	if err != nil || created || secondID != firstID {
		t.Fatalf("second AcceptEvent = %q, %v, %v, want %q and not created", secondID, created, err, firstID)
	}

	var n int
	if err := store.db.QueryRowContext(ctx,
		`SELECT count(*) FROM deliveries WHERE event_id = $1`, firstID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("got %d deliveries, want 1 (only home.* matches, and the repeat adds none)", n)
	}
}

func TestClaimRetryCompleteAndBury(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	app := newApp(t, store, "*")
	if _, _, err := store.AcceptEvent(ctx, core.NewEvent{
		AppID: app.ID, Type: "home.door.opened", Payload: []byte(`{"room":"bathroom"}`),
	}, testNow); err != nil {
		t.Fatal(err)
	}
	lease := 30 * time.Second

	jobs, err := store.Claim(ctx, testNow, testNow.Add(lease), 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("Claim = %d jobs, %v, want 1", len(jobs), err)
	}
	job := jobs[0]
	if job.Attempt != 1 || job.EventType != "home.door.opened" || job.Secret != "s" || job.URL == "" {
		t.Errorf("job = %+v", job)
	}

	// The lease hides the delivery, and it returns when the lease ends.
	if again, _ := store.Claim(ctx, testNow.Add(lease-time.Second), testNow.Add(2*lease), 10); len(again) != 0 {
		t.Fatalf("Claim during the lease returned %d jobs, want 0", len(again))
	}
	jobs, err = store.Claim(ctx, testNow.Add(lease), testNow.Add(2*lease), 10)
	if err != nil || len(jobs) != 1 || jobs[0].Attempt != 2 {
		t.Fatalf("Claim after the lease = %+v, %v, want attempt 2", jobs, err)
	}

	// Retry makes the delivery due at the retry time and not before.
	retryAt := testNow.Add(time.Hour)
	if err := store.Retry(ctx, job.DeliveryID, retryAt, "endpoint answered 503"); err != nil {
		t.Fatal(err)
	}
	if early, _ := store.Claim(ctx, retryAt.Add(-time.Second), retryAt.Add(lease), 10); len(early) != 0 {
		t.Fatalf("Claim before the retry time returned %d jobs, want 0", len(early))
	}
	jobs, err = store.Claim(ctx, retryAt, retryAt.Add(lease), 10)
	if err != nil || len(jobs) != 1 || jobs[0].Attempt != 3 {
		t.Fatalf("Claim at the retry time = %+v, %v, want attempt 3", jobs, err)
	}

	// A completed delivery is never claimed again.
	if err := store.Complete(ctx, job.DeliveryID, retryAt); err != nil {
		t.Fatal(err)
	}
	if after, _ := store.Claim(ctx, retryAt.Add(24*time.Hour), retryAt.Add(25*time.Hour), 10); len(after) != 0 {
		t.Errorf("Claim after Complete returned %d jobs, want 0", len(after))
	}

	if err := store.Bury(ctx, "00000000-0000-0000-0000-000000000000", testNow, "x"); !errors.Is(err, core.ErrNotFound) {
		t.Errorf("Bury for an unknown delivery = %v, want ErrNotFound", err)
	}
}

func TestBuriedDeliveryIsNotClaimed(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	app := newApp(t, store, "*")
	if _, _, err := store.AcceptEvent(ctx, core.NewEvent{AppID: app.ID, Type: "site.down", Payload: []byte(`{}`)}, testNow); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.Claim(ctx, testNow, testNow.Add(time.Second), 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("Claim = %d jobs, %v, want 1", len(jobs), err)
	}

	if err := store.Bury(ctx, jobs[0].DeliveryID, testNow, "gave up"); err != nil {
		t.Fatal(err)
	}

	if after, _ := store.Claim(ctx, testNow.Add(24*time.Hour), testNow.Add(25*time.Hour), 10); len(after) != 0 {
		t.Errorf("Claim after Bury returned %d jobs, want 0", len(after))
	}
}

func TestConcurrentClaimsNeverShareADelivery(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	app := newApp(t, store, "*")
	const events = 40
	for i := 0; i < events; i++ {
		if _, _, err := store.AcceptEvent(ctx, core.NewEvent{AppID: app.ID, Type: "site.down", Payload: []byte(`{}`)}, testNow); err != nil {
			t.Fatal(err)
		}
	}

	const workers = 8
	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		wg      sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				jobs, err := store.Claim(ctx, testNow, testNow.Add(time.Hour), 3)
				if err != nil {
					t.Error(err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, job := range jobs {
					claimed[job.DeliveryID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(claimed) != events {
		t.Errorf("workers claimed %d deliveries, want %d", len(claimed), events)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Errorf("delivery %s was claimed %d times, want 1", id, n)
		}
	}
}
