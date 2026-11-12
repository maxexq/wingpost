package ingest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maxexq/wingpost/internal/core"
	"github.com/maxexq/wingpost/internal/ingest"
	"github.com/maxexq/wingpost/internal/memstore"
)

const apiKey = "wp_test_key"

var fixedNow = time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)

// newAPI returns a handler over a store that holds one app with two
// endpoints: one for "home.*" and one for "judge.*".
func newAPI(t *testing.T) (http.Handler, *memstore.Store) {
	t.Helper()
	ctx := context.Background()
	store := memstore.New()
	app, err := store.CreateApp(ctx, "smart-home", core.HashKey(apiKey))
	if err != nil {
		t.Fatal(err)
	}
	for _, pattern := range []string{"home.*", "judge.*"} {
		_, err := store.CreateEndpoint(ctx, core.Endpoint{
			AppID: app.ID, URL: "http://example.invalid/" + pattern, Secret: "s", Patterns: []string{pattern},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	h := &ingest.Handler{Store: store, Now: func() time.Time { return fixedNow }}
	return h.Routes(), store
}

func post(t *testing.T, api http.Handler, key, idempotencyKey, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	var decoded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("response is not JSON: %q", rec.Body.String())
	}
	return rec.Code, decoded
}

func TestPostEventCreatesDeliveryForMatchingEndpointOnly(t *testing.T) {
	api, store := newAPI(t)

	status, body := post(t, api, apiKey, "", `{"type":"home.door.opened","payload":{"room":"bathroom"}}`)

	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%v)", status, body)
	}
	if body["id"] == "" || body["duplicate"] != false {
		t.Errorf("body = %v, want an id and duplicate=false", body)
	}
	deliveries := store.Deliveries()
	if len(deliveries) != 1 {
		t.Fatalf("got %d deliveries, want 1 (only the home.* endpoint matches)", len(deliveries))
	}
	d := deliveries[0]
	if d.Status != memstore.Pending || !d.NextAttemptAt.Equal(fixedNow) || d.Attempts != 0 {
		t.Errorf("delivery = %+v, want pending, due now, 0 attempts", d)
	}
}

func TestPostEventWithRepeatedIdempotencyKeyDoesNotDuplicate(t *testing.T) {
	api, store := newAPI(t)
	body := `{"type":"home.door.opened"}`

	_, first := post(t, api, apiKey, "door-1", body)
	status, second := post(t, api, apiKey, "door-1", body)

	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", status)
	}
	if second["id"] != first["id"] || second["duplicate"] != true {
		t.Errorf("second = %v, want the id %v and duplicate=true", second, first["id"])
	}
	if n := len(store.Deliveries()); n != 1 {
		t.Errorf("got %d deliveries, want 1", n)
	}
}

func TestPostEventWithoutIdempotencyKeyAlwaysCreates(t *testing.T) {
	api, store := newAPI(t)
	body := `{"type":"home.door.opened"}`

	_, first := post(t, api, apiKey, "", body)
	_, second := post(t, api, apiKey, "", body)

	if first["id"] == second["id"] {
		t.Errorf("both requests returned id %v, want two events", first["id"])
	}
	if n := len(store.Deliveries()); n != 2 {
		t.Errorf("got %d deliveries, want 2", n)
	}
}

func TestPostEventRejectsBadRequests(t *testing.T) {
	cases := []struct {
		name, key, body string
		want            int
	}{
		{"no key", "", `{"type":"home.door.opened"}`, http.StatusUnauthorized},
		{"unknown key", "wp_wrong", `{"type":"home.door.opened"}`, http.StatusUnauthorized},
		{"not JSON", apiKey, `type=home`, http.StatusBadRequest},
		{"missing type", apiKey, `{"payload":{}}`, http.StatusBadRequest},
		{"invalid type", apiKey, `{"type":"Home Door"}`, http.StatusBadRequest},
		{"too large", apiKey, `{"type":"home.x","payload":"` + strings.Repeat("a", 300<<10) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			api, store := newAPI(t)
			status, body := post(t, api, c.key, "", c.body)
			if status != c.want {
				t.Errorf("status = %d, want %d (%v)", status, c.want, body)
			}
			if n := len(store.Deliveries()); n != 0 {
				t.Errorf("got %d deliveries, want 0", n)
			}
		})
	}
}

func TestHealthz(t *testing.T) {
	api, _ := newAPI(t)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}
