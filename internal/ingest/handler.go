// Package ingest is the HTTP API that accepts events.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/maxexq/wingpost/internal/core"
)

const maxBodyBytes = 256 << 10

// Store is what the handler needs from storage.
type Store interface {
	// AppByKeyHash returns core.ErrNotFound when no app has that key.
	AppByKeyHash(ctx context.Context, keyHash string) (core.App, error)
	// AcceptEvent saves the event and one pending delivery for each endpoint
	// that matches the event type, as one atomic write. A repeated
	// idempotency key returns the first event's id and created = false.
	AcceptEvent(ctx context.Context, ev core.NewEvent, now time.Time) (id string, created bool, err error)
}

// Handler serves the ingest API.
type Handler struct {
	Store Store
	Now   func() time.Time
	Log   *slog.Logger
}

// Routes returns the HTTP routes of the ingest API.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/events", h.postEvent)
	return mux
}

type eventRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type eventResponse struct {
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate"`
}

func (h *Handler) postEvent(w http.ResponseWriter, r *http.Request) {
	key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || key == "" {
		writeError(w, http.StatusUnauthorized, "missing bearer API key")
		return
	}
	app, err := h.Store.AppByKeyHash(r.Context(), core.HashKey(key))
	if errors.Is(err, core.ErrNotFound) {
		writeError(w, http.StatusUnauthorized, "unknown API key")
		return
	}
	if err != nil {
		h.fail(w, "look up app", err)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body is larger than 256 KiB")
		return
	}
	var req eventRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "body is not valid JSON")
		return
	}
	if !core.ValidEventType(req.Type) {
		writeError(w, http.StatusBadRequest, `"type" must be dot-separated segments of a-z, 0-9, "_" or "-"`)
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage("{}")
	}

	id, created, err := h.Store.AcceptEvent(r.Context(), core.NewEvent{
		AppID:          app.ID,
		Type:           req.Type,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Payload:        req.Payload,
	}, h.Now())
	if err != nil {
		h.fail(w, "accept event", err)
		return
	}
	writeJSON(w, http.StatusAccepted, eventResponse{ID: id, Duplicate: !created})
}

func (h *Handler) fail(w http.ResponseWriter, what string, err error) {
	if h.Log != nil {
		h.Log.Error(what, "error", err)
	}
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
