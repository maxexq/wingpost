// Package memstore is an in-memory store with the same behaviour as the
// Postgres store. Tests use it as a fake, and it documents that behaviour.
package memstore

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/maxexq/wingpost/internal/core"
)

// Delivery statuses.
const (
	Pending   = "pending"
	Delivered = "delivered"
	Dead      = "dead"
)

// Delivery is the stored state of one event for one endpoint.
type Delivery struct {
	ID            string
	EventID       string
	EndpointID    string
	Status        string
	Attempts      int
	NextAttemptAt time.Time
	LastError     string
}

type event struct {
	id, appID, eventType, idempotencyKey string
	payload                              []byte
}

// Store keeps everything in maps behind one mutex.
type Store struct {
	mu         sync.Mutex
	seq        int
	apps       map[string]core.App // by key hash
	endpoints  []core.Endpoint
	events     []event
	deliveries []*Delivery
}

// New returns an empty store.
func New() *Store {
	return &Store{apps: map[string]core.App{}}
}

func (s *Store) nextID(prefix string) string {
	s.seq++
	return fmt.Sprintf("%s_%d", prefix, s.seq)
}

// CreateApp registers an app under the hash of its API key.
func (s *Store) CreateApp(_ context.Context, name, keyHash string) (core.App, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	app := core.App{ID: s.nextID("app"), Name: name}
	s.apps[keyHash] = app
	return app, nil
}

// CreateEndpoint registers a destination for an app.
func (s *Store) CreateEndpoint(_ context.Context, ep core.Endpoint) (core.Endpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ep.ID = s.nextID("ep")
	s.endpoints = append(s.endpoints, ep)
	return ep, nil
}

// AppByKeyHash implements ingest.Store.
func (s *Store) AppByKeyHash(_ context.Context, keyHash string) (core.App, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	app, ok := s.apps[keyHash]
	if !ok {
		return core.App{}, core.ErrNotFound
	}
	return app, nil
}

// AcceptEvent implements ingest.Store.
func (s *Store) AcceptEvent(_ context.Context, ev core.NewEvent, now time.Time) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ev.IdempotencyKey != "" {
		for _, e := range s.events {
			if e.appID == ev.AppID && e.idempotencyKey == ev.IdempotencyKey {
				return e.id, false, nil
			}
		}
	}
	e := event{
		id:             s.nextID("evt"),
		appID:          ev.AppID,
		eventType:      ev.Type,
		idempotencyKey: ev.IdempotencyKey,
		payload:        append([]byte(nil), ev.Payload...),
	}
	s.events = append(s.events, e)
	for _, ep := range s.endpoints {
		if ep.AppID != ev.AppID || !core.MatchAny(ep.Patterns, ev.Type) {
			continue
		}
		s.deliveries = append(s.deliveries, &Delivery{
			ID:            s.nextID("dlv"),
			EventID:       e.id,
			EndpointID:    ep.ID,
			Status:        Pending,
			NextAttemptAt: now,
		})
	}
	return e.id, true, nil
}

// Claim implements dispatch.Queue.
func (s *Store) Claim(_ context.Context, now, leaseUntil time.Time, limit int) ([]core.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []*Delivery
	for _, d := range s.deliveries {
		if d.Status == Pending && !d.NextAttemptAt.After(now) {
			due = append(due, d)
		}
	}
	sort.SliceStable(due, func(i, j int) bool { return due[i].NextAttemptAt.Before(due[j].NextAttemptAt) })
	if len(due) > limit {
		due = due[:limit]
	}
	jobs := make([]core.Job, 0, len(due))
	for _, d := range due {
		d.Attempts++
		d.NextAttemptAt = leaseUntil
		ev := s.eventByID(d.EventID)
		ep := s.endpointByID(d.EndpointID)
		jobs = append(jobs, core.Job{
			DeliveryID: d.ID,
			EventID:    ev.id,
			EventType:  ev.eventType,
			URL:        ep.URL,
			Secret:     ep.Secret,
			Payload:    ev.payload,
			Attempt:    d.Attempts,
		})
	}
	return jobs, nil
}

// Complete implements dispatch.Queue.
func (s *Store) Complete(_ context.Context, deliveryID string, _ time.Time) error {
	return s.update(deliveryID, func(d *Delivery) { d.Status = Delivered; d.LastError = "" })
}

// Retry implements dispatch.Queue.
func (s *Store) Retry(_ context.Context, deliveryID string, at time.Time, reason string) error {
	return s.update(deliveryID, func(d *Delivery) { d.NextAttemptAt = at; d.LastError = reason })
}

// Bury implements dispatch.Queue.
func (s *Store) Bury(_ context.Context, deliveryID string, _ time.Time, reason string) error {
	return s.update(deliveryID, func(d *Delivery) { d.Status = Dead; d.LastError = reason })
}

// Deliveries returns a copy of every delivery, oldest first.
func (s *Store) Deliveries() []Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Delivery, len(s.deliveries))
	for i, d := range s.deliveries {
		out[i] = *d
	}
	return out
}

func (s *Store) update(deliveryID string, change func(*Delivery)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.deliveries {
		if d.ID == deliveryID {
			change(d)
			return nil
		}
	}
	return core.ErrNotFound
}

func (s *Store) eventByID(id string) event {
	for _, e := range s.events {
		if e.id == id {
			return e
		}
	}
	return event{}
}

func (s *Store) endpointByID(id string) core.Endpoint {
	for _, ep := range s.endpoints {
		if ep.ID == id {
			return ep
		}
	}
	return core.Endpoint{}
}
