// Package postgres stores apps, endpoints, events and deliveries in
// PostgreSQL. The deliveries table is also the queue.
package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/maxexq/wingpost/internal/core"
)

//go:embed schema.sql
var schema string

// Store implements ingest.Store and dispatch.Queue.
type Store struct {
	db *sql.DB
}

// Open connects to the database at url and checks the connection.
func Open(ctx context.Context, url string) (*Store, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the connection pool.
func (s *Store) Close() error { return s.db.Close() }

// Migrate creates the tables and indexes that do not exist yet.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

// CreateApp registers an app under the hash of its API key.
func (s *Store) CreateApp(ctx context.Context, name, keyHash string) (core.App, error) {
	app := core.App{Name: name}
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO apps (name, key_hash) VALUES ($1, $2) RETURNING id`,
		name, keyHash).Scan(&app.ID)
	return app, err
}

// CreateEndpoint registers a destination for an app.
func (s *Store) CreateEndpoint(ctx context.Context, ep core.Endpoint) (core.Endpoint, error) {
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO endpoints (app_id, url, secret, patterns) VALUES ($1, $2, $3, $4) RETURNING id`,
		ep.AppID, ep.URL, ep.Secret, pq.Array(ep.Patterns)).Scan(&ep.ID)
	return ep, err
}

// AppByKeyHash implements ingest.Store.
func (s *Store) AppByKeyHash(ctx context.Context, keyHash string) (core.App, error) {
	var app core.App
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name FROM apps WHERE key_hash = $1`, keyHash).Scan(&app.ID, &app.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return core.App{}, core.ErrNotFound
	}
	return app, err
}

// AcceptEvent implements ingest.Store. The event and its deliveries commit
// together, so an accepted event always has its deliveries queued.
func (s *Store) AcceptEvent(ctx context.Context, ev core.NewEvent, now time.Time) (string, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()

	var id string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO events (app_id, type, payload, idempotency_key, created_at)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5)
		ON CONFLICT (app_id, idempotency_key) DO NOTHING
		RETURNING id`,
		ev.AppID, ev.Type, string(ev.Payload), ev.IdempotencyKey, now).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx,
			`SELECT id FROM events WHERE app_id = $1 AND idempotency_key = $2`,
			ev.AppID, ev.IdempotencyKey).Scan(&id)
		if err != nil {
			return "", false, fmt.Errorf("find event for repeated idempotency key: %w", err)
		}
		return id, false, tx.Commit()
	}
	if err != nil {
		return "", false, fmt.Errorf("insert event: %w", err)
	}

	endpointIDs, err := matchingEndpoints(ctx, tx, ev.AppID, ev.Type)
	if err != nil {
		return "", false, err
	}
	for _, endpointID := range endpointIDs {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO deliveries (event_id, endpoint_id, next_attempt_at, updated_at)
			VALUES ($1, $2, $3, $3)`, id, endpointID, now)
		if err != nil {
			return "", false, fmt.Errorf("insert delivery: %w", err)
		}
	}
	return id, true, tx.Commit()
}

func matchingEndpoints(ctx context.Context, tx *sql.Tx, appID, eventType string) ([]string, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT id, patterns FROM endpoints WHERE app_id = $1 AND enabled ORDER BY created_at, id`, appID)
	if err != nil {
		return nil, fmt.Errorf("list endpoints: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		var patterns []string
		if err := rows.Scan(&id, pq.Array(&patterns)); err != nil {
			return nil, err
		}
		if core.MatchAny(patterns, eventType) {
			ids = append(ids, id)
		}
	}
	return ids, rows.Err()
}

// Claim implements dispatch.Queue. FOR UPDATE SKIP LOCKED lets several
// workers claim at the same time without waiting for each other and without
// taking the same row.
func (s *Store) Claim(ctx context.Context, now, leaseUntil time.Time, limit int) ([]core.Job, error) {
	rows, err := s.db.QueryContext(ctx, `
		WITH due AS (
			SELECT id FROM deliveries
			WHERE status = 'pending' AND next_attempt_at <= $1
			ORDER BY next_attempt_at
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE deliveries d
			SET attempts = d.attempts + 1, next_attempt_at = $2, updated_at = $1
			FROM due
			WHERE d.id = due.id
			RETURNING d.id, d.event_id, d.endpoint_id, d.attempts
		)
		SELECT c.id, c.event_id, e.type, e.payload, ep.url, ep.secret, c.attempts
		FROM claimed c
		JOIN events e ON e.id = c.event_id
		JOIN endpoints ep ON ep.id = c.endpoint_id
		ORDER BY e.created_at, c.id`, now, leaseUntil, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []core.Job
	for rows.Next() {
		var job core.Job
		var payload []byte
		if err := rows.Scan(&job.DeliveryID, &job.EventID, &job.EventType, &payload, &job.URL, &job.Secret, &job.Attempt); err != nil {
			return nil, err
		}
		job.Payload = payload
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// Complete implements dispatch.Queue.
func (s *Store) Complete(ctx context.Context, deliveryID string, now time.Time) error {
	return s.updateDelivery(ctx,
		`UPDATE deliveries SET status = 'delivered', last_error = '', updated_at = $2 WHERE id = $1`,
		deliveryID, now)
}

// Retry implements dispatch.Queue.
func (s *Store) Retry(ctx context.Context, deliveryID string, at time.Time, reason string) error {
	return s.updateDelivery(ctx,
		`UPDATE deliveries SET next_attempt_at = $2, last_error = $3, updated_at = now() WHERE id = $1`,
		deliveryID, at, reason)
}

// Bury implements dispatch.Queue.
func (s *Store) Bury(ctx context.Context, deliveryID string, now time.Time, reason string) error {
	return s.updateDelivery(ctx,
		`UPDATE deliveries SET status = 'dead', last_error = $3, updated_at = $2 WHERE id = $1`,
		deliveryID, now, reason)
}

func (s *Store) updateDelivery(ctx context.Context, query string, args ...any) error {
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return core.ErrNotFound
	}
	return nil
}
