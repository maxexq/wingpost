// Package core holds the types and rules that every other package shares.
// It imports nothing from the rest of the module.
package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
)

// ErrNotFound reports that a lookup matched no row.
var ErrNotFound = errors.New("not found")

// App is a project that sends events to Wingpost.
type App struct {
	ID   string
	Name string
}

// Endpoint is one destination that receives the events of an app.
type Endpoint struct {
	ID       string
	AppID    string
	URL      string
	Secret   string
	Patterns []string
}

// NewEvent is an event as the ingest API accepts it.
type NewEvent struct {
	AppID          string
	Type           string
	IdempotencyKey string
	Payload        json.RawMessage
}

// Job is one delivery attempt: one event for one endpoint.
type Job struct {
	DeliveryID string
	EventID    string
	EventType  string
	URL        string
	Secret     string
	Payload    json.RawMessage
	// Attempt counts from 1 and includes the attempt in progress.
	Attempt int
}

// HashKey returns the form in which an API key is stored and looked up.
func HashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Match reports whether an event type matches an endpoint pattern.
// A pattern is "*", an exact type, or a prefix that ends in ".*".
func Match(pattern, eventType string) bool {
	if pattern == "*" {
		return true
	}
	if prefix, ok := strings.CutSuffix(pattern, ".*"); ok {
		return strings.HasPrefix(eventType, prefix+".")
	}
	return pattern == eventType
}

// MatchAny reports whether an event type matches at least one pattern.
func MatchAny(patterns []string, eventType string) bool {
	for _, p := range patterns {
		if Match(p, eventType) {
			return true
		}
	}
	return false
}

// ValidEventType reports whether s is a usable event type:
// lowercase letters, digits, "_", "-", and "." between segments.
func ValidEventType(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for _, segment := range strings.Split(s, ".") {
		if segment == "" {
			return false
		}
		for _, r := range segment {
			ok := r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-'
			if !ok {
				return false
			}
		}
	}
	return true
}
