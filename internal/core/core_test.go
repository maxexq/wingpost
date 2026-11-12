package core

import (
	"testing"
	"time"
)

func TestMatch(t *testing.T) {
	cases := []struct {
		pattern, eventType string
		want               bool
	}{
		{"*", "home.door.opened", true},
		{"home.door.opened", "home.door.opened", true},
		{"home.door.opened", "home.door.closed", false},
		{"home.*", "home.door.opened", true},
		{"home.*", "home", false},
		{"home.*", "homework.done", false},
		{"judge.*", "home.door.opened", false},
	}
	for _, c := range cases {
		if got := Match(c.pattern, c.eventType); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.pattern, c.eventType, got, c.want)
		}
	}
}

func TestValidEventType(t *testing.T) {
	valid := []string{"home.door.opened", "judge.submission-passed", "site_down", "a.b2"}
	for _, s := range valid {
		if !ValidEventType(s) {
			t.Errorf("ValidEventType(%q) = false, want true", s)
		}
	}
	invalid := []string{"", ".", "home.", ".home", "home..door", "Home.Door", "home door", "home.*"}
	for _, s := range invalid {
		if ValidEventType(s) {
			t.Errorf("ValidEventType(%q) = true, want false", s)
		}
	}
}

func TestScheduleNext(t *testing.T) {
	s := Schedule{10 * time.Second, time.Minute}
	cases := []struct {
		failedAttempt int
		want          time.Duration
		ok            bool
	}{
		{0, 0, false},
		{1, 10 * time.Second, true},
		{2, time.Minute, true},
		{3, 0, false},
	}
	for _, c := range cases {
		got, ok := s.Next(c.failedAttempt)
		if got != c.want || ok != c.ok {
			t.Errorf("Next(%d) = %v, %v, want %v, %v", c.failedAttempt, got, ok, c.want, c.ok)
		}
	}
}

func TestSignAndVerify(t *testing.T) {
	sentAt := time.Unix(1_700_000_000, 0)
	body := []byte(`{"type":"home.door.opened"}`)
	header := Sign("secret", sentAt, body)

	if !Verify("secret", header, sentAt, body) {
		t.Fatal("Verify rejected a signature that Sign produced")
	}
	if Verify("other-secret", header, sentAt, body) {
		t.Error("Verify accepted a different secret")
	}
	if Verify("secret", header, sentAt.Add(time.Second), body) {
		t.Error("Verify accepted a different timestamp")
	}
	if Verify("secret", header, sentAt, append(body, ' ')) {
		t.Error("Verify accepted a different body")
	}
}

func TestHashKeyIsStable(t *testing.T) {
	if HashKey("wp_abc") != HashKey("wp_abc") {
		t.Error("HashKey returned two values for one key")
	}
	if HashKey("wp_abc") == HashKey("wp_abd") {
		t.Error("HashKey returned one value for two keys")
	}
}
