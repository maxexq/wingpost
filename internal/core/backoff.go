package core

import "time"

// Schedule lists the delay before each retry.
type Schedule []time.Duration

// DefaultSchedule allows six attempts over about two and a half hours.
var DefaultSchedule = Schedule{
	10 * time.Second,
	time.Minute,
	5 * time.Minute,
	30 * time.Minute,
	2 * time.Hour,
}

// Next returns the delay before the retry that follows a failed attempt.
// Attempts count from 1. Next returns false when no retry remains.
func (s Schedule) Next(failedAttempt int) (time.Duration, bool) {
	if failedAttempt < 1 || failedAttempt > len(s) {
		return 0, false
	}
	return s[failedAttempt-1], true
}
