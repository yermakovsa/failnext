package failnext

import (
	"sync"
	"time"
)

// cooldownTracker tracks per-endpoint cooldown state and is safe for concurrent use.
type cooldownTracker struct {
	enabled   bool
	threshold int
	duration  time.Duration

	mu        sync.Mutex
	consec    []int       // consecutive failures that count toward cooldown
	coolingTo []time.Time // endpoint is cooling while now is before coolingTo[i]
}

func newCooldownTracker(n int, cooldown effectiveCooldown) *cooldownTracker {
	ct := &cooldownTracker{
		enabled:   cooldown.enabled,
		threshold: cooldown.threshold,
		duration:  cooldown.duration,
		consec:    make([]int, n),
		coolingTo: make([]time.Time, n),
	}

	if !ct.enabled {
		ct.threshold = 0
		ct.duration = 0
	}

	return ct
}

func (c *cooldownTracker) validIndex(idx int) bool {
	return idx >= 0 && idx < len(c.coolingTo)
}

// eligible reports whether cooldown currently allows endpoint idx to be tried.
func (c *cooldownTracker) eligible(now time.Time, idx int) bool {
	if c == nil || !c.enabled {
		return true
	}
	if !c.validIndex(idx) {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	until := c.coolingTo[idx]
	if until.IsZero() {
		return true
	}
	if now.Before(until) {
		return false
	}

	// Clear an expired cooldown when the endpoint is next checked.
	// A new failure streak starts from zero.
	c.coolingTo[idx] = time.Time{}
	c.consec[idx] = 0
	return true
}

func (c *cooldownTracker) recordNonFailure(idx int) {
	if c == nil || !c.enabled {
		return
	}
	if !c.validIndex(idx) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// A result that does not count as a cooldown failure resets the current
	// failure streak. It does not end an active cooldown early.
	c.consec[idx] = 0
}

// recordFailure records one failure that counts toward cooldown.
//
// A concurrent attempt may finish after another request has already put the
// endpoint into cooldown. That failure is recorded without extending the
// active cooldown.
func (c *cooldownTracker) recordFailure(now time.Time, idx int) {
	if c == nil || !c.enabled {
		return
	}
	if !c.validIndex(idx) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if until := c.coolingTo[idx]; !until.IsZero() {
		if now.Before(until) {
			c.consec[idx]++
			return
		}

		// The previous cooldown has expired. Start a new failure streak.
		c.coolingTo[idx] = time.Time{}
		c.consec[idx] = 0
	}

	c.consec[idx]++
	if c.threshold <= 0 {
		return
	}
	if c.consec[idx] >= c.threshold {
		c.consec[idx] = 0
		c.coolingTo[idx] = now.Add(c.duration)
	}
}

// isCooldownFailureStatus reports whether an HTTP status counts toward cooldown.
// This set is fixed and is separate from additional failover trigger status codes.
func isCooldownFailureStatus(code int) bool {
	switch code {
	case 502, 503, 504:
		return true
	default:
		return false
	}
}
