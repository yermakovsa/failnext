package rcpx

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
	consec    []int       // consecutive rcpx-defined availability failures
	coolingTo []time.Time // if now is before coolingTo[i], endpoint i is cooling down
}

func newCooldownTracker(n int, cooldown effectiveCooldown) *cooldownTracker {
	ct := &cooldownTracker{
		enabled:   cooldown.enabled,
		threshold: cooldown.threshold,
		duration:  cooldown.duration,
		consec:    make([]int, n),
		coolingTo: make([]time.Time, n),
	}

	// If disabled, keep parameters inert.
	if !ct.enabled {
		ct.threshold = 0
		ct.duration = 0
	}

	return ct
}

func (c *cooldownTracker) validIndex(idx int) bool {
	return idx >= 0 && idx < len(c.coolingTo)
}

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

	// Cooldown expiry is observed lazily at admission. A new streak starts fresh.
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

	// A non-failure response interrupts the failure streak, but an already-active
	// fixed-duration cooldown remains in force until its admission-time expiry.
	c.consec[idx] = 0
}

// recordFailure records one rcpx-defined availability failure. An attempt that
// was admitted before a concurrent cooldown transition may finish while the
// endpoint is already cooling; its evidence is recorded without extending the
// active fixed-duration cooldown.
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

		// The previous cooldown has expired. Start new evidence from a fresh streak.
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

func isCooldownFailureStatus(code int) bool {
	switch code {
	case 502, 503, 504:
		return true
	default:
		return false
	}
}
