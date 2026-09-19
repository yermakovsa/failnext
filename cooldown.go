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
	consec    []int       // consecutive failover-causing failures
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
	return until.IsZero() || !now.Before(until)
}

func (c *cooldownTracker) recordSuccess(idx int) {
	if c == nil || !c.enabled {
		return
	}
	if !c.validIndex(idx) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.consec[idx] = 0
	c.coolingTo[idx] = time.Time{}
}

// recordFailoverFailure records a failure for endpoint idx that caused rcpx to
// try another endpoint. When consecutive failures reach the configured
// threshold, the endpoint cools down for the configured duration.
func (c *cooldownTracker) recordFailoverFailure(now time.Time, idx int) {
	if c == nil || !c.enabled {
		return
	}
	if !c.validIndex(idx) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Don't count failures during cooldown; we shouldn't be attempting them.
	if until := c.coolingTo[idx]; !until.IsZero() && now.Before(until) {
		return
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
