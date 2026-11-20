package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yermakovsa/failnext"
)

type endpointLatency struct {
	slowAttempts int
	until        time.Time
}

type latencyState struct {
	mu                 sync.Mutex
	endpoints          map[failnext.EndpointID]endpointLatency
	threshold          time.Duration
	consecutive        int
	quarantineDuration time.Duration
}

func (s *latencyState) observe(id failnext.EndpointID, duration time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	current := s.endpoints[id]
	if !current.until.IsZero() {
		if now.Before(current.until) {
			return false
		}
		current = endpointLatency{}
	}

	if duration > s.threshold {
		current.slowAttempts++
	} else {
		current.slowAttempts = 0
	}

	if current.slowAttempts >= s.consecutive {
		current.slowAttempts = 0
		current.until = now.Add(s.quarantineDuration)
		s.endpoints[id] = current
		return true
	}

	s.endpoints[id] = current
	return false
}

func (s *latencyState) isEligible(id failnext.EndpointID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.endpoints[id]
	if current.until.IsZero() {
		return true
	}
	if time.Now().Before(current.until) {
		return false
	}
	delete(s.endpoints, id) // Expired quarantine starts a new slow streak.
	return true
}

func get(client *http.Client, url string) string {
	resp, err := client.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("unexpected HTTP status: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	return string(body)
}

func main() {
	var primaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if primaryCalls.Add(1) <= 3 {
			time.Sleep(60 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "primary")
	}))
	defer primary.Close()

	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "backup")
	}))
	defer backup.Close()

	// Short durations for the demonstration; applications can use longer limits.
	const (
		slowThreshold      = 30 * time.Millisecond
		consecutiveSlow    = 3
		quarantineDuration = 300 * time.Millisecond
	)
	state := &latencyState{
		endpoints:          make(map[failnext.EndpointID]endpointLatency),
		threshold:          slowThreshold,
		consecutive:        consecutiveSlow,
		quarantineDuration: quarantineDuration,
	}

	tr, err := failnext.New(failnext.Config{
		Endpoints: []failnext.Endpoint{
			{ID: "primary", URL: primary.URL},
			{ID: "backup", URL: backup.URL},
		},
		Eligible: state.isEligible,
		OnEvent: func(ctx context.Context, event failnext.Event) {
			if event.Kind != failnext.EventAttempt {
				return
			}
			fmt.Printf("attempt %d: %s (%s)\n", event.Attempt, event.Endpoint, event.Duration.Round(time.Microsecond))

			// Only successful attempts from active requests affect the streak.
			if ctx.Err() != nil || event.Err != nil || event.StatusCode != http.StatusOK {
				return
			}
			if state.observe(event.Endpoint, event.Duration) {
				fmt.Printf("%s quarantined after %d slow attempts\n", event.Endpoint, consecutiveSlow)
			}
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()

	for i := 1; i <= consecutiveSlow; i++ {
		fmt.Printf("request %d: %s\n", i, get(client, primary.URL))
	}
	// Eligibility is captured once per logical request; quarantine affects later requests.
	fmt.Printf("while primary is quarantined: %s\n", get(client, primary.URL))

	time.Sleep(quarantineDuration + 100*time.Millisecond)
	fmt.Printf("after quarantine expires: %s\n", get(client, primary.URL))
}
