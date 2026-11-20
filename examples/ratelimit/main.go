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

type rateLimitState struct {
	mu       sync.Mutex
	until    map[failnext.EndpointID]time.Time
	duration time.Duration
}

func (s *rateLimitState) quarantine(id failnext.EndpointID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	until := time.Now().Add(s.duration)
	if until.After(s.until[id]) {
		s.until[id] = until
	}
}

func (s *rateLimitState) isEligible(id failnext.EndpointID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return !time.Now().Before(s.until[id])
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
		if primaryCalls.Add(1) == 1 {
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, "primary")
	}))
	defer primary.Close()

	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "backup")
	}))
	defer backup.Close()

	// Short duration for this demonstration; applications can choose a longer delay.
	const quarantineDuration = 500 * time.Millisecond
	state := &rateLimitState{
		until:    make(map[failnext.EndpointID]time.Time),
		duration: quarantineDuration,
	}

	tr, err := failnext.New(failnext.Config{
		Endpoints: []failnext.Endpoint{
			{ID: "primary", URL: primary.URL},
			{ID: "backup", URL: backup.URL},
		},
		Eligible:                     state.isEligible,
		AdditionalTriggerStatusCodes: []int{http.StatusTooManyRequests},
		OnEvent: func(_ context.Context, event failnext.Event) {
			if event.Kind != failnext.EventAttempt {
				return
			}
			fmt.Printf("attempt %d: %s (HTTP %d)\n", event.Attempt, event.Endpoint, event.StatusCode)
			if event.StatusCode == http.StatusTooManyRequests {
				// Eligibility is captured once per logical request. This
				// quarantine affects subsequent requests, not in-flight ones.
				state.quarantine(event.Endpoint)
			}
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()

	fmt.Printf("first request: %s\n", get(client, primary.URL))
	fmt.Printf("while primary is quarantined: %s\n", get(client, primary.URL))

	time.Sleep(quarantineDuration + 100*time.Millisecond)
	fmt.Printf("after quarantine expires: %s\n", get(client, primary.URL))
}
