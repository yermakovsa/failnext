package failnext

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOnEvent_IsSynchronous(t *testing.T) {
	u1 := "https://u1.test/rpc"
	entered := make(chan struct{}, 1)
	release := make(chan struct{})

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return httpResp(http.StatusOK, "ok"), nil
		}),
		OnEvent: func(_ context.Context, event Event) {
			if event.Kind != EventAttempt {
				return
			}
			entered <- struct{}{}
			<-release
		},
	})

	req := newTestGETRequest(t, u1)
	done := make(chan error, 1)
	go func() {
		resp, err := rt.RoundTrip(req)
		if resp != nil {
			resp.Body.Close()
		}
		done <- err
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("OnEvent was not invoked")
	}

	select {
	case err := <-done:
		close(release)
		t.Fatalf("RoundTrip returned before synchronous OnEvent completed: %v", err)
	default:
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RoundTrip error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RoundTrip did not complete after OnEvent returned")
	}
}

func TestOnEvent_MayRunConcurrentlyAcrossLogicalRequests(t *testing.T) {
	u1 := "https://u1.test/rpc"
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var attemptEvents atomic.Int32

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return httpResp(http.StatusOK, "ok"), nil
		}),
		OnEvent: func(_ context.Context, event Event) {
			if event.Kind != EventAttempt {
				return
			}
			attemptEvents.Add(1)
			entered <- struct{}{}
			<-release
		},
	})

	requests := []*http.Request{
		newTestGETRequest(t, u1),
		newTestGETRequest(t, u1),
	}
	errCh := make(chan error, len(requests))
	var wg sync.WaitGroup
	for _, req := range requests {
		wg.Add(1)
		go func(req *http.Request) {
			defer wg.Done()
			resp, err := rt.RoundTrip(req)
			if resp != nil {
				resp.Body.Close()
			}
			errCh <- err
		}(req)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			wg.Wait()
			t.Fatal("OnEvent calls were serialized; expected both logical requests to enter concurrently")
		}
	}

	close(release)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("RoundTrip error: %v", err)
		}
	}
	if got := attemptEvents.Load(); got != int32(len(requests)) {
		t.Fatalf("expected EventAttempt callbacks=%d, got %d", len(requests), got)
	}
}

func TestOnEvent_CooldownSkipRunsOutsideCooldownLock(t *testing.T) {
	u1 := "https://u1.test/rpc"
	fixedNow := time.Unix(1300, 0)

	var rt *Transport
	var skipCalls atomic.Int32
	rt = mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      &scriptRT{results: map[string][]rtResult{}},
		OnEvent: func(_ context.Context, event Event) {
			if event.Kind != EventCooldownSkip {
				return
			}
			skipCalls.Add(1)
			rt.cooldown.eligible(rt.now(), 0)
		},
	})
	rt.now = func() time.Time { return fixedNow }

	rt.cooldown.mu.Lock()
	rt.cooldown.coolingTo[0] = fixedNow.Add(time.Hour)
	rt.cooldown.mu.Unlock()

	req := newTestGETRequest(t, u1)
	done := make(chan error, 1)
	go func() {
		resp, err := rt.RoundTrip(req)
		if resp != nil {
			resp.Body.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ErrNoUsableEndpoint")
		}
		if !errors.Is(err, ErrNoUsableEndpoint) {
			t.Fatalf("expected ErrNoUsableEndpoint, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("OnEvent appears to run while the cooldown lock is held")
	}

	if got := skipCalls.Load(); got != 1 {
		t.Fatalf("expected one EventCooldownSkip callback, got %d", got)
	}
}
