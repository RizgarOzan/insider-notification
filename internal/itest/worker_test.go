package itest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"insider-notification/internal/domain"
	"insider-notification/internal/worker"
)

// funcSender lets a test control each provider call, including how long it takes.
type funcSender func(ctx context.Context, n domain.Notification) error

func (f funcSender) Send(ctx context.Context, n domain.Notification) (string, int, error) {
	if err := f(ctx, n); err != nil {
		return "", 0, err
	}
	return "msg-" + n.ID, 202, nil
}

func (e *env) runWorkersWith(t *testing.T, cfg worker.Config, s funcSender) (stop func()) {
	t.Helper()
	if cfg.RatePerSec == 0 {
		cfg.RatePerSec = 1000
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	cfg.Lease, cfg.PollInterval, cfg.ReapInterval = 30*time.Second, 20*time.Millisecond, 100*time.Millisecond
	cfg.BatchSize, cfg.Parallel = 20, 20
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); worker.New(cfg, e.store, s, e.log, e.metrics).Run(ctx) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func (e *env) create(t *testing.T, content string) string {
	t.Helper()
	var n domain.Notification
	if code, _ := e.call(t, "POST", "/api/v1/notifications", sms(content), nil, &n); code != 202 {
		t.Fatalf("create %q: status %d", content, code)
	}
	return n.ID
}

func TestSlowSendDoesNotStallTheChannel(t *testing.T) {
	e := setup(t)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	e.runWorkersWith(t, worker.Config{}, func(ctx context.Context, n domain.Notification) error {
		if n.Content == "slow" {
			<-release
		}
		return nil
	})
	t.Cleanup(unblock) // registered after the workers' stop, so it runs first and a failed test cannot hang on the blocked call
	slow := e.create(t, "slow")
	time.Sleep(100 * time.Millisecond) // let the slow one be claimed first
	for i := 0; i < 45; i++ {          // more than one claim batch of 20
		e.create(t, fmt.Sprintf("fast-%d", i))
	}

	deadline := time.Now().Add(5 * time.Second)
	for e.countStatus(t, "sent") < 45 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d fast messages sent while one call hangs", e.countStatus(t, "sent"))
		}
		time.Sleep(20 * time.Millisecond)
	}
	unblock()
	e.waitStatus(t, slow, domain.StatusSent)
}

func TestRateLimitedResponsePausesTheWholeChannel(t *testing.T) {
	e := setup(t)
	for i := 0; i < 10; i++ { // all due before the workers start, so they are claimed as one batch
		e.create(t, fmt.Sprintf("m-%d", i))
	}
	var mu sync.Mutex
	var calls []time.Time
	e.runWorkersWith(t, worker.Config{}, func(ctx context.Context, n domain.Notification) error {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, time.Now())
		if len(calls) == 1 {
			return &domain.SendError{Kind: domain.KindRateLimited, StatusCode: 429, RetryAfter: time.Second, Err: fmt.Errorf("429")}
		}
		return nil
	})
	deadline := time.Now().Add(8 * time.Second)
	for e.countStatus(t, "sent") < 10 {
		if time.Now().After(deadline) {
			t.Fatalf("%d of 10 sent", e.countStatus(t, "sent"))
		}
		time.Sleep(30 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if gap := calls[1].Sub(calls[0]); gap < 900*time.Millisecond {
		t.Fatalf("next provider call came %v after the 429, want at least the 1s Retry-After", gap)
	}
	if len(calls) != 11 {
		t.Fatalf("%d provider calls for 10 messages and one 429, want 11", len(calls))
	}
}

func TestShutdownFinishesInFlightAndReleasesTheRest(t *testing.T) {
	e := setup(t)
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, e.create(t, fmt.Sprintf("s-%d", i)))
	}
	started := make(chan struct{}, 10)
	unblock := make(chan struct{})
	stop := e.runWorkersWith(t, worker.Config{RatePerSec: 1}, func(ctx context.Context, n domain.Notification) error {
		started <- struct{}{}
		<-unblock
		return nil
	})
	<-started // one is at the provider; the other four wait for the 1/sec limiter

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Run returned while a send was still in flight")
	case <-time.After(300 * time.Millisecond):
	}
	close(unblock)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the in-flight send finished")
	}

	ctx := context.Background()
	var sent, retrying int
	for _, id := range ids {
		n, err := e.store.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		switch n.Status {
		case domain.StatusSent:
			sent++
		case domain.StatusRetrying:
			retrying++
			if n.Attempts != 0 {
				t.Errorf("released row %s kept %d attempts, want the attempt refunded", id, n.Attempts)
			}
		default:
			t.Errorf("row %s is %s after shutdown", id, n.Status)
		}
	}
	if sent != 1 || retrying != 4 {
		t.Fatalf("sent %d retrying %d, want 1 and 4", sent, retrying)
	}
	var leased int
	e.pool.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE lease_token IS NOT NULL`).Scan(&leased)
	if leased != 0 {
		t.Fatalf("%d rows still hold a lease after shutdown", leased)
	}
}

func TestCancelledNotificationIsNeverSent(t *testing.T) {
	e := setup(t)
	later := time.Now().Add(time.Second)
	in := sms("cancel me")
	in.ScheduledAt = &later
	var n domain.Notification
	e.call(t, "POST", "/api/v1/notifications", in, nil, &n)
	if code, _ := e.call(t, "POST", "/api/v1/notifications/"+n.ID+"/cancel", nil, nil, nil); code != 200 {
		t.Fatalf("cancel: %d", code)
	}
	f := newFake()
	e.runWorkers(t, worker.Config{}, f)
	time.Sleep(1500 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sent) != 0 {
		t.Fatalf("a cancelled notification reached the provider: %v", f.sent)
	}
}
