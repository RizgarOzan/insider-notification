// Package itest runs the real API, store and workers against a real Postgres and a fake provider.
// Set TEST_DATABASE_URL (make test does). Without a database the tests are skipped, loudly.
package itest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"insider-notification/internal/api"
	"insider-notification/internal/domain"
	"insider-notification/internal/observ"
	"insider-notification/internal/store"
	"insider-notification/internal/worker"
	"insider-notification/migrations"
)

type env struct {
	pool    *pgxpool.Pool
	store   *store.Store
	srv     *httptest.Server
	metrics *observ.Metrics
	log     *slog.Logger
}

func setup(t *testing.T) *env {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = "postgres://postgres@127.0.0.1:55432/notif_test?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		if os.Getenv("REQUIRE_DB") != "" {
			t.Fatalf("database required but unreachable: %v", err)
		}
		t.Skipf("SKIPPED, no test database (%v). Run `make test`.", err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE notification_attempts, notifications, batches, idempotency_keys`); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := observ.NewMetrics()
	st := store.New(pool)
	m.RegisterQueueDepth(st.QueueDepth)
	srv := httptest.NewServer(api.New(st, log, m).Handler())
	t.Cleanup(srv.Close)
	return &env{pool: pool, store: st, srv: srv, metrics: m, log: log}
}

// call sends a JSON request and decodes the JSON reply into out (if not nil).
func (e *env) call(t *testing.T, method, path string, body any, headers map[string]string, out any) (int, http.Header) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, rd)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("%s %s: decode: %v", method, path, err)
		}
	}
	return resp.StatusCode, resp.Header
}

func sms(content string) domain.NewNotification {
	return domain.NewNotification{Recipient: "+905551234567", Channel: "sms", Content: content}
}

// fakeSender records what it was asked to send and answers from a script.
type fakeSender struct {
	mu     sync.Mutex
	sent   map[string]int // notification id -> times seen
	order  []string       // contents in the order they arrived
	times  []time.Time
	script func(n domain.Notification) error
}

func newFake() *fakeSender { return &fakeSender{sent: map[string]int{}} }

func (f *fakeSender) Send(ctx context.Context, n domain.Notification) (string, int, error) {
	f.mu.Lock()
	f.sent[n.ID]++
	f.order = append(f.order, n.Content)
	f.times = append(f.times, time.Now())
	defer f.mu.Unlock() // the script runs under the lock, so tests can keep plain counters in it
	if f.script != nil {
		if err := f.script(n); err != nil {
			return "", 0, err
		}
	}
	return "msg-" + n.ID, 202, nil
}

func (e *env) runWorkers(t *testing.T, cfg worker.Config, s *fakeSender) (stop func()) {
	t.Helper()
	if cfg.RatePerSec == 0 {
		cfg.RatePerSec = 1000
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	cfg.Lease, cfg.PollInterval, cfg.ReapInterval = orDur(cfg.Lease, 30*time.Second), 20*time.Millisecond, 100*time.Millisecond
	cfg.BatchSize, cfg.Parallel = 20, 20
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); worker.New(cfg, e.store, s, e.log, e.metrics).Run(ctx) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func orDur(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}

func (e *env) waitStatus(t *testing.T, id string, want domain.Status) domain.Notification {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		n, err := e.store.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if n.Status == want {
			return n
		}
		if time.Now().After(deadline) {
			t.Fatalf("notification %s is %s (attempts %d, error %v), want %s", id, n.Status, n.Attempts, n.LastError, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) countStatus(t *testing.T, status string) int {
	var c int
	if err := e.pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications WHERE status = $1`, status).Scan(&c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCreateGetAndValidation(t *testing.T) {
	e := setup(t)
	var n domain.Notification
	code, hdr := e.call(t, "POST", "/api/v1/notifications", sms("hello"), map[string]string{"X-Correlation-ID": "corr-1"}, &n)
	if code != 202 || n.Status != domain.StatusQueued || n.Priority != domain.Normal {
		t.Fatalf("create: %d %+v", code, n)
	}
	if hdr.Get("X-Correlation-ID") != "corr-1" || n.CorrelationID != "corr-1" {
		t.Fatalf("correlation id not carried: header %q, stored %q", hdr.Get("X-Correlation-ID"), n.CorrelationID)
	}
	var got domain.Notification
	if code, _ := e.call(t, "GET", "/api/v1/notifications/"+n.ID, nil, nil, &got); code != 200 || got.ID != n.ID {
		t.Fatalf("get: %d %+v", code, got)
	}
	if code, _ := e.call(t, "GET", "/api/v1/notifications/"+n.ID[:30], nil, nil, nil); code != 404 {
		t.Fatalf("malformed id should be 404, got %d", code)
	}

	var bad struct{ Details []string }
	if code, _ := e.call(t, "POST", "/api/v1/notifications", domain.NewNotification{Channel: "sms"}, nil, &bad); code != 422 || len(bad.Details) != 2 {
		t.Fatalf("validation: %d %v", code, bad)
	}
	if code, _ := e.call(t, "POST", "/api/v1/notifications", map[string]any{"to": "x", "bogus": 1}, nil, nil); code != 400 {
		t.Fatalf("unknown field should be 400, got %d", code)
	}
}

func TestIdempotency(t *testing.T) {
	e := setup(t)
	h := map[string]string{"Idempotency-Key": "k-1"}
	var a, b domain.Notification
	_, h1 := e.call(t, "POST", "/api/v1/notifications", sms("same"), h, &a)
	code, h2 := e.call(t, "POST", "/api/v1/notifications", sms("same"), h, &b)
	if code != 202 || a.ID != b.ID || h1.Get("Idempotent-Replayed") != "" || h2.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d first=%s second=%s headers %q %q", code, a.ID, b.ID, h1.Get("Idempotent-Replayed"), h2.Get("Idempotent-Replayed"))
	}
	if code, _ := e.call(t, "POST", "/api/v1/notifications", sms("different"), h, nil); code != 422 {
		t.Fatalf("same key, other body: got %d, want 422", code)
	}

	// 20 requests with one key at the same moment must create exactly one row.
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n domain.Notification
			e.call(t, "POST", "/api/v1/notifications", sms("race"), map[string]string{"Idempotency-Key": "k-race"}, &n)
			ids <- n.ID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	var rows int
	e.pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications WHERE content = 'race'`).Scan(&rows)
	if len(seen) != 1 || rows != 1 {
		t.Fatalf("race: %d distinct ids, %d rows, want 1 and 1", len(seen), rows)
	}

	// Same key on a batch replays the batch.
	items := map[string]any{"notifications": []domain.NewNotification{sms("b1"), sms("b2")}}
	var r1, r2 store.BatchResult
	e.call(t, "POST", "/api/v1/notifications/batch", items, map[string]string{"Idempotency-Key": "k-batch"}, &r1)
	e.call(t, "POST", "/api/v1/notifications/batch", items, map[string]string{"Idempotency-Key": "k-batch"}, &r2)
	if r1.BatchID != r2.BatchID || r1.Count != 2 {
		t.Fatalf("batch replay: %+v vs %+v", r1, r2)
	}
}

func TestBatchLimits(t *testing.T) {
	e := setup(t)
	mk := func(n int) map[string]any {
		items := make([]domain.NewNotification, n)
		for i := range items {
			items[i] = sms(fmt.Sprintf("m%d", i))
		}
		return map[string]any{"notifications": items}
	}
	var res store.BatchResult
	if code, _ := e.call(t, "POST", "/api/v1/notifications/batch", mk(1000), nil, &res); code != 202 || res.Count != 1000 || len(res.Notifications) != 1000 {
		t.Fatalf("1000 items: %d %d", code, res.Count)
	}
	var st store.BatchStatus
	e.call(t, "GET", "/api/v1/batches/"+res.BatchID, nil, nil, &st)
	if st.Count != 1000 || st.Statuses["queued"] != 1000 {
		t.Fatalf("batch status: %+v", st)
	}
	if code, _ := e.call(t, "POST", "/api/v1/notifications/batch", mk(1001), nil, nil); code != 422 {
		t.Fatalf("1001 items: got %d, want 422", code)
	}
	if code, _ := e.call(t, "POST", "/api/v1/notifications/batch", mk(0), nil, nil); code != 422 {
		t.Fatalf("empty batch: got %d, want 422", code)
	}

	// One bad item rejects the whole batch and stores nothing.
	before := e.countStatus(t, "queued")
	bad := map[string]any{"notifications": []domain.NewNotification{sms("ok"), {Channel: "sms", Content: "x", Recipient: "nope"}}}
	var errBody struct{ Details []string }
	if code, _ := e.call(t, "POST", "/api/v1/notifications/batch", bad, nil, &errBody); code != 422 || len(errBody.Details) != 1 {
		t.Fatalf("bad item: %d %v", code, errBody)
	}
	if after := e.countStatus(t, "queued"); after != before {
		t.Fatalf("rejected batch left %d rows behind", after-before)
	}
}

func TestListFiltersAndPaging(t *testing.T) {
	e := setup(t)
	var ids []string
	for i := 0; i < 7; i++ {
		in := sms(fmt.Sprintf("n%d", i))
		if i%2 == 1 {
			in = domain.NewNotification{Recipient: "a@b.co", Channel: "email", Content: "mail"}
		}
		var n domain.Notification
		e.call(t, "POST", "/api/v1/notifications", in, nil, &n)
		ids = append(ids, n.ID)
		time.Sleep(2 * time.Millisecond)
	}
	e.call(t, "POST", "/api/v1/notifications/"+ids[0]+"/cancel", nil, nil, nil)

	type page struct {
		Items      []domain.Notification
		NextCursor string
	}
	var all []string
	cursor := ""
	for pages := 0; pages < 10; pages++ {
		var p page
		if code, _ := e.call(t, "GET", "/api/v1/notifications?limit=3&cursor="+cursor, nil, nil, &p); code != 200 {
			t.Fatalf("page %d: %d", pages, code)
		}
		for _, n := range p.Items {
			all = append(all, n.ID)
		}
		if cursor = p.NextCursor; cursor == "" {
			break
		}
	}
	if len(all) != 7 {
		t.Fatalf("paging returned %d rows, want 7", len(all))
	}
	for i, id := range all { // newest first
		if id != ids[6-i] {
			t.Fatalf("position %d: got %s, want %s", i, id, ids[6-i])
		}
	}

	var p page
	e.call(t, "GET", "/api/v1/notifications?channel=email", nil, nil, &p)
	if len(p.Items) != 3 {
		t.Fatalf("channel filter: %d", len(p.Items))
	}
	e.call(t, "GET", "/api/v1/notifications?status=cancelled", nil, nil, &p)
	if len(p.Items) != 1 || p.Items[0].ID != ids[0] {
		t.Fatalf("status filter: %+v", p.Items)
	}
	from := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	to := time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339)
	e.call(t, "GET", "/api/v1/notifications?from="+from+"&to="+to, nil, nil, &p)
	if len(p.Items) != 0 {
		t.Fatalf("date range in the past should be empty, got %d", len(p.Items))
	}
	if code, _ := e.call(t, "GET", "/api/v1/notifications?status=bogus", nil, nil, nil); code != 400 {
		t.Fatalf("bad status filter: %d", code)
	}
	if code, _ := e.call(t, "GET", "/api/v1/notifications?cursor=not-a-cursor!", nil, nil, nil); code != 400 {
		t.Fatalf("bad cursor: %d", code)
	}
}

func TestCancel(t *testing.T) {
	e := setup(t)
	var n domain.Notification
	e.call(t, "POST", "/api/v1/notifications", sms("cancel me"), nil, &n)
	if code, _ := e.call(t, "POST", "/api/v1/notifications/"+n.ID+"/cancel", nil, nil, &n); code != 200 || n.Status != domain.StatusCancelled {
		t.Fatalf("cancel: %d %s", code, n.Status)
	}
	if code, _ := e.call(t, "POST", "/api/v1/notifications/"+n.ID+"/cancel", nil, nil, nil); code != 409 {
		t.Fatalf("cancel twice: got %d, want 409", code)
	}
	if code, _ := e.call(t, "POST", "/api/v1/notifications/00000000-0000-0000-0000-000000000000/cancel", nil, nil, nil); code != 404 {
		t.Fatalf("cancel unknown: got %d, want 404", code)
	}

	// A cancelled notification is never sent, even with workers running.
	f := newFake()
	e.runWorkers(t, worker.Config{}, f)
	var live domain.Notification
	e.call(t, "POST", "/api/v1/notifications", sms("send me"), nil, &live)
	e.waitStatus(t, live.ID, domain.StatusSent)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sent[n.ID] != 0 {
		t.Fatal("cancelled notification was sent")
	}
	if code, _ := e.call(t, "POST", "/api/v1/notifications/"+live.ID+"/cancel", nil, nil, nil); code != 409 {
		t.Fatalf("cancel after sent: got %d, want 409", code)
	}
}

// Two worker pools compete for the same rows. FOR UPDATE SKIP LOCKED must hand each row to one of them.
func TestTwoWorkersNeverSendTwice(t *testing.T) {
	e := setup(t)
	items := make([]domain.NewNotification, 300)
	for i := range items {
		items[i] = sms(fmt.Sprintf("m%d", i))
	}
	var res store.BatchResult
	e.call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": items}, nil, &res)

	f := newFake()
	e.runWorkers(t, worker.Config{}, f)
	e.runWorkers(t, worker.Config{}, f)
	for _, id := range res.Notifications {
		e.waitStatus(t, id, domain.StatusSent)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sent) != 300 {
		t.Fatalf("%d distinct messages sent, want 300", len(f.sent))
	}
	for id, c := range f.sent {
		if c != 1 {
			t.Fatalf("notification %s was sent %d times", id, c)
		}
	}
}

func TestRateLimitPerChannel(t *testing.T) {
	e := setup(t)
	items := make([]domain.NewNotification, 150)
	for i := range items {
		items[i] = sms(fmt.Sprintf("m%d", i))
	}
	var res store.BatchResult
	e.call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": items}, nil, &res)

	f := newFake()
	start := time.Now()
	e.runWorkers(t, worker.Config{RatePerSec: 100}, f)
	for _, id := range res.Notifications {
		e.waitStatus(t, id, domain.StatusSent)
	}
	took := time.Since(start)
	// 100 per second means 150 messages need at least ~1.4s.
	if took < 1300*time.Millisecond {
		t.Fatalf("150 messages took %v; the 100/sec limit was not applied", took)
	}
	// And in no one-second window more than 100 (+ small slack for timer jitter).
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.times {
		inWindow := 0
		for j := i; j < len(f.times) && f.times[j].Sub(f.times[i]) < time.Second; j++ {
			inWindow++
		}
		if inWindow > 105 {
			t.Fatalf("%d sends within one second", inWindow)
		}
	}
}

// Channels have separate limits: a flooded SMS channel does not slow email down.
func TestChannelsAreIndependent(t *testing.T) {
	e := setup(t)
	items := make([]domain.NewNotification, 100)
	for i := range items {
		items[i] = sms(fmt.Sprintf("sms%d", i))
	}
	e.call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": items}, nil, nil)
	var mail domain.Notification
	e.call(t, "POST", "/api/v1/notifications", domain.NewNotification{Recipient: "a@b.co", Channel: "email", Content: "mail"}, nil, &mail)

	f := newFake()
	e.runWorkers(t, worker.Config{RatePerSec: 2}, f) // sms needs ~50s at this rate
	got := e.waitStatus(t, mail.ID, domain.StatusSent)
	if got.Attempts != 1 {
		t.Fatalf("attempts %d", got.Attempts)
	}
	if e.countStatus(t, "sent") > 10 {
		t.Fatal("sms was not slowed down by its limit")
	}
}

func TestPriorityOrder(t *testing.T) {
	e := setup(t)
	var items []domain.NewNotification
	for i := 0; i < 30; i++ {
		items = append(items, domain.NewNotification{Recipient: "+905551234567", Channel: "sms", Content: "low", Priority: "low"})
	}
	for i := 0; i < 30; i++ {
		items = append(items, domain.NewNotification{Recipient: "+905551234567", Channel: "sms", Content: "high", Priority: "high"})
	}
	var res store.BatchResult
	e.call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": items}, nil, &res)

	claimed, err := e.store.Claim(context.Background(), domain.SMS, 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[domain.Priority]int{}
	for _, n := range claimed {
		counts[n.Priority]++
	}
	// 20 slots: high 12, normal 6 (none exist, so it flows on), low 2, spare slots go to the next priority down.
	if len(claimed) != 20 || counts[domain.High] != 18 || counts[domain.Low] != 2 {
		t.Fatalf("claim mix: %v", counts)
	}
}

// With sends leaving one at a time the loop claims one row at a time. Low must still get its
// turn while high has a backlog, not wait until high is empty.
func TestLowNotStarvedBehindHighBacklog(t *testing.T) {
	e := setup(t)
	var items []domain.NewNotification
	for i := 0; i < 300; i++ {
		items = append(items, domain.NewNotification{Recipient: "+905551234567", Channel: "sms", Content: "high", Priority: "high"})
	}
	for i := 0; i < 30; i++ {
		items = append(items, domain.NewNotification{Recipient: "+905551234567", Channel: "sms", Content: "low", Priority: "low"})
	}
	var res store.BatchResult
	e.call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": items}, nil, &res)

	f := newFake()
	e.runWorkers(t, worker.Config{RatePerSec: 100}, f)
	deadline := time.Now().Add(20 * time.Second)
	for e.countStatus(t, "sent") < 330 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.order) != 330 {
		t.Fatalf("sent %d of 330", len(f.order))
	}
	// One slot in ten is low's, so about 20 of the first 200 sends. Before the fix it was 2.
	low := 0
	for _, c := range f.order[:200] {
		if c == "low" {
			low++
		}
	}
	if low < 10 {
		t.Fatalf("only %d low messages in the first 200 sends", low)
	}
}

func TestRetryClassification(t *testing.T) {
	e := setup(t)
	f := newFake()
	calls := map[string]int{}
	f.script = func(n domain.Notification) error {
		calls[n.Content]++
		switch n.Content {
		case "flaky":
			if calls[n.Content] < 3 {
				return &domain.SendError{Kind: domain.KindTransient, StatusCode: 503, Err: fmt.Errorf("503")}
			}
		case "bad":
			return &domain.SendError{Kind: domain.KindPermanent, StatusCode: 400, Err: fmt.Errorf("400")}
		case "down":
			return &domain.SendError{Kind: domain.KindTransient, StatusCode: 503, Err: fmt.Errorf("503")}
		}
		return nil
	}
	e.runWorkers(t, worker.Config{MaxAttempts: 3}, f)

	create := func(c string) string {
		var n domain.Notification
		e.call(t, "POST", "/api/v1/notifications", sms(c), nil, &n)
		return n.ID
	}
	flaky, bad, down := create("flaky"), create("bad"), create("down")

	// Backoff is seconds long; the test pulls next_attempt_at forward instead of waiting.
	skipWait := func() {
		e.pool.Exec(context.Background(), `UPDATE notifications SET next_attempt_at = now() WHERE status = 'retrying'`)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		skipWait()
		if e.countStatus(t, "sent")+e.countStatus(t, "failed") == 3 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if n := e.waitStatus(t, flaky, domain.StatusSent); n.Attempts != 3 {
		t.Errorf("flaky: attempts %d, want 3", n.Attempts)
	}
	if n := e.waitStatus(t, bad, domain.StatusFailed); n.Attempts != 1 || n.LastError == nil {
		t.Errorf("permanent error must not be retried: attempts %d", n.Attempts)
	}
	if n := e.waitStatus(t, down, domain.StatusFailed); n.Attempts != 3 {
		t.Errorf("down: attempts %d, want the maximum 3", n.Attempts)
	}
	var rows int
	e.pool.QueryRow(context.Background(), `SELECT count(*) FROM notification_attempts WHERE notification_id = $1`, flaky).Scan(&rows)
	if rows != 3 {
		t.Errorf("attempt history rows: %d, want 3", rows)
	}
}

func TestRateLimitedResponseKeepsAttempts(t *testing.T) {
	e := setup(t)
	f := newFake()
	var calls int
	f.script = func(n domain.Notification) error {
		calls++
		if calls <= 4 { // more 429s than MaxAttempts: they must not count
			return &domain.SendError{Kind: domain.KindRateLimited, StatusCode: 429, RetryAfter: time.Second, Err: fmt.Errorf("429")}
		}
		return nil
	}
	e.runWorkers(t, worker.Config{MaxAttempts: 2}, f)
	var n domain.Notification
	e.call(t, "POST", "/api/v1/notifications", sms("limited"), nil, &n)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && e.countStatus(t, "sent") == 0 {
		e.pool.Exec(context.Background(), `UPDATE notifications SET next_attempt_at = now() WHERE status = 'retrying'`)
		time.Sleep(30 * time.Millisecond)
	}
	got := e.waitStatus(t, n.ID, domain.StatusSent)
	if got.Attempts != 1 {
		t.Fatalf("attempts after four 429s: %d, want 1", got.Attempts)
	}
}

func TestLeaseRecoveryAndStaleResult(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	var n domain.Notification
	e.call(t, "POST", "/api/v1/notifications", sms("orphan"), nil, &n)

	// A worker claims the row and "dies".
	first, err := e.store.Claim(ctx, domain.SMS, 5, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("claim: %v %v", first, err)
	}
	// Before the lease expires nothing is reaped and nobody else can claim it.
	if reaped, _ := e.store.ReapExpired(ctx, 3); reaped != 0 {
		t.Fatal("reaped a live lease")
	}
	if again, _ := e.store.Claim(ctx, domain.SMS, 5, time.Minute); len(again) != 0 {
		t.Fatal("row claimed twice before its lease expired")
	}

	e.pool.Exec(ctx, `UPDATE notifications SET locked_until = now() - interval '1 second'`)
	if reaped, _ := e.store.ReapExpired(ctx, 3); reaped != 1 {
		t.Fatalf("reaped %d, want 1", reaped)
	}
	second, _ := e.store.Claim(ctx, domain.SMS, 5, time.Minute)
	if len(second) != 1 || second[0].Attempts != 2 {
		t.Fatalf("reclaim: %+v", second)
	}

	// The first worker wakes up late. Its result must be thrown away.
	applied, err := e.store.Finish(ctx, first[0], store.Result{Outcome: domain.StatusSent})
	if err != nil || applied {
		t.Fatalf("stale result applied=%v err=%v", applied, err)
	}
	if applied, err := e.store.Finish(ctx, second[0], store.Result{Outcome: domain.StatusSent}); err != nil || !applied {
		t.Fatalf("current owner result applied=%v err=%v", applied, err)
	}

	// A message that keeps killing its worker ends up failed instead of looping forever.
	var poison domain.Notification
	e.call(t, "POST", "/api/v1/notifications", sms("poison"), nil, &poison)
	for i := 0; i < 3; i++ {
		e.store.Claim(ctx, domain.SMS, 5, time.Minute)
		e.pool.Exec(ctx, `UPDATE notifications SET locked_until = now() - interval '1 second' WHERE status = 'sending'`)
		e.store.ReapExpired(ctx, 3)
	}
	if got, _ := e.store.Get(ctx, poison.ID); got.Status != domain.StatusFailed || got.Attempts != 3 {
		t.Fatalf("poison message: %s after %d attempts", got.Status, got.Attempts)
	}
}

func TestScheduledNotificationWaits(t *testing.T) {
	e := setup(t)
	f := newFake()
	e.runWorkers(t, worker.Config{}, f)
	later := time.Now().Add(1500 * time.Millisecond)
	in := sms("later")
	in.ScheduledAt = &later
	var n domain.Notification
	e.call(t, "POST", "/api/v1/notifications", in, nil, &n)
	time.Sleep(400 * time.Millisecond)
	if got, _ := e.store.Get(context.Background(), n.ID); got.Status != domain.StatusQueued {
		t.Fatalf("sent before its time: %s", got.Status)
	}
	e.waitStatus(t, n.ID, domain.StatusSent)
}

func TestHealthAndMetrics(t *testing.T) {
	e := setup(t)
	f := newFake()
	e.runWorkers(t, worker.Config{}, f)
	var n domain.Notification
	e.call(t, "POST", "/api/v1/notifications", sms("m"), nil, &n)
	e.waitStatus(t, n.ID, domain.StatusSent)
	later := time.Now().Add(time.Hour)
	waiting := sms("waiting")
	waiting.ScheduledAt = &later
	e.call(t, "POST", "/api/v1/notifications", waiting, nil, nil)

	for _, p := range []string{"/healthz", "/readyz"} {
		if code, _ := e.call(t, "GET", p, nil, nil, nil); code != 200 {
			t.Fatalf("%s: %d", p, code)
		}
	}
	resp, err := http.Get(e.srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{
		`notifications_processed_total{channel="sms",result="sent"} 1`,
		`provider_request_duration_seconds_count{channel="sms"} 1`,
		`http_requests_total{code="202",method="POST",route="POST /api/v1/notifications"}`,
		`notification_queue_depth`,
	} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("metrics output lacks %q", want)
		}
	}
}
