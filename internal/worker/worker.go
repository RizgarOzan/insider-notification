// Package worker pulls due notifications from Postgres and sends them, one loop per channel.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"insider-notification/internal/domain"
	"insider-notification/internal/observ"
	"insider-notification/internal/provider"
	"insider-notification/internal/store"
)

type Config struct {
	RatePerSec   int // per channel
	MaxAttempts  int
	Lease        time.Duration
	PollInterval time.Duration
	ReapInterval time.Duration
	BatchSize    int // rows claimed per round trip
	Parallel     int // sends in flight per channel
}

type Pool struct {
	cfg     Config
	store   *store.Store
	sender  provider.Sender
	log     *slog.Logger
	metrics *observ.Metrics
	rnd     func() float64
}

func New(cfg Config, st *store.Store, sender provider.Sender, log *slog.Logger, m *observ.Metrics) *Pool {
	return &Pool{cfg: cfg, store: st, sender: sender, log: log, metrics: m, rnd: rand.Float64}
}

// Run starts one loop per channel plus the lease reaper and blocks until ctx is cancelled
// and every loop has returned. A send that is in flight when ctx is cancelled finishes
// (its result is written with a fresh context) before Run returns.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, ch := range domain.Channels {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.channelLoop(ctx, ch)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.reapLoop(ctx)
	}()
	wg.Wait()
}

func (p *Pool) reapLoop(ctx context.Context) {
	t := time.NewTicker(p.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := p.store.ReapExpired(ctx, p.cfg.MaxAttempts)
			if err != nil && ctx.Err() == nil {
				p.log.Error("reap expired leases", "error", err)
			} else if n > 0 {
				p.log.Warn("recovered notifications from expired leases", "count", n)
			}
		}
	}
}

func (p *Pool) channelLoop(ctx context.Context, ch domain.Channel) {
	// Burst of 1 spaces sends 10ms apart at 100/sec. A bigger burst would let a closed
	// one-second window hold burst + rate sends, which breaks "at most 100 per second".
	limiter := rate.NewLimiter(rate.Limit(p.cfg.RatePerSec), 1)
	var pausedUntil atomic.Int64 // unix nanos; set when the provider answers 429
	log := p.log.With("channel", ch)

	// One slot per send in flight. Claiming only as many rows as there are free slots, and not
	// waiting for the whole batch, keeps a hanging provider call from holding up the rest.
	slots := make(chan struct{}, p.cfg.Parallel)
	var inflight sync.WaitGroup
	defer inflight.Wait()

	for ctx.Err() == nil {
		if wait := time.Until(time.Unix(0, pausedUntil.Load())); wait > 0 {
			sleep(ctx, wait)
			continue
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		want := 1
		for want < p.cfg.BatchSize {
			select {
			case slots <- struct{}{}:
				want++
				continue
			default:
			}
			break
		}

		batch, err := p.store.Claim(ctx, ch, want, p.cfg.Lease)
		if err != nil && ctx.Err() == nil {
			log.Error("claim failed", "error", err)
		}
		// Rows claimed before an error already carry a lease and a counted attempt: deliver them.
		for i := len(batch); i < want; i++ {
			<-slots
		}
		if len(batch) == 0 {
			sleep(ctx, p.cfg.PollInterval)
			continue
		}

		for _, n := range batch {
			inflight.Add(1)
			go func() {
				defer inflight.Done()
				defer func() { <-slots }()
				p.deliver(ctx, limiter, &pausedUntil, n)
			}()
		}
	}
}

func (p *Pool) deliver(ctx context.Context, limiter *rate.Limiter, pausedUntil *atomic.Int64, n domain.Notification) {
	log := p.log.With("channel", n.Channel, "notification_id", n.ID, "correlation_id", n.CorrelationID, "attempt", n.Attempts)

	// The limit applies to provider calls, not to claiming. If we are shutting down, give the row back with
	// its attempt refunded; nothing was sent. The same goes for a 429 that arrived while this row
	// waited for its turn: Retry-After can be far longer than the lease, so it is not held through the pause.
	err := limiter.Wait(ctx)
	if err == nil && time.Now().UnixNano() < pausedUntil.Load() {
		err = errChannelPaused
	}
	if err != nil {
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := p.store.Release(relCtx, n); err != nil {
			log.Warn("could not release row; the reaper will", "error", err)
		}
		return
	}

	// Sending is not tied to ctx: a request aborted by shutdown may already have reached the provider,
	// and counting that as a failed attempt would risk a duplicate. The client has its own timeout.
	sendCtx := observ.WithCorrelationID(context.WithoutCancel(ctx), n.CorrelationID)
	start := time.Now()
	msgID, code, sendErr := p.sender.Send(sendCtx, n)
	took := time.Since(start)
	p.metrics.ProviderTime.WithLabelValues(string(n.Channel)).Observe(took.Seconds())

	res := store.Result{StatusCode: code, ProviderID: msgID, Duration: took}
	label := "sent"
	if sendErr == nil {
		res.Outcome = domain.StatusSent
	} else {
		o := domain.Decide(sendErr, n.Attempts, p.cfg.MaxAttempts, p.rnd)
		res.Outcome, res.RefundTry, res.RetryAfter, res.Error = o.Status, o.RefundTry, o.RetryAfter, sendErr.Error()
		if o.ChannelWait > 0 {
			pausedUntil.Store(time.Now().Add(o.ChannelWait).UnixNano())
		}
		// A 429 costs no attempt, so on its own it would keep a message alive forever, and a
		// message the provider always answers with 429 would pause its channel every time.
		if o.RefundTry && time.Since(dueSince(n)) > rateLimitGiveUp {
			res.Outcome, res.RefundTry, res.RetryAfter = domain.StatusFailed, false, 0
			res.Error = "rate limited for over " + rateLimitGiveUp.String() + ": " + res.Error
		}
		label = "retry"
		if res.Outcome == domain.StatusFailed {
			label = "failed"
		}
	}

	// Write the result even when ctx is cancelled: the message already went out. A database
	// hiccup gets a few retries, because losing the write means the lease expires and the
	// message is sent again.
	var applied bool
	for try := 1; try <= 4; try++ {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		applied, err = p.store.Finish(writeCtx, n, res)
		cancel()
		if err == nil {
			break
		}
		log.Warn("recording result failed, retrying", "error", err, "try", try)
		if try < 4 {
			time.Sleep(time.Duration(try) * 500 * time.Millisecond)
		}
	}
	// Counted here, after the write, so one provider call is one result: a dropped write is
	// "dropped" and not also "sent".
	switch {
	case err != nil:
		log.Error("could not record result", "error", err, "outcome", res.Outcome)
	case !applied:
		p.metrics.Delivered.WithLabelValues(string(n.Channel), "dropped").Inc()
		log.Warn("result dropped: lease was lost", "outcome", res.Outcome)
	default:
		p.metrics.Delivered.WithLabelValues(string(n.Channel), label).Inc()
		log.Info("delivery attempt", "outcome", res.Outcome, "status_code", code, "duration_ms", took.Milliseconds())
	}
}

var errChannelPaused = errors.New("channel paused by a 429")

// rateLimitGiveUp is how long a message may keep getting 429s before it is failed.
const rateLimitGiveUp = time.Hour

// dueSince is when the message was first meant to go out.
func dueSince(n domain.Notification) time.Time {
	if n.ScheduledAt != nil && n.ScheduledAt.After(n.CreatedAt) {
		return *n.ScheduledAt
	}
	return n.CreatedAt
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
