package domain

import (
	"errors"
	"time"
)

const (
	BackoffBase = 5 * time.Second
	BackoffCap  = 5 * time.Minute
)

// Backoff is the wait before the next try; attempt 1 is the wait after the first failure.
// Exponential, capped, "equal jitter": half is fixed and half is random, so the first retry
// never collapses to zero and a whole batch does not come back at the same instant.
// rnd must return a value in [0,1).
func Backoff(attempt int, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := BackoffBase
	for i := 1; i < attempt && d < BackoffCap; i++ {
		d *= 2
	}
	if d > BackoffCap {
		d = BackoffCap
	}
	half := d / 2
	return half + time.Duration(float64(half)*rnd())
}

type Kind int

const (
	KindTransient   Kind = iota // retry with backoff
	KindPermanent               // never retry
	KindRateLimited             // retry later, does not use up an attempt
)

// SendError is what a provider call returns when it did not succeed.
type SendError struct {
	Kind       Kind
	StatusCode int           // 0 when there was no HTTP response
	RetryAfter time.Duration // only meaningful for rate limiting
	Err        error
}

func (e *SendError) Error() string { return e.Err.Error() }
func (e *SendError) Unwrap() error { return e.Err }

// KindOf treats any error that is not a *SendError (network, timeout) as transient.
func KindOf(err error) Kind {
	var se *SendError
	if errors.As(err, &se) {
		return se.Kind
	}
	return KindTransient
}

// Outcome is the next state after a failed try.
type Outcome struct {
	Status      Status
	RefundTry   bool          // rate limited: give the attempt back
	RetryAfter  time.Duration // wait before the next try (retrying only)
	ChannelWait time.Duration // pause the whole channel (rate limited only)
}

// Decide turns a provider error into the next state. attempts already counts this try.
func Decide(err error, attempts, maxAttempts int, rnd func() float64) Outcome {
	switch KindOf(err) {
	case KindPermanent:
		return Outcome{Status: StatusFailed}
	case KindRateLimited:
		wait := time.Second
		var se *SendError
		if errors.As(err, &se) && se.RetryAfter > 0 {
			wait = se.RetryAfter
		}
		return Outcome{Status: StatusRetrying, RefundTry: true, RetryAfter: wait, ChannelWait: wait}
	}
	if attempts >= maxAttempts {
		return Outcome{Status: StatusFailed}
	}
	return Outcome{Status: StatusRetrying, RetryAfter: Backoff(attempts, rnd)}
}
