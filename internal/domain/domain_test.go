package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		in   NewNotification
		bad  int // number of problems expected
	}{
		{"valid sms", NewNotification{Recipient: "+905551234567", Channel: "sms", Content: "hi"}, 0},
		{"valid email", NewNotification{Recipient: "a@b.co", Channel: "email", Content: "hi", Priority: "low"}, 0},
		{"valid push", NewNotification{Recipient: "device-token", Channel: "push", Content: "hi"}, 0},
		{"unknown channel", NewNotification{Recipient: "x", Channel: "fax", Content: "hi"}, 1},
		{"missing everything", NewNotification{}, 3},
		{"sms not e164", NewNotification{Recipient: "0555", Channel: "sms", Content: "hi"}, 1},
		{"sms too long", NewNotification{Recipient: "+905551234567", Channel: "sms", Content: strings.Repeat("a", 161)}, 1},
		{"sms exactly 160 runes", NewNotification{Recipient: "+905551234567", Channel: "sms", Content: strings.Repeat("ç", 160)}, 0},
		{"push too long", NewNotification{Recipient: "t", Channel: "push", Content: strings.Repeat("a", 257)}, 1},
		{"email with display name", NewNotification{Recipient: "Bob <a@b.co>", Channel: "email", Content: "hi"}, 1},
		{"bad priority", NewNotification{Recipient: "t", Channel: "push", Content: "hi", Priority: "urgent"}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.Validate(); len(got) != tc.bad {
				t.Fatalf("got %d problems %v, want %d", len(got), got, tc.bad)
			}
		})
	}
}

func TestParseChannelAndPriority(t *testing.T) {
	if _, err := ParseChannel("sms"); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseChannel("SMS"); err == nil {
		t.Fatal("channel parsing is case sensitive")
	}
	if p, err := ParsePriority(""); err != nil || p != Normal {
		t.Fatalf("empty priority should be normal, got %q %v", p, err)
	}
}

// Every pair of statuses is checked, so adding a status or a transition forces a decision here.
func TestCanTransition(t *testing.T) {
	all := []Status{StatusQueued, StatusSending, StatusRetrying, StatusSent, StatusFailed, StatusCancelled}
	legal := map[[2]Status]bool{
		{StatusQueued, StatusSending}:     true,
		{StatusQueued, StatusCancelled}:   true,
		{StatusRetrying, StatusSending}:   true,
		{StatusRetrying, StatusCancelled}: true,
		{StatusSending, StatusSent}:       true,
		{StatusSending, StatusRetrying}:   true,
		{StatusSending, StatusFailed}:     true,
	}
	for _, from := range all {
		for _, to := range all {
			if got := CanTransition(from, to); got != legal[[2]Status{from, to}] {
				t.Errorf("%s -> %s: got %v", from, to, got)
			}
		}
	}
	for _, s := range []Status{StatusSent, StatusFailed, StatusCancelled} {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
}

func TestBackoff(t *testing.T) {
	zero := func() float64 { return 0 }
	almostOne := func() float64 { return 0.999999 }
	tests := []struct {
		attempt  int
		min, max time.Duration
	}{
		{1, 2500 * time.Millisecond, 5 * time.Second},
		{2, 5 * time.Second, 10 * time.Second},
		{3, 10 * time.Second, 20 * time.Second},
		{7, 150 * time.Second, 300 * time.Second}, // 5s*2^6 = 320s, capped at 300s
		{50, 150 * time.Second, 300 * time.Second},
		{0, 2500 * time.Millisecond, 5 * time.Second},
	}
	for _, tc := range tests {
		if got := Backoff(tc.attempt, zero); got != tc.min {
			t.Errorf("attempt %d, no jitter: got %v, want %v", tc.attempt, got, tc.min)
		}
		if got := Backoff(tc.attempt, almostOne); got < tc.min || got > tc.max {
			t.Errorf("attempt %d, full jitter: got %v, want within [%v, %v]", tc.attempt, got, tc.min, tc.max)
		}
	}
}

func TestDecide(t *testing.T) {
	rnd := func() float64 { return 0 }
	transient := &SendError{Kind: KindTransient, Err: errors.New("503")}
	permanent := &SendError{Kind: KindPermanent, Err: errors.New("400")}
	limited := &SendError{Kind: KindRateLimited, RetryAfter: 7 * time.Second, Err: errors.New("429")}

	if o := Decide(transient, 1, 8, rnd); o.Status != StatusRetrying || o.RetryAfter == 0 || o.RefundTry {
		t.Errorf("transient first failure: %+v", o)
	}
	if o := Decide(transient, 8, 8, rnd); o.Status != StatusFailed {
		t.Errorf("transient out of attempts: %+v", o)
	}
	if o := Decide(permanent, 1, 8, rnd); o.Status != StatusFailed {
		t.Errorf("permanent: %+v", o)
	}
	if o := Decide(limited, 8, 8, rnd); o.Status != StatusRetrying || !o.RefundTry || o.ChannelWait != 7*time.Second {
		t.Errorf("429 must not use up the last attempt and must pause the channel: %+v", o)
	}
	if o := Decide(errors.New("connection reset"), 2, 8, rnd); o.Status != StatusRetrying {
		t.Errorf("plain network error should be transient: %+v", o)
	}
}
