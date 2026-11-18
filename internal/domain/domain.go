// Package domain holds the types and rules that do not depend on HTTP or Postgres.
package domain

import (
	"errors"
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

type Channel string

const (
	SMS   Channel = "sms"
	Email Channel = "email"
	Push  Channel = "push"
)

var Channels = []Channel{SMS, Email, Push}

func ParseChannel(s string) (Channel, error) {
	switch c := Channel(s); c {
	case SMS, Email, Push:
		return c, nil
	}
	return "", fmt.Errorf("unknown channel %q (want sms, email or push)", s)
}

type Priority string

const (
	High   Priority = "high"
	Normal Priority = "normal"
	Low    Priority = "low"
)

// Priorities in the order workers pull them.
var Priorities = []Priority{High, Normal, Low}

// ParsePriority treats an empty string as normal.
func ParsePriority(s string) (Priority, error) {
	switch p := Priority(s); p {
	case High, Normal, Low:
		return p, nil
	case "":
		return Normal, nil
	}
	return "", fmt.Errorf("unknown priority %q (want high, normal or low)", s)
}

type Status string

const (
	StatusQueued    Status = "queued"
	StatusSending   Status = "sending"
	StatusRetrying  Status = "retrying"
	StatusSent      Status = "sent"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

func ParseStatus(s string) (Status, error) {
	switch st := Status(s); st {
	case StatusQueued, StatusSending, StatusRetrying, StatusSent, StatusFailed, StatusCancelled:
		return st, nil
	}
	return "", fmt.Errorf("unknown status %q", s)
}

func (s Status) IsTerminal() bool {
	return s == StatusSent || s == StatusFailed || s == StatusCancelled
}

var transitions = map[Status][]Status{
	StatusQueued:   {StatusSending, StatusCancelled},
	StatusRetrying: {StatusSending, StatusCancelled},
	// An expired lease sends a sending row back to retrying, or to failed when it is out of attempts.
	StatusSending: {StatusSent, StatusRetrying, StatusFailed},
}

// CanTransition is the one place that says which status changes are legal.
func CanTransition(from, to Status) bool {
	for _, t := range transitions[from] {
		if t == to {
			return true
		}
	}
	return false
}

type Notification struct {
	ID                string     `json:"id"`
	BatchID           *string    `json:"batchId,omitempty"`
	Channel           Channel    `json:"channel"`
	Recipient         string     `json:"to"`
	Content           string     `json:"content"`
	Priority          Priority   `json:"priority"`
	Status            Status     `json:"status"`
	Attempts          int        `json:"attempts"`
	NextAttemptAt     time.Time  `json:"nextAttemptAt"`
	ScheduledAt       *time.Time `json:"scheduledAt,omitempty"`
	ProviderMessageID *string    `json:"providerMessageId,omitempty"`
	LastError         *string    `json:"lastError,omitempty"`
	CorrelationID     string     `json:"correlationId,omitempty"`
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
	SentAt            *time.Time `json:"sentAt,omitempty"`

	// LeaseToken is only filled for rows a worker just claimed.
	LeaseToken string `json:"-"`
}

// NewNotification is what a client asks for, before it has an ID or a status.
type NewNotification struct {
	Recipient   string     `json:"to"`
	Channel     string     `json:"channel"`
	Content     string     `json:"content"`
	Priority    string     `json:"priority"`
	ScheduledAt *time.Time `json:"scheduledAt"`
}

// Limits count characters, not bytes. The case PDF gives no numbers; SMS 160 is one GSM segment.
const (
	MaxSMSChars   = 160
	MaxPushChars  = 256
	MaxEmailChars = 10000
	MaxBatchSize  = 1000
)

var phoneRe = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)

// Validate returns one message per problem so a client can fix everything in one go.
func (n NewNotification) Validate() []string {
	var problems []string
	ch, chErr := ParseChannel(n.Channel)
	if chErr != nil {
		problems = append(problems, chErr.Error())
	}
	if _, err := ParsePriority(n.Priority); err != nil {
		problems = append(problems, err.Error())
	}
	if strings.TrimSpace(n.Recipient) == "" {
		problems = append(problems, "to is required")
	}
	if strings.TrimSpace(n.Content) == "" {
		problems = append(problems, "content is required")
	}
	if strings.ContainsRune(n.Recipient, 0) || strings.ContainsRune(n.Content, 0) {
		problems = append(problems, "to and content must not contain NUL characters")
	}
	if chErr != nil {
		return problems
	}
	switch ch {
	case SMS:
		if n.Recipient != "" && !phoneRe.MatchString(n.Recipient) {
			problems = append(problems, "sms recipient must be a phone number in E.164 format, like +905551234567")
		}
		if utf8.RuneCountInString(n.Content) > MaxSMSChars {
			problems = append(problems, fmt.Sprintf("sms content is limited to %d characters", MaxSMSChars))
		}
	case Email:
		if n.Recipient != "" {
			if a, err := mail.ParseAddress(n.Recipient); err != nil || a.Address != n.Recipient {
				problems = append(problems, "email recipient must be a plain address like name@example.com")
			}
		}
		if utf8.RuneCountInString(n.Content) > MaxEmailChars {
			problems = append(problems, fmt.Sprintf("email content is limited to %d characters", MaxEmailChars))
		}
	case Push:
		if utf8.RuneCountInString(n.Content) > MaxPushChars {
			problems = append(problems, fmt.Sprintf("push content is limited to %d characters", MaxPushChars))
		}
	}
	return problems
}

// Errors the store returns and the API maps to HTTP codes.
var (
	ErrNotFound            = errors.New("not found")
	ErrNotCancellable      = errors.New("notification can only be cancelled while queued or retrying")
	ErrIdempotencyMismatch = errors.New("idempotency key was already used with a different request")
)
