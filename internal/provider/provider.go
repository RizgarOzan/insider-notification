// Package provider talks to the external delivery service (webhook.site in the case).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"insider-notification/internal/domain"
)

// Sender is what the worker needs. Tests swap in a fake.
type Sender interface {
	Send(ctx context.Context, n domain.Notification) (messageID string, status int, err error)
}

type HTTP struct {
	url    string
	client *http.Client
}

func NewHTTP(url string, timeout time.Duration) *HTTP {
	return &HTTP{url: url, client: &http.Client{Timeout: timeout}}
}

type request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type response struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
}

// Send posts one message. The notification ID travels as Idempotency-Key, so a provider that
// supports it can drop the duplicate when we retry after a lost response.
func (h *HTTP) Send(ctx context.Context, n domain.Notification) (string, int, error) {
	body, _ := json.Marshal(request{To: n.Recipient, Channel: string(n.Channel), Content: n.Content})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return "", 0, &domain.SendError{Kind: domain.KindPermanent, Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", n.ID)
	if n.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", n.CorrelationID)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return "", 0, &domain.SendError{Kind: domain.KindTransient, Err: fmt.Errorf("provider request: %w", err)}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		var r response
		_ = json.Unmarshal(raw, &r) // the reply body is informational; a 2xx is a success either way
		return r.MessageID, resp.StatusCode, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return "", resp.StatusCode, &domain.SendError{Kind: domain.KindRateLimited, StatusCode: resp.StatusCode,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")), Err: fmt.Errorf("provider returned 429")}
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500:
		return "", resp.StatusCode, &domain.SendError{Kind: domain.KindTransient, StatusCode: resp.StatusCode,
			Err: fmt.Errorf("provider returned %d", resp.StatusCode)}
	default:
		return "", resp.StatusCode, &domain.SendError{Kind: domain.KindPermanent, StatusCode: resp.StatusCode,
			Err: fmt.Errorf("provider rejected the message with %d", resp.StatusCode)}
	}
}

func parseRetryAfter(v string) time.Duration {
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		if secs > 300 {
			secs = 300
		}
		return time.Duration(secs) * time.Second
	}
	return 0
}
