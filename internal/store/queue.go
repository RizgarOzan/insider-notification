package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"insider-notification/internal/domain"
)

// turnOrder says which priority each claimed slot belongs to: 6 high, 3 normal, 1 low in every 10.
var turnOrder = [10]domain.Priority{
	domain.High, domain.Normal, domain.High, domain.High, domain.Normal,
	domain.High, domain.Low, domain.High, domain.Normal, domain.High,
}

// Claim leases up to limit due notifications of one channel. Each priority gets a weighted share
// (60% high, 30% normal, 10% low); slots a priority does not fill go to the next one down the list,
// so low is never starved and nothing sits idle while work exists.
//
// The share is counted across calls, not inside one. A worker with one free slot calls Claim(1)
// over and over, and a share worked out per call would round low down to nothing every time.
//
// FOR UPDATE SKIP LOCKED lets several workers run the same query without waiting on each other
// or taking the same row. The lease (locked_until + lease_token) is written in the same statement.
func (s *Store) Claim(ctx context.Context, channel domain.Channel, limit int, lease time.Duration) ([]domain.Notification, error) {
	s.mu.Lock()
	start := s.turn[channel]
	s.turn[channel] = (start + limit) % len(turnOrder)
	s.mu.Unlock()
	shares := map[domain.Priority]int{}
	for i := 0; i < limit; i++ {
		shares[turnOrder[(start+i)%len(turnOrder)]]++
	}
	var out []domain.Notification
	for _, p := range domain.Priorities {
		n := min(shares[p], limit-len(out))
		got, err := s.claimPriority(ctx, channel, p, n, lease)
		if err != nil {
			return out, err
		}
		out = append(out, got...)
	}
	// Second pass: spare slots, highest priority first.
	for _, p := range domain.Priorities {
		spare := limit - len(out)
		if spare <= 0 {
			break
		}
		got, err := s.claimPriority(ctx, channel, p, spare, lease)
		if err != nil {
			return out, err
		}
		out = append(out, got...)
	}
	return out, nil
}

func (s *Store) claimPriority(ctx context.Context, channel domain.Channel, p domain.Priority, n int, lease time.Duration) ([]domain.Notification, error) {
	if n <= 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE notifications SET
			status = 'sending', attempts = attempts + 1, updated_at = now(),
			locked_until = now() + make_interval(secs => $4),
			lease_token = gen_random_uuid()
		WHERE id IN (
			SELECT id FROM notifications
			WHERE channel = $1 AND priority = $2 AND status IN ('queued', 'retrying') AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			LIMIT $3
			FOR UPDATE SKIP LOCKED)
		RETURNING `+cols+`, lease_token`, string(channel), string(p), n, lease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Notification
	for rows.Next() {
		var nt domain.Notification
		var id uuid.UUID
		var batch *uuid.UUID
		var token uuid.UUID
		if err := rows.Scan(&id, &batch, &nt.Channel, &nt.Recipient, &nt.Content, &nt.Priority, &nt.Status, &nt.Attempts,
			&nt.NextAttemptAt, &nt.ScheduledAt, &nt.ProviderMessageID, &nt.LastError, &nt.CorrelationID,
			&nt.CreatedAt, &nt.UpdatedAt, &nt.SentAt, &token); err != nil {
			return out, err
		}
		nt.ID = id.String()
		nt.LeaseToken = token.String()
		out = append(out, nt)
	}
	return out, rows.Err()
}

// Result is what a worker reports after one provider call.
type Result struct {
	Outcome    domain.Status
	ProviderID string
	Error      string
	StatusCode int
	RetryAfter time.Duration
	RefundTry  bool
	Duration   time.Duration
}

// Finish records the outcome. It only applies if the caller still owns the lease; a worker
// whose lease expired and was handed to someone else gets applied=false and must drop its result.
func (s *Store) Finish(ctx context.Context, n domain.Notification, r Result) (applied bool, err error) {
	if !domain.CanTransition(domain.StatusSending, r.Outcome) {
		return false, fmt.Errorf("illegal transition sending -> %s", r.Outcome)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE notifications SET
			status = $3,
			attempts = attempts - CASE WHEN $4 THEN 1 ELSE 0 END,
			next_attempt_at = CASE WHEN $3 = 'retrying' THEN now() + make_interval(secs => $5) ELSE next_attempt_at END,
			provider_message_id = COALESCE(NULLIF($6, ''), provider_message_id),
			last_error = NULLIF($7, ''),
			sent_at = CASE WHEN $3 = 'sent' THEN now() ELSE sent_at END,
			locked_until = NULL, lease_token = NULL, updated_at = now()
		WHERE id = $1 AND lease_token = $2 AND status = 'sending'`,
		n.ID, n.LeaseToken, string(r.Outcome), r.RefundTry, r.RetryAfter.Seconds(), r.ProviderID, r.Error)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	var code *int
	if r.StatusCode != 0 {
		code = &r.StatusCode
	}
	if _, err := tx.Exec(ctx, `INSERT INTO notification_attempts (notification_id, attempt, outcome, status_code, error, duration_ms)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6)`,
		n.ID, n.Attempts, string(r.Outcome), code, r.Error, r.Duration.Milliseconds()); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// Release hands a claimed row back untouched (shutdown before the send): the attempt counted at
// claim time is refunded and the row is due again right away. Only the lease holder can do it.
func (s *Store) Release(ctx context.Context, n domain.Notification) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE notifications SET status = 'retrying', attempts = attempts - 1, next_attempt_at = now(),
			locked_until = NULL, lease_token = NULL, updated_at = now()
		WHERE id = $1 AND lease_token = $2 AND status = 'sending'`, n.ID, n.LeaseToken)
	return err
}

// ReapExpired handles rows whose worker died: the lease ran out while still "sending".
// Rows with attempts left go back to retrying; the rest fail, so a message that crashes
// its worker every time cannot loop forever (attempts was already counted at claim time).
func (s *Store) ReapExpired(ctx context.Context, maxAttempts int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE notifications SET
			status = CASE WHEN attempts >= $1 THEN 'failed' ELSE 'retrying' END,
			next_attempt_at = now(),
			last_error = 'lease expired: worker did not report a result',
			locked_until = NULL, lease_token = NULL, updated_at = now()
		WHERE status = 'sending' AND locked_until < now()`, maxAttempts)
	return tag.RowsAffected(), err
}
