// Package store is all the SQL. Postgres is both the source of truth and the queue.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"insider-notification/internal/domain"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const cols = `id, batch_id, channel, recipient, content, priority, status, attempts,
	next_attempt_at, scheduled_at, provider_message_id, last_error, correlation_id,
	created_at, updated_at, sent_at`

func scan(row pgx.Row) (domain.Notification, error) {
	var n domain.Notification
	var batch *uuid.UUID
	var id uuid.UUID
	err := row.Scan(&id, &batch, &n.Channel, &n.Recipient, &n.Content, &n.Priority, &n.Status, &n.Attempts,
		&n.NextAttemptAt, &n.ScheduledAt, &n.ProviderMessageID, &n.LastError, &n.CorrelationID,
		&n.CreatedAt, &n.UpdatedAt, &n.SentAt)
	n.ID = id.String()
	if batch != nil {
		b := batch.String()
		n.BatchID = &b
	}
	return n, err
}

// HashRequest is what idempotency compares: the request after parsing, not the raw bytes.
func HashRequest(kind string, v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(append([]byte(kind+":"), b...))
	return hex.EncodeToString(sum[:])
}

// claimIdempotencyKey inserts the key inside tx. If another request holds the key, the insert
// waits for it to commit (unique index), so by the time we read, the stored response is final.
// It returns the stored response when this is a replay, nil when the caller should do the work.
func claimIdempotencyKey(ctx context.Context, tx pgx.Tx, key, hash string) (json.RawMessage, error) {
	if key == "" {
		return nil, nil
	}
	tag, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2) ON CONFLICT DO NOTHING`, key, hash)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	var storedHash string
	var resp json.RawMessage
	if err := tx.QueryRow(ctx, `SELECT request_hash, response FROM idempotency_keys WHERE key = $1`, key).Scan(&storedHash, &resp); err != nil {
		return nil, err
	}
	if storedHash != hash {
		return nil, domain.ErrIdempotencyMismatch
	}
	return resp, nil
}

func saveIdempotencyResponse(ctx context.Context, tx pgx.Tx, key string, v any) error {
	if key == "" {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE idempotency_keys SET response = $2 WHERE key = $1`, key, b)
	return err
}

const insertSQL = `INSERT INTO notifications
	(id, batch_id, channel, recipient, content, priority, next_attempt_at, scheduled_at, correlation_id)
	VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), $7, $8)
	RETURNING ` + cols

func insert(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, batchID *uuid.UUID, n domain.NewNotification, corr string) (domain.Notification, error) {
	pri, _ := domain.ParsePriority(n.Priority)
	return scan(q.QueryRow(ctx, insertSQL, uuid.New(), batchID, n.Channel, n.Recipient, n.Content, string(pri), n.ScheduledAt, corr))
}

// Create inserts one notification. With a key it is idempotent: the same key and body returns
// the first response (replayed=true), a different body returns ErrIdempotencyMismatch.
func (s *Store) Create(ctx context.Context, n domain.NewNotification, key, corr string) (res domain.Notification, replayed bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, false, err
	}
	defer tx.Rollback(ctx)

	stored, err := claimIdempotencyKey(ctx, tx, key, HashRequest("single", n))
	if err != nil {
		return res, false, err
	}
	if stored != nil {
		return res, true, json.Unmarshal(stored, &res)
	}
	if res, err = insert(ctx, tx, nil, n, corr); err != nil {
		return res, false, err
	}
	if err := saveIdempotencyResponse(ctx, tx, key, res); err != nil {
		return res, false, err
	}
	return res, false, tx.Commit(ctx)
}

type BatchResult struct {
	BatchID       string   `json:"batchId"`
	Count         int      `json:"count"`
	Notifications []string `json:"notificationIds"`
}

// CreateBatch inserts all notifications and the batch row in one transaction: all or nothing.
func (s *Store) CreateBatch(ctx context.Context, items []domain.NewNotification, key, corr string) (res BatchResult, replayed bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, false, err
	}
	defer tx.Rollback(ctx)

	stored, err := claimIdempotencyKey(ctx, tx, key, HashRequest("batch", items))
	if err != nil {
		return res, false, err
	}
	if stored != nil {
		return res, true, json.Unmarshal(stored, &res)
	}

	batchID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO batches (id, count) VALUES ($1, $2)`, batchID, len(items)); err != nil {
		return res, false, err
	}
	res = BatchResult{BatchID: batchID.String(), Count: len(items), Notifications: make([]string, 0, len(items))}

	// One round trip for the whole batch instead of 1000.
	b := &pgx.Batch{}
	for _, n := range items {
		pri, _ := domain.ParsePriority(n.Priority)
		id := uuid.New()
		res.Notifications = append(res.Notifications, id.String())
		b.Queue(`INSERT INTO notifications (id, batch_id, channel, recipient, content, priority, next_attempt_at, scheduled_at, correlation_id)
			VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), $7, $8)`,
			id, batchID, n.Channel, n.Recipient, n.Content, string(pri), n.ScheduledAt, corr)
	}
	br := tx.SendBatch(ctx, b)
	for range items {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return res, false, err
		}
	}
	if err := br.Close(); err != nil {
		return res, false, err
	}
	if err := saveIdempotencyResponse(ctx, tx, key, res); err != nil {
		return res, false, err
	}
	return res, false, tx.Commit(ctx)
}

func (s *Store) Get(ctx context.Context, id string) (domain.Notification, error) {
	uid, err := uuid.Parse(id)
	if err != nil {
		return domain.Notification{}, domain.ErrNotFound
	}
	n, err := scan(s.pool.QueryRow(ctx, `SELECT `+cols+` FROM notifications WHERE id = $1`, uid))
	if errors.Is(err, pgx.ErrNoRows) {
		return n, domain.ErrNotFound
	}
	return n, err
}

type BatchStatus struct {
	BatchID   string         `json:"batchId"`
	Count     int            `json:"count"`
	CreatedAt time.Time      `json:"createdAt"`
	Statuses  map[string]int `json:"statuses"`
}

func (s *Store) GetBatch(ctx context.Context, id string) (BatchStatus, error) {
	uid, err := uuid.Parse(id)
	if err != nil {
		return BatchStatus{}, domain.ErrNotFound
	}
	bs := BatchStatus{BatchID: uid.String(), Statuses: map[string]int{}}
	err = s.pool.QueryRow(ctx, `SELECT count, created_at FROM batches WHERE id = $1`, uid).Scan(&bs.Count, &bs.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return bs, domain.ErrNotFound
	}
	if err != nil {
		return bs, err
	}
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, uid)
	if err != nil {
		return bs, err
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var c int
		if err := rows.Scan(&st, &c); err != nil {
			return bs, err
		}
		bs.Statuses[st] = c
	}
	return bs, rows.Err()
}

// Cancel is one conditional UPDATE. If a worker claimed the row a moment earlier, the WHERE
// no longer matches and the caller gets ErrNotCancellable: the database decides the race.
func (s *Store) Cancel(ctx context.Context, id string) (domain.Notification, error) {
	uid, err := uuid.Parse(id)
	if err != nil {
		return domain.Notification{}, domain.ErrNotFound
	}
	n, err := scan(s.pool.QueryRow(ctx, `UPDATE notifications SET status = 'cancelled', updated_at = now()
		WHERE id = $1 AND status IN ('queued', 'retrying') RETURNING `+cols, uid))
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return n, err
	}
	if _, gerr := s.Get(ctx, id); gerr != nil {
		return n, gerr
	}
	return n, domain.ErrNotCancellable
}

type ListFilter struct {
	Status  string
	Channel string
	BatchID string
	From    *time.Time
	To      *time.Time
	Limit   int
	Cursor  string
}

type ListResult struct {
	Items      []domain.Notification `json:"items"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

var ErrBadCursor = errors.New("invalid cursor")

// List is newest first, paged by a (created_at, id) cursor. A cursor does not skip or repeat rows
// when new ones arrive, which OFFSET would.
func (s *Store) List(ctx context.Context, f ListFilter) (ListResult, error) {
	var where []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != "" {
		add("status = $%d", f.Status)
	}
	if f.Channel != "" {
		add("channel = $%d", f.Channel)
	}
	if f.BatchID != "" {
		bid, err := uuid.Parse(f.BatchID)
		if err != nil {
			return ListResult{}, domain.ErrNotFound
		}
		add("batch_id = $%d", bid)
	}
	if f.From != nil {
		add("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("created_at < $%d", *f.To)
	}
	if f.Cursor != "" {
		ts, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return ListResult{}, ErrBadCursor
		}
		args = append(args, ts, id)
		where = append(where, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	q := `SELECT ` + cols + ` FROM notifications`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit+1)
	q += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return ListResult{}, err
	}
	defer rows.Close()
	res := ListResult{Items: []domain.Notification{}}
	for rows.Next() {
		n, err := scan(rows)
		if err != nil {
			return res, err
		}
		res.Items = append(res.Items, n)
	}
	if err := rows.Err(); err != nil {
		return res, err
	}
	if len(res.Items) > f.Limit {
		res.Items = res.Items[:f.Limit]
		last := res.Items[f.Limit-1]
		res.NextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return res, nil
}

func encodeCursor(ts time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(ts.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func decodeCursor(c string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}
	tsPart, idPart, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	ts, err := time.Parse(time.RFC3339Nano, tsPart)
	if err != nil {
		return ts, uuid.Nil, err
	}
	id, err := uuid.Parse(idPart)
	return ts, id, err
}

// QueueDepth counts rows per channel and status for the metrics endpoint.
func (s *Store) QueueDepth(ctx context.Context) (map[[2]string]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT channel, status, count(*) FROM notifications
		WHERE status IN ('queued', 'retrying', 'sending') GROUP BY channel, status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[[2]string]int{}
	for rows.Next() {
		var ch, st string
		var c int
		if err := rows.Scan(&ch, &st, &c); err != nil {
			return nil, err
		}
		out[[2]string{ch, st}] = c
	}
	return out, rows.Err()
}
