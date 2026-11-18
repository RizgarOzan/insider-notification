CREATE TABLE batches (
    id         uuid PRIMARY KEY,
    count      integer     NOT NULL CHECK (count > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id                  uuid PRIMARY KEY,
    batch_id            uuid REFERENCES batches (id),
    channel             text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    recipient           text        NOT NULL,
    content             text        NOT NULL,
    priority            text        NOT NULL DEFAULT 'normal' CHECK (priority IN ('high', 'normal', 'low')),
    status              text        NOT NULL DEFAULT 'queued'
                        CHECK (status IN ('queued', 'sending', 'retrying', 'sent', 'failed', 'cancelled')),
    attempts            integer     NOT NULL DEFAULT 0,
    next_attempt_at     timestamptz NOT NULL DEFAULT now(),
    locked_until        timestamptz,
    lease_token         uuid,
    scheduled_at        timestamptz,
    provider_message_id text,
    last_error          text,
    correlation_id      text        NOT NULL DEFAULT '',
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    sent_at             timestamptz
);

-- The claim query: per channel and priority, only rows that can be picked up.
CREATE INDEX notifications_claim_idx
    ON notifications (channel, priority, next_attempt_at)
    WHERE status IN ('queued', 'retrying');

-- The lease reaper only looks at rows that are being sent.
CREATE INDEX notifications_sending_idx
    ON notifications (locked_until)
    WHERE status = 'sending';

-- Listing is newest first, paged by (created_at, id).
CREATE INDEX notifications_list_idx ON notifications (created_at DESC, id DESC);
CREATE INDEX notifications_batch_idx ON notifications (batch_id) WHERE batch_id IS NOT NULL;

CREATE TABLE notification_attempts (
    id              bigserial PRIMARY KEY,
    notification_id uuid        NOT NULL REFERENCES notifications (id),
    attempt         integer     NOT NULL,
    outcome         text        NOT NULL,
    status_code     integer,
    error           text,
    duration_ms     integer     NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX notification_attempts_notification_idx ON notification_attempts (notification_id);

CREATE TABLE idempotency_keys (
    key          text PRIMARY KEY,
    request_hash text        NOT NULL,
    response     jsonb,
    created_at   timestamptz NOT NULL DEFAULT now()
);
