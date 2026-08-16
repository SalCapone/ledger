-- Core event log. This table is append-only: the application layer never
-- issues UPDATE or DELETE against it. Corrections happen by appending a new
-- compensating event, which keeps the table a true audit trail.
CREATE TABLE IF NOT EXISTS events (
    global_seq      BIGSERIAL PRIMARY KEY,
    id              UUID NOT NULL DEFAULT gen_random_uuid(),
    aggregate_id    TEXT NOT NULL,
    aggregate_type  TEXT NOT NULL,
    version         INT NOT NULL,
    type            TEXT NOT NULL,
    data            JSONB NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- This unique constraint IS the optimistic-concurrency mechanism: two
    -- concurrent writers both trying to write version N for the same
    -- aggregate will have one succeed and one fail with a unique violation,
    -- which the Go layer translates into ErrConcurrencyConflict.
    CONSTRAINT events_aggregate_version_uniq UNIQUE (aggregate_id, version)
);

CREATE INDEX IF NOT EXISTS events_aggregate_id_idx ON events (aggregate_id, version);
CREATE INDEX IF NOT EXISTS events_aggregate_type_idx ON events (aggregate_type);

-- Transactional outbox: written in the SAME db transaction as the events
-- that produced it, so "event was persisted" and "event will eventually
-- reach Kafka" can never disagree. See kafka/outbox.go for the publisher
-- that tails this table and marks rows published.
CREATE TABLE IF NOT EXISTS event_outbox (
    global_seq   BIGINT PRIMARY KEY REFERENCES events(global_seq),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS event_outbox_unpublished_idx
    ON event_outbox (global_seq) WHERE published_at IS NULL;
