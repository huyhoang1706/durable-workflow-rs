-- SQLite translation of the Postgres schema (plugins/durable-postgres):
-- ids are hyphenated UUID text, timestamps are Unix microseconds and JSON
-- (including string lists) is stored as text.

CREATE TABLE runs (
    id                  TEXT    PRIMARY KEY,
    namespace           TEXT    NOT NULL,
    workflow            TEXT    NOT NULL,
    idempotency_key     TEXT,
    status              TEXT    NOT NULL,
    input               TEXT    NOT NULL,
    output              TEXT,
    error               TEXT,
    -- Optimistic version: seq of the last event.
    last_seq            INTEGER NOT NULL,
    -- The runnable index: when the run next needs a worker.
    wake_at             INTEGER,
    awaiting            TEXT    NOT NULL DEFAULT '[]',
    concurrency_key     TEXT,
    throttle_key        TEXT,
    deadline_at         INTEGER,
    cancel_requested_at INTEGER,
    cancel_reason       TEXT,
    last_error          TEXT,
    -- W3C trace context of the `workflow.start` span.
    trace_context       TEXT,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL
) STRICT;

CREATE UNIQUE INDEX runs_idempotency
    ON runs (namespace, workflow, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX runs_wake
    ON runs (namespace, wake_at)
    WHERE wake_at IS NOT NULL AND status NOT IN ('succeeded', 'failed', 'cancelled');

CREATE INDEX runs_list ON runs (namespace, created_at DESC, id DESC);

CREATE TABLE run_events (
    run_id  TEXT    NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    seq     INTEGER NOT NULL,
    at      INTEGER NOT NULL,
    kind    TEXT    NOT NULL,
    PRIMARY KEY (run_id, seq)
) STRICT, WITHOUT ROWID;

-- The inbox: external inputs wait here until a commit consumes them.
CREATE TABLE signals (
    id            TEXT    PRIMARY KEY,
    run_id        TEXT    NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    name          TEXT    NOT NULL,
    -- Event signals address the specific wait that matched.
    target_key    TEXT,
    payload       TEXT    NOT NULL,
    created_at    INTEGER NOT NULL,
    consumed_seq  INTEGER
) STRICT;

CREATE INDEX signals_pending ON signals (run_id, created_at, id) WHERE consumed_seq IS NULL;

-- Hook tokens are scoped to a namespace, so namespaces cannot claim each
-- other's tokens.
CREATE TABLE hooks (
    namespace  TEXT NOT NULL,
    token      TEXT NOT NULL,
    run_id     TEXT NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    PRIMARY KEY (namespace, token)
) STRICT, WITHOUT ROWID;

CREATE INDEX hooks_run ON hooks (run_id);

-- Open signal / hook / event waits. Event filters are matched in Rust.
CREATE TABLE waits (
    run_id      TEXT    NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    key         TEXT    NOT NULL,
    kind        TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    filter      TEXT,
    timeout_at  INTEGER,
    PRIMARY KEY (run_id, key)
) STRICT, WITHOUT ROWID;

CREATE INDEX waits_event ON waits (name) WHERE kind = 'event';

CREATE TABLE workflow_definitions (
    namespace   TEXT NOT NULL,
    name        TEXT NOT NULL,
    definition  TEXT NOT NULL,
    PRIMARY KEY (namespace, name)
) STRICT, WITHOUT ROWID;
