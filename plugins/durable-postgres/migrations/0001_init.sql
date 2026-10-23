CREATE TABLE {schema}.runs (
    id                  uuid        PRIMARY KEY,
    namespace           text        NOT NULL,
    workflow            text        NOT NULL,
    idempotency_key     text,
    status              text        NOT NULL,
    input               jsonb       NOT NULL,
    output              jsonb,
    error               text,
    -- Optimistic version: seq of the last event.
    last_seq            bigint      NOT NULL,
    -- The runnable index: when the run next needs a worker.
    wake_at             timestamptz,
    awaiting            text[]      NOT NULL DEFAULT '{}',
    concurrency_key     text,
    throttle_key        text,
    deadline_at         timestamptz,
    cancel_requested_at timestamptz,
    cancel_reason       text,
    last_error          text,
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL
);

CREATE UNIQUE INDEX runs_idempotency
    ON {schema}.runs (namespace, workflow, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX runs_wake
    ON {schema}.runs (namespace, wake_at)
    WHERE wake_at IS NOT NULL AND status NOT IN ('succeeded', 'failed', 'cancelled');

CREATE INDEX runs_list ON {schema}.runs (namespace, created_at DESC, id DESC);

CREATE TABLE {schema}.run_events (
    run_id  uuid        NOT NULL REFERENCES {schema}.runs (id) ON DELETE CASCADE,
    seq     bigint      NOT NULL,
    at      timestamptz NOT NULL,
    kind    jsonb       NOT NULL,
    PRIMARY KEY (run_id, seq)
);

-- The inbox: external inputs wait here until a commit consumes them.
CREATE TABLE {schema}.signals (
    id            uuid        PRIMARY KEY,
    run_id        uuid        NOT NULL REFERENCES {schema}.runs (id) ON DELETE CASCADE,
    name          text        NOT NULL,
    -- Event signals address the specific wait that matched.
    target_key    text,
    payload       jsonb       NOT NULL,
    created_at    timestamptz NOT NULL,
    consumed_seq  bigint
);

CREATE INDEX signals_pending ON {schema}.signals (run_id, created_at, id) WHERE consumed_seq IS NULL;

-- Hook tokens are scoped to a namespace, so namespaces cannot claim each
-- other's tokens.
CREATE TABLE {schema}.hooks (
    namespace  text NOT NULL,
    token      text NOT NULL,
    run_id     uuid NOT NULL REFERENCES {schema}.runs (id) ON DELETE CASCADE,
    PRIMARY KEY (namespace, token)
);

CREATE INDEX hooks_run ON {schema}.hooks (run_id);

-- Open signal / hook / event waits. Event matching is `data @> filter`.
CREATE TABLE {schema}.waits (
    run_id      uuid        NOT NULL REFERENCES {schema}.runs (id) ON DELETE CASCADE,
    key         text        NOT NULL,
    kind        text        NOT NULL,
    name        text        NOT NULL,
    filter      jsonb,
    timeout_at  timestamptz,
    PRIMARY KEY (run_id, key)
);

CREATE INDEX waits_event ON {schema}.waits (name) WHERE kind = 'event';

CREATE TABLE {schema}.workflow_definitions (
    namespace   text  NOT NULL,
    name        text  NOT NULL,
    definition  jsonb NOT NULL,
    PRIMARY KEY (namespace, name)
);
