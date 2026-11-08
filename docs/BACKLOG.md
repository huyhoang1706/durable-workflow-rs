# Backlog

Deferred work, grouped by area. Each entry says where it came from: [iter1](plans/iter1.md) or [iter2](plans/iter2.md). Iteration 3 (language SDKs) is still in progress; its open items are in [its plan](plans/iter3-sansio-core.md).

## Observability

- **DuckDB observability store** (`plugins/durable-duckdb`), from iter2.
  - A local, zero-infra sink for spans, logs and metrics, written to a `.duckdb` file.
  - Plugged in through `OtelConfig` programmatic exporters (`.span_exporter`, `.log_exporter`, `.metric_exporter`), so the runtime needs no changes.
  - Add `durable traces|logs` CLI queries, for example all spans of a run and the logs of a step attempt.
- **ClickHouse for production**, from iter2.
  - Path: OTLP → OpenTelemetry Collector → ClickHouse exporter.
  - Decide the table schema (traces, logs, metrics), TTL and retention, partitioning by day and namespace, and the query patterns for run/step drill-down.
  - Ship a reference Collector config and docker-compose profile.
- **Local metrics sink**, from iter2. Metrics are off in local-first mode. Options: a Prometheus pull endpoint (`/metrics` on the standalone HTTP server) or periodic JSON snapshots to file.
- **Span links from signals and events**, from iter2. Capture the sender's trace context in `deliver_signal`, `publish_event` and `resolve_hook`, and link it from the execution that consumes it.
- **GenAI semantic conventions**, from iter2. Add `gen_ai.*` attributes, and possibly time-to-first-chunk, in an optional LLM-step helper once the conventions leave Development status.

## Streaming

- **Durable stream storage**, from iter2. Postgres-backed chunks for use cases that need replayable streams, not transient LLM tokens.
- **Workflow-body stream writes**, from iter2. Exactly-once writes from the body, recorded through the event log as DBOS does.
- **Server-side "latest attempt only" filter**, from iter2. Hide chunks of failed attempts from readers, instead of relying on `AttemptFailed` markers.

## Workflow features

These come from iter1.

- Child workflows.
- Cron / scheduled starts.
- Debounce, batching and priority (flow control).
- Versioning / patching APIs for changing in-flight workflow code.
- First-party integration clients (email, Slack, Teams) under `plugins/`.

## Storage and operations

These come from iter1.

- Blob offload for large payloads (a `BlobStore` plugin, for example S3).
- Retention cleanup for finished runs and their events.
- Dashboard UI.
