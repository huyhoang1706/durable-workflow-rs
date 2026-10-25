-- W3C trace context of the `workflow.start` span, so every execution of a run
-- continues one trace.
ALTER TABLE {schema}.runs ADD COLUMN trace_context jsonb;
