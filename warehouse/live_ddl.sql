-- Postgres live and health tables (spec 17.3, 17.17 to 17.21; contract: .superpowers/sdd/.../g-shared-contract.md).
-- Local serving tables written by the Silver job and the heartbeat helper, read by Grafana. Idempotent.
-- Every measure is a FULL recalculated total (INSERT ... ON CONFLICT DO UPDATE SET measure = EXCLUDED.measure).
-- Apply with: python scripts/apply_live_ddl.py   (the read-only role's password comes from GRAFANA_DB_PASSWORD, never from this file)

CREATE TABLE IF NOT EXISTS live_metrics_minute (
    minute_start    timestamptz NOT NULL,
    country         text        NOT NULL,
    events          bigint      NOT NULL DEFAULT 0,
    active_sessions bigint      NOT NULL DEFAULT 0,
    product_views   bigint      NOT NULL DEFAULT 0,
    add_to_cart     bigint      NOT NULL DEFAULT 0,
    orders_placed   bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (minute_start, country)
);

CREATE TABLE IF NOT EXISTS pipeline_health_minute (
    minute_start    timestamptz PRIMARY KEY,
    events          bigint NOT NULL DEFAULT 0,   -- Silver events whose Kafka arrival falls in the minute
    late_events     bigint NOT NULL DEFAULT 0,   -- of those, is_late = true
    quarantined     bigint NOT NULL DEFAULT 0,   -- silver_quarantine rows received in the minute
    silver_batches  bigint NOT NULL DEFAULT 0    -- Silver micro-batches that finished in the minute
);

CREATE TABLE IF NOT EXISTS pipeline_batch_log (   -- one row per finished Silver batch; silver_batches is counted from it
    job_run_id   text        NOT NULL,            -- uuid4 per process start
    batch_id     bigint      NOT NULL,
    finished_at  timestamptz NOT NULL,
    PRIMARY KEY (job_run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    component    text PRIMARY KEY,                -- silver_job, gold_build, publish
    last_seen_at timestamptz NOT NULL,
    details      text
);

-- BEGIN grafana_ro (removed by the apply script when GRAFANA_DB_PASSWORD is not set)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro LOGIN PASSWORD '__GRAFANA_DB_PASSWORD__';
    ELSE
        ALTER ROLE grafana_ro LOGIN PASSWORD '__GRAFANA_DB_PASSWORD__';
    END IF;
END
$$;

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM grafana_ro;
GRANT SELECT ON live_metrics_minute, pipeline_health_minute, pipeline_batch_log, pipeline_heartbeat TO grafana_ro;
-- END grafana_ro
