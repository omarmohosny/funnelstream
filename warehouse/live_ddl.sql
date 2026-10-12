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

CREATE TABLE IF NOT EXISTS dim_store (
    store_id    text PRIMARY KEY,
    store_name  text NOT NULL,
    city        text NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_store_district (
    city      text NOT NULL,
    district  text NOT NULL,
    store_id  text NOT NULL,
    PRIMARY KEY (city, district)
);

CREATE TABLE IF NOT EXISTS dim_product_current (     -- latest version of each product (from silver_products), so Grafana shows names
    product_id  text PRIMARY KEY,
    name        text,
    category    text,
    price       numeric(10,2)
);

CREATE TABLE IF NOT EXISTS live_stock_now (          -- one row per store and product, recomputed from Silver (simulated stock)
    store_id       text        NOT NULL,
    product_id     text        NOT NULL,
    on_hand        integer     NOT NULL,
    reserved_qty   integer     NOT NULL,
    available      integer     NOT NULL,             -- greatest(on_hand - reserved_qty, 0)
    reorder_point  integer     NOT NULL,
    updated_at     timestamptz NOT NULL,
    PRIMARY KEY (store_id, product_id)
);

CREATE TABLE IF NOT EXISTS live_stock_store_minute (  -- one row per store and minute: stock history for the live stock monitor (simulated stock)
    minute_start   timestamptz NOT NULL,
    store_id       text        NOT NULL,
    skus           integer     NOT NULL,             -- products tracked in the store
    out_of_stock   integer     NOT NULL,             -- available <= 0
    low_stock      integer     NOT NULL,             -- 0 < available < reorder_point
    units_on_hand  bigint      NOT NULL,
    PRIMARY KEY (minute_start, store_id)
);

CREATE TABLE IF NOT EXISTS live_oos_demand_minute (  -- ALL served demand per minute, store, product and campaign
    minute_start   timestamptz NOT NULL,
    store_id       text        NOT NULL,
    product_id     text        NOT NULL,
    campaign_id    text        NOT NULL,             -- from utm_campaign via the campaign list; 'direct' when none, 'unknown' when not listed
    utm_campaign   text        NOT NULL DEFAULT '',  -- attribute only; '' when the event has no campaign
    views          bigint      NOT NULL DEFAULT 0,
    add_to_cart    bigint      NOT NULL DEFAULT 0,
    landing_clicks bigint      NOT NULL DEFAULT 0,   -- product views that carry a utm_campaign
    oos_at_write   boolean     NOT NULL DEFAULT false, -- stock was zero when the sink wrote this minute (not replay-safe)
    PRIMARY KEY (minute_start, store_id, product_id, campaign_id)
);

CREATE TABLE IF NOT EXISTS live_express_orders (     -- one row per express order created in the last 3 hours (recomputed from Silver)
    order_id                 text        PRIMARY KEY,
    store_id                 text        NOT NULL,
    promised_minutes         integer     NOT NULL,
    created_at               timestamptz NOT NULL,
    delivered_at             timestamptz,            -- NULL until the order's first delivered status
    closed_without_delivery  boolean     NOT NULL DEFAULT false  -- latest status is cancelled or returned and it was never delivered
);

CREATE TABLE IF NOT EXISTS inventory_daily (          -- one row per store, product and Cairo day (simulated stock)
    report_date        date    NOT NULL,
    store_id           text    NOT NULL,
    product_id         text    NOT NULL,
    closing_on_hand    integer NOT NULL,              -- stock at the end of the day, never below zero
    units_sold         integer NOT NULL DEFAULT 0,
    units_received     integer NOT NULL DEFAULT 0,
    units_shrunk       integer NOT NULL DEFAULT 0,
    units_adjusted     integer NOT NULL DEFAULT 0,
    oversold_units     integer NOT NULL DEFAULT 0,    -- asked for beyond the shelf: a lower bound
    ended_out_of_stock boolean NOT NULL,
    PRIMARY KEY (report_date, store_id, product_id)
);

CREATE TABLE IF NOT EXISTS oos_demand_daily (         -- live_oos_demand_minute summed per Cairo day
    report_date        date   NOT NULL,
    store_id           text   NOT NULL,
    product_id         text   NOT NULL,
    campaign_id        text   NOT NULL,                -- 'direct' when the click had no campaign
    utm_campaign       text   NOT NULL DEFAULT '',     -- attribute only
    served_views       bigint NOT NULL DEFAULT 0,      -- all demand in served districts
    served_add_to_cart bigint NOT NULL DEFAULT 0,
    served_clicks      bigint NOT NULL DEFAULT 0,      -- all landing clicks in served districts (denominator of the share)
    oos_views          bigint NOT NULL DEFAULT 0,      -- the part of the minutes flagged oos_at_write (stock when written)
    oos_add_to_cart    bigint NOT NULL DEFAULT 0,
    oos_clicks         bigint NOT NULL DEFAULT 0,      -- numerator of the share
    PRIMARY KEY (report_date, store_id, product_id, campaign_id)
);

CREATE TABLE IF NOT EXISTS express_daily (            -- one row per store and Cairo day, express orders and the promise
    report_date          date    NOT NULL,
    store_id             text    NOT NULL,
    orders               integer NOT NULL DEFAULT 0,
    delivered            integer NOT NULL DEFAULT 0,
    on_time              integer NOT NULL DEFAULT 0,
    breached             integer NOT NULL DEFAULT 0,
    avg_delivery_minutes numeric(8,2),
    max_delivery_minutes numeric(8,2),
    PRIMARY KEY (report_date, store_id)
);

CREATE TABLE IF NOT EXISTS marketing_d1_campaign (    -- one row per campaign with a spend row, per ad day (D-1 mart)
    report_date      date    NOT NULL,
    campaign_id      text    NOT NULL,
    utm_campaign     text    NOT NULL DEFAULT '',
    spend            numeric(12,2),
    ad_clicks        bigint,
    orders_placed    bigint,
    revenue_placed   numeric(12,2),
    revenue_delivered numeric(12,2),
    cpa              numeric(12,2),
    roas_placed      numeric(12,2),
    roas_delivered   numeric(12,2),
    served_clicks    bigint  NOT NULL DEFAULT 0,       -- landing clicks in served districts (Cairo day)
    oos_clicks       bigint  NOT NULL DEFAULT 0,       -- the part flagged oos_at_write
    oos_click_share  numeric(8,4),
    exposed_spend    numeric(12,2),                    -- estimate: spend x oos_clicks / served_clicks
    provisional      boolean NOT NULL DEFAULT false,
    PRIMARY KEY (report_date, campaign_id)
);

CREATE TABLE IF NOT EXISTS marketing_d1_product (     -- every out-of-stock (store, product) pair of the Cairo day
    report_date      date   NOT NULL,
    store_id         text   NOT NULL,
    product_id       text   NOT NULL,
    oos_views        bigint NOT NULL DEFAULT 0,
    oos_add_to_carts bigint NOT NULL DEFAULT 0,
    revenue_at_risk  numeric(12,2),                    -- NULL when the product has no price
    PRIMARY KEY (report_date, store_id, product_id)
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
GRANT SELECT ON live_metrics_minute, pipeline_health_minute, pipeline_batch_log, pipeline_heartbeat,
    dim_store, dim_store_district, dim_product_current, live_stock_now, live_stock_store_minute, live_oos_demand_minute, live_express_orders,
    inventory_daily, oos_demand_daily, express_daily, marketing_d1_campaign, marketing_d1_product TO grafana_ro;
-- END grafana_ro
