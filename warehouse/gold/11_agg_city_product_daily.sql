-- agg_city_product_daily: city x product x Cairo day, recomputed, never incremented (spec 17.1). Same semantics
-- as streaming/gold/city_product.py. The table stays at CITY grain (no district).
--
-- Event measures (silver_events): sessions_viewed = DISTINCT sessions with at least one product_view event for the
--   product in that city on that event_date; add_to_cart = DISTINCT sessions with at least one add_to_cart event
--   for the product (product_id is a Silver column for every event type, the simulator's add_to_cart carries it).
--   Distinct sessions, not event counts. City = the event's own city; a session that changed city during a day is
--   counted in each city it has events in. Day = the Silver event_date (a Cairo date). Same exclusions as
--   agg_funnel_daily: rows without session_id are excluded, late rows are INCLUDED (spec 16.1), bot sessions are
--   recomputed (see below). Events without product_id are ignored.
-- Order measures (fact_order_lines, all placed lines of any order status, the same basis as revenue_placed of
--   agg_orders_daily, so cancelled orders are included): units = SUM(quantity), revenue = SUM(line_total) in EGP,
--   by the line's city, product_id and date_key (the Cairo day the order was placed). Lines without product_id are
--   ignored.
-- NULL or empty city becomes 'unknown'. The row set is the union of the event keys and the order-line keys (full
--   outer join); the missing side is 0 (0.00 for revenue), never NULL: in a count table a zero is a real fact.
--
-- Bots (spec 16.5, same rule as streaming/gold/bots.py and 09_agg_funnel_daily.sql): is_bot is recomputed over the
--   WHOLE silver_events rows of every session that appears in the recomputed dates, not taken from the stored
--   is_bot column: a session is a bot when some event has more than 100 events of the same session in the 10
--   seconds up to it (CTE bots). Nothing is persisted.
--
-- (WHEN clause order is fixed by the grammar: MATCHED, NOT MATCHED, NOT MATCHED BY SOURCE.)
-- Window handling, ONE atomic MERGE: the source holds the recomputed rows (events AND order lines) for these dates:
--   the window; the dates of every event of a session that has an event in the window (a session that crossed
--   midnight can become a bot); the dates of late events (event_date before from_date, kafka_timestamp Cairo date
--   inside the window); and the dates of the order lines of every order the fact_order_lines step recomputed
--   (a late status row for an old order, order date or status row in the window). Matched keys get all measures
--   updated, new keys are inserted, and WHEN NOT MATCHED BY SOURCE deletes a target row whose date_key is inside the
--   window and that has neither events nor order lines any more.
-- A failure rolls the statement back, the window is never left empty. Limit: a key that vanishes on a widened date
--   OUTSIDE the window is not deleted (a MERGE condition cannot hold a subquery); its measures are updated only
--   while the key still has data. In history mode (a replay, kafka_timestamp is the replay time) every replayed date
--   counts as late and is recomputed.
MERGE INTO {gold}.agg_city_product_daily AS t
USING (
  WITH win_sessions AS (
    SELECT DISTINCT e.session_id
    FROM {silver}.silver_events e
    WHERE e.session_id IS NOT NULL AND e.event_date BETWEEN DATE'{from_date}' AND DATE'{to_date}'
  ),
  win_orders AS (
    SELECT o.order_id
    FROM {silver}.silver_orders o
    WHERE o.order_created_at IS NOT NULL
      AND CAST(o.order_created_at_cairo AS DATE)
          BETWEEN DATE'{from_date}' AND DATE'{to_date}'
    UNION
    SELECT s.order_id
    FROM {silver}.silver_order_status s
    WHERE s.event_date BETWEEN DATE'{from_date}' AND DATE'{to_date}'
  ),
  dates AS (
    SELECT DISTINCT e.event_date AS d
    FROM {silver}.silver_events e
    WHERE e.event_date BETWEEN DATE'{from_date}' AND DATE'{to_date}'
    UNION
    SELECT DISTINCT e.event_date
    FROM {silver}.silver_events e JOIN win_sessions w ON w.session_id = e.session_id
    UNION
    SELECT DISTINCT e.event_date
    FROM {silver}.silver_events e
    WHERE e.event_date < DATE'{from_date}' AND e.kafka_timestamp IS NOT NULL
      AND CAST(e.kafka_timestamp_cairo AS DATE)
          BETWEEN DATE'{from_date}' AND DATE'{to_date}'
    UNION
    SELECT DISTINCT to_date(CAST(l.date_key AS STRING), 'yyyyMMdd')
    FROM {gold}.fact_order_lines l JOIN win_orders w ON w.order_id = l.order_id
    WHERE l.date_key IS NOT NULL
  ),
  dkeys AS (SELECT DISTINCT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key FROM dates),
  ev AS (
    SELECT e.event_date, e.session_id, e.event_type, e.city, e.product_id
    FROM {silver}.silver_events e JOIN dates d ON d.d = e.event_date
    WHERE e.session_id IS NOT NULL
  ),
  cand AS (SELECT DISTINCT session_id FROM ev),
  timed AS (
    SELECT e.session_id, unix_micros(e.event_time) AS us
    FROM {silver}.silver_events e JOIN cand c ON c.session_id = e.session_id
    WHERE e.event_time IS NOT NULL
  ),
  burst AS (
    SELECT session_id,
           COUNT(1) OVER (PARTITION BY session_id ORDER BY us
                          RANGE BETWEEN 10000000 PRECEDING AND CURRENT ROW) AS n
    FROM timed
  ),
  bots AS (SELECT DISTINCT session_id FROM burst WHERE n > 100),
  evg AS (
    SELECT CAST(date_format(e.event_date, 'yyyyMMdd') AS INT) AS date_key,
           CASE WHEN e.city IS NULL OR TRIM(e.city) = '' THEN 'unknown' ELSE e.city END AS city,
           e.product_id,
           CAST(COUNT(DISTINCT CASE WHEN e.event_type = 'product_view' THEN e.session_id END) AS BIGINT)
             AS sessions_viewed,
           CAST(COUNT(DISTINCT CASE WHEN e.event_type = 'add_to_cart' THEN e.session_id END) AS BIGINT)
             AS add_to_cart
    FROM ev e LEFT ANTI JOIN bots b ON b.session_id = e.session_id
    WHERE e.product_id IS NOT NULL AND e.event_type IN ('product_view', 'add_to_cart')
    GROUP BY CAST(date_format(e.event_date, 'yyyyMMdd') AS INT),
             CASE WHEN e.city IS NULL OR TRIM(e.city) = '' THEN 'unknown' ELSE e.city END,
             e.product_id
  ),
  olg AS (
    SELECT l.date_key,
           CASE WHEN l.city IS NULL OR TRIM(l.city) = '' THEN 'unknown' ELSE l.city END AS city,
           l.product_id,
           CAST(COALESCE(SUM(l.quantity), 0) AS BIGINT) AS units,
           CAST(COALESCE(SUM(l.line_total), 0) AS DECIMAL(12,2)) AS revenue
    FROM {gold}.fact_order_lines l LEFT JOIN dkeys k ON k.date_key = l.date_key
    WHERE l.product_id IS NOT NULL AND l.date_key IS NOT NULL
      AND (l.date_key BETWEEN CAST(date_format(DATE'{from_date}', 'yyyyMMdd') AS INT)
                          AND CAST(date_format(DATE'{to_date}', 'yyyyMMdd') AS INT)
           OR k.date_key IS NOT NULL)
    GROUP BY l.date_key,
             CASE WHEN l.city IS NULL OR TRIM(l.city) = '' THEN 'unknown' ELSE l.city END,
             l.product_id
  )
  SELECT COALESCE(v.city, o.city) AS city,
         COALESCE(v.product_id, o.product_id) AS product_id,
         COALESCE(v.date_key, o.date_key) AS date_key,
         COALESCE(v.sessions_viewed, CAST(0 AS BIGINT)) AS sessions_viewed,
         COALESCE(v.add_to_cart, CAST(0 AS BIGINT)) AS add_to_cart,
         COALESCE(o.units, CAST(0 AS BIGINT)) AS units,
         COALESCE(o.revenue, CAST(0 AS DECIMAL(12,2))) AS revenue
  FROM evg v
  FULL OUTER JOIN olg o ON o.city = v.city AND o.product_id = v.product_id AND o.date_key = v.date_key
) AS s
ON t.city = s.city AND t.product_id = s.product_id AND t.date_key = s.date_key
WHEN MATCHED THEN UPDATE SET
  t.sessions_viewed = s.sessions_viewed, t.add_to_cart = s.add_to_cart, t.units = s.units,
  t.revenue = s.revenue
WHEN NOT MATCHED THEN INSERT (city, product_id, date_key, sessions_viewed, add_to_cart, units, revenue)
VALUES (s.city, s.product_id, s.date_key, s.sessions_viewed, s.add_to_cart, s.units, s.revenue)
WHEN NOT MATCHED BY SOURCE
  AND t.date_key BETWEEN CAST(date_format(DATE'{from_date}', 'yyyyMMdd') AS INT)
                     AND CAST(date_format(DATE'{to_date}', 'yyyyMMdd') AS INT) THEN DELETE;
