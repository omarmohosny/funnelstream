-- fact_attribution: exactly two rows per order (model last_touch and first_touch), key (order_id, model).
-- Production logic. The reference is streaming/gold/attribution.py (the Spark parity test compares them).
--
-- Click (spec 4): the FIRST page_view or product_view event of a session (event_time, ties by event_id) that has a
-- utm_campaign. Sessions that are bots are excluded (rule recomputed over the whole events table, as in the
-- funnel step: more than 100 events of the session in 10 seconds). Rows of quarantined messages are not in
-- silver_events, so they never produce a click. The campaign is silver_events.campaign_id when populated, else the
-- dim_campaign row with the same utm_campaign (the unknown member never matches). An unresolved click is dropped.
-- The click carries the customer_id of its own event: no linking of anonymous pre-login sessions (spec 17.11).
--
-- Resolution per order (fact_orders row with an order_created_at):
--   1. own session: the click whose click_id equals the order click_id (no time condition)
--   2. no own click and the order has a customer_id: the customer's clicks from ANY session in
--      [order_created_at - 7 days, order_created_at): last_touch takes the latest, first_touch the earliest
--   3. otherwise direct: campaign_id NULL and click_id NULL
-- Ties on click time: smallest click_id (NULL last), then smallest session_id.
--
-- Which orders are recomputed (window = from_date .. to_date, Cairo dates):
--   the orders of the fact_orders step window (order date or a status row event_date in the window), the orders
--   with no fact_attribution row yet, and the orders whose attribution inputs changed: orders reached by a click
--   candidate (own click_id, or same customer within 7 days after it) of any session that has an event dated in
--   the window or a late event (event_date before the window, kafka_timestamp Cairo date inside it).
-- (WHEN clause order is fixed by the grammar.) ONE atomic MERGE: matched keys with changed values are updated, new
-- keys inserted, and an order that no longer exists in fact_orders is deleted. A MERGE NOT MATCHED BY SOURCE
-- condition cannot be limited to the window orders (no date column, no subquery), so those rows enter the source
-- as tombstones (gone = TRUE) and WHEN MATCHED AND s.gone deletes them. A failure rolls the statement back.
-- Limits: a click that changes for an order OUTSIDE these rules (for example an event of an old session that
-- arrives with a kafka_timestamp before the window and an event_date before the window) is found by the next
-- covering window or --full.
MERGE INTO {gold}.fact_attribution AS t
USING (
  WITH win AS (
    SELECT o.order_id
    FROM {silver}.silver_orders o
    WHERE o.order_created_at IS NOT NULL
      AND CAST(from_utc_timestamp(o.order_created_at, 'Africa/Cairo') AS DATE)
          BETWEEN DATE'{from_date}' AND DATE'{to_date}'
    UNION
    SELECT s.order_id
    FROM {silver}.silver_order_status s
    WHERE s.event_date BETWEEN DATE'{from_date}' AND DATE'{to_date}'
  ),
  chg_sessions AS (
    SELECT DISTINCT e.session_id
    FROM {silver}.silver_events e
    WHERE e.session_id IS NOT NULL
      AND (e.event_date BETWEEN DATE'{from_date}' AND DATE'{to_date}'
           OR (e.event_date < DATE'{from_date}' AND e.kafka_timestamp IS NOT NULL
               AND CAST(from_utc_timestamp(e.kafka_timestamp, 'Africa/Cairo') AS DATE)
                   BETWEEN DATE'{from_date}' AND DATE'{to_date}'))
  ),
  cand AS (
    SELECT e.session_id, e.customer_id, e.click_id, e.campaign_id, e.utm_campaign, e.event_time, e.event_id
    FROM {silver}.silver_events e
    WHERE e.session_id IS NOT NULL AND e.event_time IS NOT NULL AND e.utm_campaign IS NOT NULL
      AND e.event_type IN ('page_view', 'product_view')
  ),
  timed AS (
    SELECT e.session_id, unix_micros(e.event_time) AS us
    FROM {silver}.silver_events e JOIN (SELECT DISTINCT session_id FROM cand) c ON c.session_id = e.session_id
    WHERE e.event_time IS NOT NULL
  ),
  burst AS (
    SELECT session_id,
           COUNT(1) OVER (PARTITION BY session_id ORDER BY us
                          RANGE BETWEEN 10000000 PRECEDING AND CURRENT ROW) AS n
    FROM timed
  ),
  bots AS (SELECT DISTINCT session_id FROM burst WHERE n > 100),
  fc AS (
    SELECT x.session_id, x.customer_id, x.click_id, x.campaign_id, x.utm_campaign, x.event_time
    FROM (
      SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.session_id ORDER BY c.event_time, c.event_id) AS rn
      FROM cand c LEFT ANTI JOIN bots b ON b.session_id = c.session_id
    ) x
    WHERE x.rn = 1
  ),
  camp AS (
    SELECT d.utm_campaign, MIN(d.campaign_id) AS campaign_id
    FROM {gold}.dim_campaign d
    WHERE d.campaign_id <> 'unknown' AND d.utm_campaign IS NOT NULL
    GROUP BY d.utm_campaign
  ),
  clicks AS (
    SELECT f.click_id, f.session_id, f.customer_id, f.event_time AS click_time,
           COALESCE(f.campaign_id, m.campaign_id) AS campaign_id
    FROM fc f LEFT JOIN camp m ON m.utm_campaign = f.utm_campaign
    WHERE COALESCE(f.campaign_id, m.campaign_id) IS NOT NULL
  ),
  uclicks AS (
    SELECT y.click_id, y.campaign_id
    FROM (
      SELECT k.click_id, k.campaign_id,
             ROW_NUMBER() OVER (PARTITION BY k.click_id ORDER BY k.click_time, k.session_id) AS rn
      FROM clicks k WHERE k.click_id IS NOT NULL
    ) y
    WHERE y.rn = 1
  ),
  chg_cand AS (
    SELECT c.customer_id, c.click_id, c.event_time
    FROM cand c JOIN chg_sessions g ON g.session_id = c.session_id
  ),
  ord_ids AS (
    SELECT w.order_id FROM win w
    UNION
    SELECT f.order_id FROM {gold}.fact_orders f JOIN chg_cand c ON f.click_id = c.click_id
    UNION
    SELECT f.order_id
    FROM {gold}.fact_orders f
      JOIN chg_cand c ON f.customer_id = c.customer_id
                     AND f.order_created_at >= c.event_time
                     AND f.order_created_at <= c.event_time + INTERVAL 7 DAYS
    UNION
    SELECT f.order_id FROM {gold}.fact_orders f LEFT ANTI JOIN {gold}.fact_attribution a ON a.order_id = f.order_id
  ),
  ord AS (
    SELECT f.order_id, f.customer_id, f.click_id, f.order_created_at, f.order_total
    FROM {gold}.fact_orders f JOIN ord_ids i ON i.order_id = f.order_id
    WHERE f.order_created_at IS NOT NULL
  ),
  own AS (
    SELECT o.order_id, u.campaign_id, u.click_id
    FROM ord o JOIN uclicks u ON u.click_id = o.click_id
  ),
  fb AS (
    SELECT z.order_id, z.campaign_id, z.click_id, z.rn_last, z.rn_first
    FROM (
      SELECT o.order_id, k.campaign_id, k.click_id,
             ROW_NUMBER() OVER (PARTITION BY o.order_id
                                ORDER BY k.click_time DESC, k.click_id ASC NULLS LAST, k.session_id ASC) AS rn_last,
             ROW_NUMBER() OVER (PARTITION BY o.order_id
                                ORDER BY k.click_time ASC, k.click_id ASC NULLS LAST, k.session_id ASC) AS rn_first
      FROM ord o JOIN clicks k
        ON o.customer_id IS NOT NULL AND k.customer_id = o.customer_id
       AND k.click_time >= o.order_created_at - INTERVAL 7 DAYS AND k.click_time < o.order_created_at
    ) z
    WHERE z.rn_last = 1 OR z.rn_first = 1
  ),
  res AS (
    SELECT o.order_id, 'last_touch' AS model,
           CASE WHEN w.order_id IS NOT NULL THEN w.campaign_id ELSE l.campaign_id END AS campaign_id,
           CASE WHEN w.order_id IS NOT NULL THEN w.click_id ELSE l.click_id END AS click_id,
           o.order_total
    FROM ord o LEFT JOIN own w ON w.order_id = o.order_id
               LEFT JOIN fb l ON l.order_id = o.order_id AND l.rn_last = 1
    UNION ALL
    SELECT o.order_id, 'first_touch' AS model,
           CASE WHEN w.order_id IS NOT NULL THEN w.campaign_id ELSE l.campaign_id END AS campaign_id,
           CASE WHEN w.order_id IS NOT NULL THEN w.click_id ELSE l.click_id END AS click_id,
           o.order_total
    FROM ord o LEFT JOIN own w ON w.order_id = o.order_id
               LEFT JOIN fb l ON l.order_id = o.order_id AND l.rn_first = 1
  )
  SELECT r.order_id, r.model, r.campaign_id, r.click_id, r.order_total, FALSE AS gone
  FROM res r
  UNION ALL
  SELECT a.order_id, a.model, a.campaign_id, a.click_id, a.order_total, TRUE AS gone
  FROM {gold}.fact_attribution a LEFT ANTI JOIN {gold}.fact_orders f ON f.order_id = a.order_id
) AS s
ON t.order_id = s.order_id AND t.model = s.model
WHEN MATCHED AND s.gone THEN DELETE
WHEN MATCHED AND (NOT (t.campaign_id <=> s.campaign_id) OR NOT (t.click_id <=> s.click_id)
                  OR NOT (t.order_total <=> s.order_total)) THEN UPDATE SET
  t.campaign_id = s.campaign_id, t.click_id = s.click_id, t.order_total = s.order_total
WHEN NOT MATCHED AND NOT s.gone THEN INSERT (order_id, model, campaign_id, click_id, order_total)
VALUES (s.order_id, s.model, s.campaign_id, s.click_id, s.order_total);

-- fact_orders.campaign_id = the last_touch campaign of the order (NULL for direct orders). This is the only column
-- of fact_orders this step touches, and the fact_orders step never overwrites it. Only changed values are written,
-- so a rerun writes nothing. Run over all attributed orders: cheap, and it repairs any order the step above
-- recomputed or the fact_orders step inserted.
MERGE INTO {gold}.fact_orders AS t
USING (
  SELECT x.order_id, x.campaign_id
  FROM (
    SELECT a.order_id, a.campaign_id,
           ROW_NUMBER() OVER (PARTITION BY a.order_id ORDER BY a.campaign_id) AS rn
    FROM {gold}.fact_attribution a
    WHERE a.model = 'last_touch'
  ) x
  WHERE x.rn = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND NOT (t.campaign_id <=> s.campaign_id) THEN UPDATE SET t.campaign_id = s.campaign_id;
