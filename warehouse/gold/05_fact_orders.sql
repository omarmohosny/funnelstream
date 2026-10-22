-- fact_orders: one row per order, current state. Same semantics as streaming/gold/orders.py build_fact_orders.
--
-- Which orders are recomputed (spec 17.1, window = from_date .. to_date, Cairo dates):
--   win = orders whose order_created_at Cairo date is in the window, or that have a silver_order_status row
--         whose event_date is in the window;
--   r   = win plus every later order (created at or after the earliest order of that customer in win) of the
--         customers in win, because order_seq and is_first_order of those orders depend on the earlier order.
-- order_seq is computed over ALL orders of the customer (seq), not only over r. An anonymous order (NULL customer_id)
-- has order_seq NULL and is_first_order FALSE: it has no customer history, so it never counts as a new customer.
--
-- Rules: current status = highest status_rank, a counted cancel (no order_shipped row EARLIER than the cancel)
-- is terminal, rank ties go to the latest status_time (spec 17.6); delivery_lead_hours = exact hours from
-- microsecond times rounded HALF UP to 2 decimals; customer_key = dim_customer version valid at order_created_at
-- (valid_from inclusive, valid_to exclusive), -1 when there is none (a later build repairs it).
-- Orders with a NULL order_created_at are left out like the reference does; the build reports their number.
-- campaign_id is inserted NULL and is NOT in the UPDATE list: fact_attribution (Task 13) owns it.
MERGE INTO {gold}.fact_orders AS t
USING (
  WITH win AS (
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
  first_in_win AS (
    SELECT o.customer_id, MIN(o.order_created_at) AS first_at
    FROM {silver}.silver_orders o JOIN win w ON w.order_id = o.order_id
    WHERE o.customer_id IS NOT NULL AND o.order_created_at IS NOT NULL
    GROUP BY o.customer_id
  ),
  r AS (
    SELECT order_id FROM win
    UNION
    SELECT o.order_id
    FROM {silver}.silver_orders o JOIN first_in_win f ON o.customer_id = f.customer_id
    WHERE o.order_created_at >= f.first_at
  ),
  st AS (
    SELECT s.order_id, s.status, s.status_time, s.carrier, s.status_rank AS rnk,
           MIN(CASE WHEN s.status = 'order_shipped' THEN s.status_time END)
             OVER (PARTITION BY s.order_id) AS first_ship
    FROM {silver}.silver_order_status s JOIN r ON r.order_id = s.order_id
  ),
  st2 AS (
    SELECT st.*,
           (st.status = 'order_cancelled' AND NOT COALESCE(st.first_ship < st.status_time, FALSE)) AS counted
    FROM st
  ),
  st3 AS (
    SELECT st2.*,
           (st2.status <> 'order_cancelled' OR st2.counted) AS usable,
           CASE WHEN st2.counted THEN 99 ELSE st2.rnk END AS prio
    FROM st2
  ),
  cur AS (
    SELECT x.order_id, x.status AS current_status, CAST(x.rnk AS INT) AS status_rank
    FROM (
      SELECT st3.*,
             ROW_NUMBER() OVER (PARTITION BY st3.order_id
                                ORDER BY st3.prio DESC, st3.status_time DESC, st3.status) AS rn
      FROM st3 WHERE st3.usable
    ) x
    WHERE x.rn = 1
  ),
  agg AS (
    SELECT order_id,
           MAX(CASE WHEN status = 'order_shipped' THEN carrier END) AS carrier,
           MIN(CASE WHEN status = 'delivered' THEN status_time END) AS delivered_at,
           MIN(CASE WHEN counted THEN status_time END) AS counted_cancel_at,
           MIN(CASE WHEN status = 'order_returned' THEN status_time END) AS returned_at,
           CAST(SUM(CASE WHEN status = 'delivery_failed' THEN 1 ELSE 0 END) AS INT) AS delivery_failed_count,
           MIN(CASE WHEN status = 'out_for_delivery' THEN status_time END) AS first_ofd_at
    FROM st3 GROUP BY order_id
  ),
  d AS (
    SELECT so.order_id, so.customer_id, so.session_id, so.channel, so.platform, so.utm_source, so.utm_campaign,
           so.click_id, so.payment_method, a.carrier, so.city, so.district, so.country, so.order_total,
           c.current_status, c.status_rank, so.order_created_at, so.order_created_at_cairo, a.delivered_at,
           a.counted_cancel_at AS cancelled_at, a.returned_at, a.delivery_failed_count,
           (a.delivered_at IS NOT NULL) AS is_delivered,
           (a.counted_cancel_at IS NOT NULL AND a.delivered_at IS NULL) AS is_cancelled,
           (a.returned_at IS NOT NULL) AS is_returned,
           (a.delivery_failed_count > 0) AS has_failed_attempt,
           COALESCE(a.delivered_at IS NOT NULL AND a.delivery_failed_count = 0
                    AND a.first_ofd_at < a.delivered_at, FALSE) AS first_attempt_delivered,
           CAST(ROUND(CAST(unix_micros(a.delivered_at) - unix_micros(so.order_created_at) AS DECIMAL(24,0))
                      / CAST(3600000000 AS DECIMAL(24,0)), 2) AS DECIMAL(8,2)) AS delivery_lead_hours
    FROM {silver}.silver_orders so
      JOIN r ON r.order_id = so.order_id
      JOIN agg a ON a.order_id = so.order_id
      JOIN cur c ON c.order_id = so.order_id
    WHERE so.order_created_at IS NOT NULL
  ),
  ck AS (
    SELECT x.order_id, x.customer_key
    FROM (
      SELECT d.order_id, dc.customer_key,
             ROW_NUMBER() OVER (PARTITION BY d.order_id ORDER BY dc.valid_from DESC, dc.customer_key DESC) AS rn
      FROM d LEFT JOIN {gold}.dim_customer dc
        ON dc.customer_id = d.customer_id
       AND d.order_created_at >= dc.valid_from AND d.order_created_at < dc.valid_to
    ) x
    WHERE x.rn = 1
  ),
  seq AS (
    SELECT so.order_id,
           CASE WHEN so.customer_id IS NULL THEN NULL
                ELSE CAST(ROW_NUMBER() OVER (PARTITION BY so.customer_id ORDER BY so.order_created_at, so.order_id)
                          AS INT) END AS order_seq
    FROM {silver}.silver_orders so
    WHERE so.order_created_at IS NOT NULL
      AND EXISTS (SELECT 1 FROM {silver}.silver_order_status s WHERE s.order_id = so.order_id)
  )
  SELECT d.order_id,
         COALESCE(ck.customer_key, CAST(-1 AS BIGINT)) AS customer_key,
         d.customer_id, d.session_id, d.channel, d.platform, d.utm_source, d.utm_campaign, d.click_id,
         CAST(NULL AS STRING) AS campaign_id,
         d.payment_method, d.carrier, d.city, d.district, d.country, d.order_total, d.current_status,
         d.status_rank, d.order_created_at, d.delivered_at, d.cancelled_at, d.returned_at,
         d.delivery_lead_hours, d.delivery_failed_count, d.has_failed_attempt, d.first_attempt_delivered,
         d.is_cancelled, d.is_delivered, d.is_returned,
         seq.order_seq,
         COALESCE(seq.order_seq = 1, FALSE) AS is_first_order,
         CAST(date_format(d.order_created_at_cairo, 'yyyyMMdd') AS INT) AS date_key
  FROM d JOIN ck ON ck.order_id = d.order_id JOIN seq ON seq.order_id = d.order_id
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  t.customer_key = s.customer_key, t.customer_id = s.customer_id, t.session_id = s.session_id,
  t.channel = s.channel, t.platform = s.platform, t.utm_source = s.utm_source, t.utm_campaign = s.utm_campaign,
  t.click_id = s.click_id, t.payment_method = s.payment_method, t.carrier = s.carrier, t.city = s.city,
  t.district = s.district, t.country = s.country, t.order_total = s.order_total,
  t.current_status = s.current_status, t.status_rank = s.status_rank, t.order_created_at = s.order_created_at,
  t.delivered_at = s.delivered_at, t.cancelled_at = s.cancelled_at, t.returned_at = s.returned_at,
  t.delivery_lead_hours = s.delivery_lead_hours, t.delivery_failed_count = s.delivery_failed_count,
  t.has_failed_attempt = s.has_failed_attempt, t.first_attempt_delivered = s.first_attempt_delivered,
  t.is_cancelled = s.is_cancelled, t.is_delivered = s.is_delivered, t.is_returned = s.is_returned,
  t.order_seq = s.order_seq, t.is_first_order = s.is_first_order, t.date_key = s.date_key
WHEN NOT MATCHED THEN INSERT (
  order_id, customer_key, customer_id, session_id, channel, platform, utm_source, utm_campaign, click_id,
  campaign_id, payment_method, carrier, city, district, country, order_total, current_status, status_rank,
  order_created_at, delivered_at, cancelled_at, returned_at, delivery_lead_hours, delivery_failed_count,
  has_failed_attempt, first_attempt_delivered, is_cancelled, is_delivered, is_returned, order_seq,
  is_first_order, date_key)
VALUES (
  s.order_id, s.customer_key, s.customer_id, s.session_id, s.channel, s.platform, s.utm_source, s.utm_campaign,
  s.click_id, s.campaign_id, s.payment_method, s.carrier, s.city, s.district, s.country, s.order_total,
  s.current_status, s.status_rank, s.order_created_at, s.delivered_at, s.cancelled_at, s.returned_at,
  s.delivery_lead_hours, s.delivery_failed_count, s.has_failed_attempt, s.first_attempt_delivered,
  s.is_cancelled, s.is_delivered, s.is_returned, s.order_seq, s.is_first_order, s.date_key);
