-- agg_orders_daily: recomputed from fact_orders, never incremented (spec 17.1). Same semantics as
-- streaming/gold/agg_orders.py. Keys date_key, channel, payment_method, platform (NULL becomes unknown).
--
-- (WHEN clause order is fixed by the grammar: MATCHED, NOT MATCHED, NOT MATCHED BY SOURCE.)
-- Window handling, ONE atomic MERGE:
--   the source holds the freshly aggregated rows for the window date_keys plus the date_keys of every order the
--   fact_orders step recomputed (a late status row for an old order, or a shifted is_first_order, changes the
--   day the order was PLACED, spec 17.12 cohort view); matched keys get all measures updated, new keys are
--   inserted, and NOT MATCHED BY SOURCE deletes a target row inside the window that no longer has orders.
-- A failure rolls the whole statement back, the window is never left empty. Limit: a vanished key on a widened
-- date OUTSIDE the window is not deleted (a MERGE condition cannot hold a subquery); its measures are updated
-- only while the key still has orders.
MERGE INTO {gold}.agg_orders_daily AS t
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
  keys AS (
    SELECT f.date_key FROM {gold}.fact_orders f JOIN r ON r.order_id = f.order_id
    UNION
    SELECT f.date_key FROM {gold}.fact_orders f
    WHERE f.date_key BETWEEN CAST(date_format(DATE'{from_date}', 'yyyyMMdd') AS INT)
                         AND CAST(date_format(DATE'{to_date}', 'yyyyMMdd') AS INT)
  ),
  f AS (
    SELECT x.date_key,
           COALESCE(x.channel, 'unknown') AS channel,
           COALESCE(x.payment_method, 'unknown') AS payment_method,
           COALESCE(x.platform, 'unknown') AS platform,
           x.is_delivered, x.is_cancelled, x.is_returned, x.has_failed_attempt, x.is_first_order, x.order_total
    FROM {gold}.fact_orders x JOIN keys k ON k.date_key = x.date_key
  )
  SELECT date_key, channel, payment_method, platform,
         CAST(COUNT(1) AS BIGINT) AS orders_placed,
         CAST(SUM(CASE WHEN COALESCE(is_delivered, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS orders_delivered,
         CAST(SUM(CASE WHEN COALESCE(is_cancelled, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS orders_cancelled,
         CAST(SUM(CASE WHEN COALESCE(is_returned, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS orders_returned,
         CAST(SUM(CASE WHEN COALESCE(has_failed_attempt, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
           AS orders_with_failed_attempt,
         CAST(COALESCE(SUM(order_total), 0) AS DECIMAL(12,2)) AS revenue_placed,
         CAST(COALESCE(SUM(CASE WHEN COALESCE(is_delivered, FALSE) THEN order_total END), 0) AS DECIMAL(12,2))
           AS revenue_delivered,
         CAST(SUM(CASE WHEN COALESCE(is_first_order, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS new_customer_orders
  FROM f
  GROUP BY date_key, channel, payment_method, platform
) AS s
ON t.date_key = s.date_key AND t.channel = s.channel AND t.payment_method = s.payment_method
   AND t.platform = s.platform
WHEN MATCHED THEN UPDATE SET
  t.orders_placed = s.orders_placed, t.orders_delivered = s.orders_delivered,
  t.orders_cancelled = s.orders_cancelled, t.orders_returned = s.orders_returned,
  t.orders_with_failed_attempt = s.orders_with_failed_attempt, t.revenue_placed = s.revenue_placed,
  t.revenue_delivered = s.revenue_delivered, t.new_customer_orders = s.new_customer_orders
WHEN NOT MATCHED THEN INSERT (
  date_key, channel, payment_method, platform, orders_placed, orders_delivered, orders_cancelled,
  orders_returned, orders_with_failed_attempt, revenue_placed, revenue_delivered, new_customer_orders)
VALUES (
  s.date_key, s.channel, s.payment_method, s.platform, s.orders_placed, s.orders_delivered,
  s.orders_cancelled, s.orders_returned, s.orders_with_failed_attempt, s.revenue_placed,
  s.revenue_delivered, s.new_customer_orders)
WHEN NOT MATCHED BY SOURCE
  AND t.date_key BETWEEN CAST(date_format(DATE'{from_date}', 'yyyyMMdd') AS INT)
                     AND CAST(date_format(DATE'{to_date}', 'yyyyMMdd') AS INT) THEN DELETE;
