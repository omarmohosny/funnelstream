-- customer_360: one row per customer with their current state (stretch table of spec 16.2), recomputed in FULL on every
-- run and never incremented: it is a current-state table of a few thousand rows, so the window of the other steps does
-- not apply ({from_date} and {to_date} are not used). Same rules as the other Gold steps where they overlap.
--
-- Who: the CURRENT row of dim_customer (is_current), without the unknown member. country, city and district are the
--   customer's current values.
-- Events (silver_events, rows with a customer_id and a session_id; late rows are INCLUDED; bot sessions are excluded
--   with the same rule as agg_funnel_daily, recomputed here: a session is a bot when some event has more than 100
--   events of the same session in the 10 seconds up to it):
--   total_sessions = distinct sessions; product_views, add_to_cart_count and checkouts_count = event counts of
--   product_view, add_to_cart and checkout_started; last_seen_at = the newest event time.
-- Orders (fact_orders, every placed order of the customer):
--   orders_count = all placed orders; delivered_count and cancelled_count by the order's own flags;
--   total_spend (EGP) = order_total of the orders that were neither cancelled nor returned, and avg_order_value =
--   total_spend divided by the number of those orders (NULL when there are none);
--   first_order_at and last_order_at = the oldest and newest order_created_at;
--   days_since_last_order = whole Cairo calendar days from the last order to today in Cairo: the one column that
--   changes with the clock, as open_rate does;
--   is_repeat_buyer = at least two placed orders;
--   preferred_payment_method = the payment method used most often (ties: the first alphabetically);
--   last_campaign_id = the last-touch campaign of the newest order (NULL when that order was direct).
-- Category: top_category = the product category with the highest line_total over the customer's order lines (every
--   placed line, cancelled orders included, as revenue_placed does), joined to the CURRENT row of dim_product
--   (ties: the first alphabetically).
-- A customer without events or orders still has a row: counts are 0 (never NULL: a zero is a real fact), the
--   timestamps and the text columns are NULL.
--
-- ONE atomic MERGE: matched customers are updated, new customers inserted, and a customer that is no longer current
-- in dim_customer is deleted. A failure rolls the statement back.
MERGE INTO {gold}.customer_360 AS t
USING (
  WITH cust AS (
    SELECT customer_key, customer_id, country, city, district
    FROM {gold}.dim_customer
    WHERE is_current = TRUE AND customer_id IS NOT NULL AND customer_id <> 'unknown'
  ),
  cand AS (
    SELECT DISTINCT session_id
    FROM {silver}.silver_events
    WHERE customer_id IS NOT NULL AND session_id IS NOT NULL
  ),
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
  evt AS (
    SELECT e.customer_id,
           CAST(COUNT(DISTINCT e.session_id) AS BIGINT) AS total_sessions,
           CAST(SUM(CASE WHEN e.event_type = 'product_view' THEN 1 ELSE 0 END) AS BIGINT) AS product_views,
           CAST(SUM(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS BIGINT) AS add_to_cart_count,
           CAST(SUM(CASE WHEN e.event_type = 'checkout_started' THEN 1 ELSE 0 END) AS BIGINT) AS checkouts_count,
           MAX(e.event_time) AS last_seen_at
    FROM {silver}.silver_events e LEFT ANTI JOIN bots b ON b.session_id = e.session_id
    WHERE e.customer_id IS NOT NULL AND e.session_id IS NOT NULL
    GROUP BY e.customer_id
  ),
  ord AS (
    SELECT o.customer_id,
           CAST(COUNT(1) AS BIGINT) AS orders_count,
           CAST(SUM(CASE WHEN COALESCE(o.is_delivered, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS delivered_count,
           CAST(SUM(CASE WHEN COALESCE(o.is_cancelled, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS cancelled_count,
           CAST(SUM(CASE WHEN NOT COALESCE(o.is_cancelled, FALSE) AND NOT COALESCE(o.is_returned, FALSE)
                         THEN 1 ELSE 0 END) AS BIGINT) AS spend_orders,
           CAST(COALESCE(SUM(CASE WHEN NOT COALESCE(o.is_cancelled, FALSE) AND NOT COALESCE(o.is_returned, FALSE)
                                  THEN o.order_total END), 0) AS DECIMAL(12,2)) AS total_spend,
           MIN(o.order_created_at) AS first_order_at,
           MAX(o.order_created_at) AS last_order_at
    FROM {gold}.fact_orders o
    WHERE o.customer_id IS NOT NULL
    GROUP BY o.customer_id
  ),
  pay AS (
    SELECT customer_id, payment_method
    FROM (
      SELECT customer_id, payment_method,
             ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(1) DESC, payment_method ASC) AS rn
      FROM {gold}.fact_orders
      WHERE customer_id IS NOT NULL AND payment_method IS NOT NULL
      GROUP BY customer_id, payment_method
    ) ranked
    WHERE rn = 1
  ),
  lastc AS (
    SELECT customer_id, campaign_id
    FROM (
      SELECT customer_id, campaign_id,
             ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_created_at DESC, order_id DESC) AS rn
      FROM {gold}.fact_orders
      WHERE customer_id IS NOT NULL AND order_created_at IS NOT NULL
    ) ranked
    WHERE rn = 1
  ),
  topcat AS (
    SELECT customer_id, category
    FROM (
      SELECT l.customer_id, p.category,
             ROW_NUMBER() OVER (PARTITION BY l.customer_id ORDER BY SUM(l.line_total) DESC, p.category ASC) AS rn
      FROM {gold}.fact_order_lines l
      JOIN {gold}.dim_product p ON p.product_id = l.product_id AND p.is_current = TRUE
      WHERE l.customer_id IS NOT NULL AND p.category IS NOT NULL
      GROUP BY l.customer_id, p.category
    ) ranked
    WHERE rn = 1
  )
  SELECT c.customer_id,
         CAST(c.customer_key AS BIGINT) AS customer_key,
         c.country, c.city, c.district,
         COALESCE(e.total_sessions, CAST(0 AS BIGINT)) AS total_sessions,
         COALESCE(e.product_views, CAST(0 AS BIGINT)) AS product_views,
         COALESCE(e.add_to_cart_count, CAST(0 AS BIGINT)) AS add_to_cart_count,
         COALESCE(e.checkouts_count, CAST(0 AS BIGINT)) AS checkouts_count,
         COALESCE(o.orders_count, CAST(0 AS BIGINT)) AS orders_count,
         COALESCE(o.delivered_count, CAST(0 AS BIGINT)) AS delivered_count,
         COALESCE(o.cancelled_count, CAST(0 AS BIGINT)) AS cancelled_count,
         COALESCE(o.total_spend, CAST(0 AS DECIMAL(12,2))) AS total_spend,
         CAST(CASE WHEN o.spend_orders > 0 THEN CAST(o.total_spend AS DOUBLE) / o.spend_orders END
              AS DECIMAL(12,2)) AS avg_order_value,
         o.first_order_at, o.last_order_at, e.last_seen_at,
         CAST(CASE WHEN o.last_order_at IS NOT NULL
                   THEN datediff(CAST(from_utc_timestamp(current_timestamp(), 'Africa/Cairo') AS DATE),
                                 CAST(from_utc_timestamp(o.last_order_at, 'Africa/Cairo') AS DATE)) END AS INT)
         AS days_since_last_order,
         k.category AS top_category,
         p.payment_method AS preferred_payment_method,
         l.campaign_id AS last_campaign_id,
         COALESCE(o.orders_count, 0) >= 2 AS is_repeat_buyer
  FROM cust c
  LEFT JOIN evt e ON e.customer_id = c.customer_id
  LEFT JOIN ord o ON o.customer_id = c.customer_id
  LEFT JOIN pay p ON p.customer_id = c.customer_id
  LEFT JOIN lastc l ON l.customer_id = c.customer_id
  LEFT JOIN topcat k ON k.customer_id = c.customer_id
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET
  t.customer_key = s.customer_key, t.country = s.country, t.city = s.city, t.district = s.district,
  t.total_sessions = s.total_sessions, t.product_views = s.product_views, t.add_to_cart_count = s.add_to_cart_count,
  t.checkouts_count = s.checkouts_count, t.orders_count = s.orders_count, t.delivered_count = s.delivered_count,
  t.cancelled_count = s.cancelled_count, t.total_spend = s.total_spend, t.avg_order_value = s.avg_order_value,
  t.first_order_at = s.first_order_at, t.last_order_at = s.last_order_at, t.last_seen_at = s.last_seen_at,
  t.days_since_last_order = s.days_since_last_order, t.top_category = s.top_category,
  t.preferred_payment_method = s.preferred_payment_method, t.last_campaign_id = s.last_campaign_id,
  t.is_repeat_buyer = s.is_repeat_buyer
WHEN NOT MATCHED THEN INSERT (customer_id, customer_key, country, city, district, total_sessions, product_views,
  add_to_cart_count, checkouts_count, orders_count, delivered_count, cancelled_count, total_spend, avg_order_value,
  first_order_at, last_order_at, last_seen_at, days_since_last_order, top_category, preferred_payment_method,
  last_campaign_id, is_repeat_buyer)
VALUES (s.customer_id, s.customer_key, s.country, s.city, s.district, s.total_sessions, s.product_views,
  s.add_to_cart_count, s.checkouts_count, s.orders_count, s.delivered_count, s.cancelled_count, s.total_spend,
  s.avg_order_value, s.first_order_at, s.last_order_at, s.last_seen_at, s.days_since_last_order, s.top_category,
  s.preferred_payment_method, s.last_campaign_id, s.is_repeat_buyer)
WHEN NOT MATCHED BY SOURCE THEN DELETE;
