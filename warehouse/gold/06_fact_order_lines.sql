-- fact_order_lines: one row per order line. Same semantics as streaming/gold/orders.py build_fact_order_lines.
-- Recomputed for the orders in the window: order_created_at Cairo date in the window, or a silver_order_status
-- row with event_date in the window (spec 17.1). customer_key and product_key are the dimension versions valid
-- at order_created_at (valid_from inclusive, valid_to exclusive), -1 when there is none or no natural id; the
-- natural ids stay beside them, so every recomputed line repairs a -1 once the dimension row exists.
-- date_key, city and district come from the order (Cairo date). Items whose order is not in silver_orders
-- are dropped.
MERGE INTO {gold}.fact_order_lines AS t
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
  base AS (
    SELECT i.order_id, i.line_no, o.customer_id, i.product_id, o.order_created_at, o.city, o.district,
           i.quantity, i.line_total
    FROM {silver}.silver_order_items i
      JOIN {silver}.silver_orders o ON o.order_id = i.order_id
      JOIN win w ON w.order_id = i.order_id
  ),
  ck AS (
    SELECT x.order_id, x.line_no, x.customer_key
    FROM (
      SELECT b.order_id, b.line_no, dc.customer_key,
             ROW_NUMBER() OVER (PARTITION BY b.order_id, b.line_no
                                ORDER BY dc.valid_from DESC, dc.customer_key DESC) AS rn
      FROM base b LEFT JOIN {gold}.dim_customer dc
        ON dc.customer_id = b.customer_id
       AND b.order_created_at >= dc.valid_from AND b.order_created_at < dc.valid_to
    ) x
    WHERE x.rn = 1
  ),
  pk AS (
    SELECT x.order_id, x.line_no, x.product_key
    FROM (
      SELECT b.order_id, b.line_no, dp.product_key,
             ROW_NUMBER() OVER (PARTITION BY b.order_id, b.line_no
                                ORDER BY dp.valid_from DESC, dp.product_key DESC) AS rn
      FROM base b LEFT JOIN {gold}.dim_product dp
        ON dp.product_id = b.product_id
       AND b.order_created_at >= dp.valid_from AND b.order_created_at < dp.valid_to
    ) x
    WHERE x.rn = 1
  )
  SELECT b.order_id, b.line_no,
         COALESCE(ck.customer_key, CAST(-1 AS BIGINT)) AS customer_key,
         b.customer_id,
         COALESCE(pk.product_key, CAST(-1 AS BIGINT)) AS product_key,
         b.product_id,
         CAST(date_format(from_utc_timestamp(b.order_created_at, 'Africa/Cairo'), 'yyyyMMdd') AS INT) AS date_key,
         b.city, b.district, b.quantity, b.line_total
  FROM base b
    JOIN ck ON ck.order_id = b.order_id AND ck.line_no = b.line_no
    JOIN pk ON pk.order_id = b.order_id AND pk.line_no = b.line_no
) AS s
ON t.order_id = s.order_id AND t.line_no = s.line_no
WHEN MATCHED THEN UPDATE SET
  t.customer_key = s.customer_key, t.customer_id = s.customer_id, t.product_key = s.product_key,
  t.product_id = s.product_id, t.date_key = s.date_key, t.city = s.city, t.district = s.district,
  t.quantity = s.quantity, t.line_total = s.line_total
WHEN NOT MATCHED THEN INSERT (
  order_id, line_no, customer_key, customer_id, product_key, product_id, date_key, city, district, quantity,
  line_total)
VALUES (
  s.order_id, s.line_no, s.customer_key, s.customer_id, s.product_key, s.product_id, s.date_key, s.city,
  s.district, s.quantity, s.line_total);
