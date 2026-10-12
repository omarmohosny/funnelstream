-- dim_product: SCD2 built from the versions in silver_products, same pattern as dim_customer.
-- Natural key (product_id, valid_from). Tracked attributes: category, price, is_active. One new version per product per
-- Cairo day at most (the last of the day wins). name is that of the first version of a run; a product that is deactivated or
-- reactivated starts a new version, so is_active (and rec_product eligibility) is never stale.
-- A kept version only ever gets a SMALLER valid_to (it is closed by a newer version), never a later one.
-- MERGE INSERT names its columns and never supplies product_key (identity). The unknown member (key -1) is
-- seeded by apply_ddl and never touched here.
MERGE INTO {gold}.dim_product AS t
USING (
  WITH daily AS (
    SELECT p.product_id, p.name, p.category, p.price, p.is_active, p.updated_at,
           ROW_NUMBER() OVER (PARTITION BY p.product_id,
                                           CAST(from_utc_timestamp(p.updated_at, 'Africa/Cairo') AS DATE)
                              ORDER BY p.updated_at DESC) AS rn
    FROM {silver}.silver_products p
    WHERE p.product_id IS NOT NULL AND p.updated_at IS NOT NULL
  ),
  last_of_day AS (
    SELECT product_id, name, category, price, is_active, updated_at FROM daily WHERE rn = 1
  ),
  flagged AS (
    SELECT l.*,
           ROW_NUMBER() OVER (PARTITION BY l.product_id ORDER BY l.updated_at) AS vn,
           LAG(l.category) OVER (PARTITION BY l.product_id ORDER BY l.updated_at) AS p_category,
           LAG(l.price) OVER (PARTITION BY l.product_id ORDER BY l.updated_at) AS p_price,
           LAG(l.is_active) OVER (PARTITION BY l.product_id ORDER BY l.updated_at) AS p_active
    FROM last_of_day l
  ),
  kept AS (
    SELECT product_id, name, category, price, is_active, updated_at
    FROM flagged
    WHERE vn = 1 OR NOT (category <=> p_category) OR NOT (price <=> p_price)
       OR NOT (is_active <=> p_active)
  )
  SELECT k.product_id, k.name, k.category, CAST(k.price AS DECIMAL(12,2)) AS price, k.is_active,
         k.updated_at AS valid_from,
         COALESCE(LEAD(k.updated_at) OVER (PARTITION BY k.product_id ORDER BY k.updated_at),
                  TIMESTAMP '9999-12-31 00:00:00+00:00') AS valid_to,
         LEAD(k.updated_at) OVER (PARTITION BY k.product_id ORDER BY k.updated_at) IS NULL AS is_current
  FROM kept k
) AS s
ON t.product_id = s.product_id AND t.valid_from = s.valid_from
WHEN MATCHED AND (t.valid_to > s.valid_to OR (t.is_current AND NOT s.is_current)) THEN
  UPDATE SET t.valid_to = s.valid_to, t.is_current = s.is_current
WHEN NOT MATCHED THEN
  INSERT (product_id, name, category, price, is_active, valid_from, valid_to, is_current)
  VALUES (s.product_id, s.name, s.category, s.price, s.is_active, s.valid_from, s.valid_to, s.is_current);

-- Close a version that was kept on an earlier run and later collapsed away (see dim_customer).
MERGE INTO {gold}.dim_product AS t
USING (
  SELECT product_id, valid_from,
         LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from) AS next_from
  FROM {gold}.dim_product
  WHERE product_id <> 'unknown'
) AS s
ON t.product_id = s.product_id AND t.valid_from = s.valid_from
WHEN MATCHED AND s.next_from IS NOT NULL AND (t.valid_to <> s.next_from OR t.is_current) THEN
  UPDATE SET t.valid_to = s.next_from, t.is_current = FALSE;
