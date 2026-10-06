-- dim_customer: SCD2 built from the versions in silver_customers (spec 17.2 and 17.4).
-- Natural key (customer_id, valid_from). MERGE INSERT names its columns and never supplies customer_key (identity).
-- Daily batch: several Silver versions of one customer on the same Cairo day collapse to the LAST one (one new
-- version per day at most). A new version is created only when a tracked attribute (city, district,
-- primary_device) differs from the previous kept version; the other attributes (country, signup_date,
-- marketing_consent) are those of the first version of the run and are not back-filled.
-- valid_from = updated_at, valid_to = the next kept version updated_at, or 9999-12-31 00:00:00 UTC.
-- A kept version only ever gets a SMALLER valid_to (it is closed by a newer version), never a later one.
-- The unknown member (key -1, customer_id unknown) is seeded by apply_ddl and is never touched here.
MERGE INTO {gold}.dim_customer AS t
USING (
  WITH daily AS (
    SELECT c.customer_id, c.country, c.city, c.district, c.signup_date, c.primary_device, c.marketing_consent,
           c.updated_at,
           ROW_NUMBER() OVER (PARTITION BY c.customer_id,
                                           CAST(from_utc_timestamp(c.updated_at, 'Africa/Cairo') AS DATE)
                              ORDER BY c.updated_at DESC) AS rn
    FROM {silver}.silver_customers c
    WHERE c.customer_id IS NOT NULL AND c.updated_at IS NOT NULL
  ),
  last_of_day AS (
    SELECT customer_id, country, city, district, signup_date, primary_device, marketing_consent, updated_at
    FROM daily WHERE rn = 1
  ),
  flagged AS (
    SELECT l.*,
           ROW_NUMBER() OVER (PARTITION BY l.customer_id ORDER BY l.updated_at) AS vn,
           LAG(l.city) OVER (PARTITION BY l.customer_id ORDER BY l.updated_at) AS p_city,
           LAG(l.district) OVER (PARTITION BY l.customer_id ORDER BY l.updated_at) AS p_district,
           LAG(l.primary_device) OVER (PARTITION BY l.customer_id ORDER BY l.updated_at) AS p_device
    FROM last_of_day l
  ),
  kept AS (
    SELECT customer_id, country, city, district, signup_date, primary_device, marketing_consent, updated_at
    FROM flagged
    WHERE vn = 1 OR NOT (city <=> p_city) OR NOT (district <=> p_district) OR NOT (primary_device <=> p_device)
  )
  SELECT k.customer_id, k.country, k.city, k.district, k.signup_date, k.primary_device, k.marketing_consent,
         k.updated_at AS valid_from,
         COALESCE(LEAD(k.updated_at) OVER (PARTITION BY k.customer_id ORDER BY k.updated_at),
                  TIMESTAMP '9999-12-31 00:00:00+00:00') AS valid_to,
         LEAD(k.updated_at) OVER (PARTITION BY k.customer_id ORDER BY k.updated_at) IS NULL AS is_current
  FROM kept k
) AS s
ON t.customer_id = s.customer_id AND t.valid_from = s.valid_from
WHEN MATCHED AND (t.valid_to > s.valid_to OR (t.is_current AND NOT s.is_current)) THEN
  UPDATE SET t.valid_to = s.valid_to, t.is_current = s.is_current
WHEN NOT MATCHED THEN
  INSERT (customer_id, country, city, district, signup_date, primary_device, marketing_consent,
          valid_from, valid_to, is_current)
  VALUES (s.customer_id, s.country, s.city, s.district, s.signup_date, s.primary_device, s.marketing_consent,
          s.valid_from, s.valid_to, s.is_current);

-- A version that was kept on an earlier run and later collapsed away (a second change on the same day, after
-- the first run of that day) is still in the dimension: close it at the next version valid_from so that no
-- two rows are current and the versions never overlap.
MERGE INTO {gold}.dim_customer AS t
USING (
  SELECT customer_id, valid_from,
         LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) AS next_from
  FROM {gold}.dim_customer
  WHERE customer_id <> 'unknown'
) AS s
ON t.customer_id = s.customer_id AND t.valid_from = s.valid_from
WHEN MATCHED AND s.next_from IS NOT NULL AND (t.valid_to <> s.next_from OR t.is_current) THEN
  UPDATE SET t.valid_to = s.next_from, t.is_current = FALSE;
