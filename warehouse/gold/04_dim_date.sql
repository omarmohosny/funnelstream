-- dim_date: one row per calendar day from dim_date_start to dim_date_end (warehouse/calendar_egypt.py).
-- date_key is yyyymmdd as an int. week is the ISO week number.
-- Egypt weekend is Friday and Saturday (Spark dayofweek: Sunday = 1, so Friday = 6 and Saturday = 7).
-- is_friday_saturday carries the same value as is_weekend on purpose: both columns are in the table spec.
-- Ramadan and Eid ranges are APPROXIMATE (moon sighting), see calendar_egypt.py. is_holiday = Eid day or one
-- of the fixed public holidays (Jan 7, Jan 25, Apr 25, May 1, Jul 23, Oct 6).
MERGE INTO {gold}.dim_date AS t
USING (
  WITH days AS (
    SELECT explode(sequence(DATE'{dim_date_start}', DATE'{dim_date_end}', INTERVAL 1 DAY)) AS d
  ),
  ranges AS (
    SELECT kind, first_day, last_day FROM VALUES {calendar_ranges} AS c(kind, first_day, last_day)
  ),
  fixed AS (
    SELECT m, dd FROM VALUES {fixed_holidays} AS h(m, dd)
  ),
  range_flags AS (
    SELECT days.d,
           MAX(CASE WHEN ranges.kind = 'ramadan' THEN 1 ELSE 0 END) AS ramadan,
           MAX(CASE WHEN ranges.kind = 'eid' THEN 1 ELSE 0 END) AS eid
    FROM days LEFT JOIN ranges ON days.d BETWEEN ranges.first_day AND ranges.last_day
    GROUP BY days.d
  ),
  fixed_flags AS (
    SELECT days.d, MAX(CASE WHEN fixed.m IS NULL THEN 0 ELSE 1 END) AS fixed_holiday
    FROM days LEFT JOIN fixed ON month(days.d) = fixed.m AND dayofmonth(days.d) = fixed.dd
    GROUP BY days.d
  )
  SELECT CAST(date_format(r.d, 'yyyyMMdd') AS INT) AS date_key,
         r.d AS date,
         CAST(weekofyear(r.d) AS INT) AS week,
         CAST(month(r.d) AS INT) AS month,
         dayofweek(r.d) IN (6, 7) AS is_weekend,
         dayofweek(r.d) IN (6, 7) AS is_friday_saturday,
         r.ramadan = 1 AS is_ramadan,
         r.eid = 1 AS is_eid,
         (r.eid = 1 OR f.fixed_holiday = 1) AS is_holiday
  FROM range_flags r JOIN fixed_flags f ON f.d = r.d
) AS s
ON t.date_key = s.date_key
WHEN MATCHED AND (NOT (t.date <=> s.date) OR NOT (t.week <=> s.week) OR NOT (t.month <=> s.month)
                  OR NOT (t.is_weekend <=> s.is_weekend) OR NOT (t.is_friday_saturday <=> s.is_friday_saturday)
                  OR NOT (t.is_ramadan <=> s.is_ramadan) OR NOT (t.is_eid <=> s.is_eid)
                  OR NOT (t.is_holiday <=> s.is_holiday)) THEN
  UPDATE SET t.date = s.date, t.week = s.week, t.month = s.month, t.is_weekend = s.is_weekend,
             t.is_friday_saturday = s.is_friday_saturday, t.is_ramadan = s.is_ramadan, t.is_eid = s.is_eid,
             t.is_holiday = s.is_holiday
WHEN NOT MATCHED THEN
  INSERT (date_key, date, week, month, is_weekend, is_friday_saturday, is_ramadan, is_eid, is_holiday)
  VALUES (s.date_key, s.date, s.week, s.month, s.is_weekend, s.is_friday_saturday, s.is_ramadan, s.is_eid,
          s.is_holiday);
