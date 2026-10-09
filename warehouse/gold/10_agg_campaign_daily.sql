-- agg_campaign_daily: campaign x Cairo report date x attribution model (always last_touch here), recomputed from
-- silver_campaign_snapshots (ad measures) and fact_orders + fact_attribution (order measures), never incremented
-- (spec 17.1). Same semantics as streaming/gold/agg_campaign.py.
--
-- Ad measures: only the row with the LATEST pulled_at per (campaign_id, report_date) counts; earlier pulls stay in
--   Silver and are never summed. Ties on pulled_at: the row with the greatest impressions, then clicks,
--   platform_conversions, spend, ad_platform. A pull without pulled_at loses to any pull that has one. Snapshot rows
--   with a NULL campaign_id or report_date are ignored. A day without an ad row has NULL ad measures (not 0).
-- Order measures: orders whose last_touch campaign is not NULL and not 'unknown', on the Cairo date the order was
--   PLACED (fact_orders.date_key), so delivered revenue sits on the order day. A day with ad data and no orders
--   has 0 counts and 0.00 revenue and NULL order rates.
-- Rates are DOUBLE division cast to DECIMAL(8,4); money ratios are cast to DECIMAL(12,2); NULL when the
--   denominator is NULL or 0 (cpa, cost_per_delivered_order, roas_*: also when spend is NULL or 0).
-- open_rate uses current_timestamp(): it changes with the wall clock, an order turns "open" 14 days after it was
--   placed even when nothing else changed. Only recomputed rows pick that up.
--
-- (WHEN clause order is fixed by the grammar: MATCHED, NOT MATCHED, NOT MATCHED BY SOURCE.)
-- Window handling, ONE atomic MERGE: the source holds the campaign-days whose report_date is in the window plus
--   every date of an order the fact_orders step recomputed (a late status row for an old order moves the outcome
--   counts of the day it was PLACED, as in agg_orders_daily). Matched keys get all measures updated, new keys are
--   inserted, and NOT MATCHED BY SOURCE deletes a target row inside the window that has neither ad data nor
--   orders any more. A failure rolls the whole statement back. Limits: a vanished key on a widened date OUTSIDE the
--   window is not deleted (a MERGE condition cannot hold a subquery); a late snapshot pull for an old report day
--   is only picked up when that day is in the window (or is the day of a recomputed order).
MERGE INTO {gold}.agg_campaign_daily AS t
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
  days AS (
    SELECT DISTINCT to_date(CAST(f.date_key AS STRING), 'yyyyMMdd') AS report_date
    FROM {gold}.fact_orders f JOIN r ON r.order_id = f.order_id
    WHERE f.date_key IS NOT NULL
  ),
  snap AS (
    SELECT x.campaign_id, x.report_date, x.ad_platform, x.impressions, x.clicks, x.platform_conversions, x.spend,
           ROW_NUMBER() OVER (PARTITION BY x.campaign_id, x.report_date
                              ORDER BY x.pulled_at DESC NULLS LAST, x.impressions DESC NULLS LAST,
                                       x.clicks DESC NULLS LAST, x.platform_conversions DESC NULLS LAST,
                                       x.spend DESC NULLS LAST, x.ad_platform DESC NULLS LAST) AS rn
    FROM {silver}.silver_campaign_snapshots x
    LEFT JOIN days d ON d.report_date = x.report_date
    WHERE x.campaign_id IS NOT NULL AND x.report_date IS NOT NULL
      AND (x.report_date BETWEEN DATE'{from_date}' AND DATE'{to_date}' OR d.report_date IS NOT NULL)
  ),
  ad AS (
    SELECT campaign_id, report_date, ad_platform, impressions, clicks, platform_conversions, spend
    FROM snap WHERE rn = 1
  ),
  od AS (
    SELECT a.campaign_id, to_date(CAST(f.date_key AS STRING), 'yyyyMMdd') AS report_date,
           CAST(COUNT(1) AS BIGINT) AS orders_placed,
           CAST(SUM(CASE WHEN COALESCE(f.is_delivered, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS orders_delivered,
           CAST(SUM(CASE WHEN COALESCE(f.is_cancelled, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS orders_cancelled,
           CAST(SUM(CASE WHEN COALESCE(f.is_returned, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS orders_returned,
           CAST(SUM(CASE WHEN COALESCE(f.has_failed_attempt, FALSE) THEN 1 ELSE 0 END) AS BIGINT)
             AS orders_with_failed_attempt,
           CAST(COALESCE(SUM(f.order_total), 0) AS DECIMAL(12,2)) AS revenue_placed,
           CAST(COALESCE(SUM(CASE WHEN COALESCE(f.is_delivered, FALSE) THEN f.order_total END), 0) AS DECIMAL(12,2))
             AS revenue_delivered,
           CAST(SUM(CASE WHEN COALESCE(f.is_first_order, FALSE) THEN 1 ELSE 0 END) AS BIGINT) AS new_customer_orders,
           CAST(SUM(CASE WHEN NOT COALESCE(f.is_delivered, FALSE) AND NOT COALESCE(f.is_cancelled, FALSE)
                          AND NOT COALESCE(f.is_returned, FALSE)
                          AND f.order_created_at < current_timestamp() - INTERVAL 14 DAYS
                         THEN 1 ELSE 0 END) AS BIGINT) AS orders_open
    FROM {gold}.fact_orders f
    JOIN {gold}.fact_attribution a ON a.order_id = f.order_id AND a.model = 'last_touch'
    LEFT JOIN days d ON d.report_date = to_date(CAST(f.date_key AS STRING), 'yyyyMMdd')
    WHERE a.campaign_id IS NOT NULL AND a.campaign_id <> 'unknown' AND f.date_key IS NOT NULL
      AND (to_date(CAST(f.date_key AS STRING), 'yyyyMMdd') BETWEEN DATE'{from_date}' AND DATE'{to_date}'
           OR d.report_date IS NOT NULL)
    GROUP BY a.campaign_id, to_date(CAST(f.date_key AS STRING), 'yyyyMMdd')
  ),
  m AS (
    SELECT COALESCE(ad.campaign_id, od.campaign_id) AS campaign_id,
           COALESCE(ad.report_date, od.report_date) AS report_date,
           ad.ad_platform, ad.impressions, ad.clicks, ad.platform_conversions, ad.spend,
           COALESCE(od.orders_placed, CAST(0 AS BIGINT)) AS orders_placed,
           COALESCE(od.orders_delivered, CAST(0 AS BIGINT)) AS orders_delivered,
           COALESCE(od.orders_cancelled, CAST(0 AS BIGINT)) AS orders_cancelled,
           COALESCE(od.orders_returned, CAST(0 AS BIGINT)) AS orders_returned,
           COALESCE(od.orders_with_failed_attempt, CAST(0 AS BIGINT)) AS orders_with_failed_attempt,
           COALESCE(od.revenue_placed, CAST(0 AS DECIMAL(12,2))) AS revenue_placed,
           COALESCE(od.revenue_delivered, CAST(0 AS DECIMAL(12,2))) AS revenue_delivered,
           COALESCE(od.new_customer_orders, CAST(0 AS BIGINT)) AS new_customer_orders,
           COALESCE(od.orders_open, CAST(0 AS BIGINT)) AS orders_open
    FROM ad FULL OUTER JOIN od ON ad.campaign_id = od.campaign_id AND ad.report_date = od.report_date
  )
  SELECT campaign_id, report_date, 'last_touch' AS attribution_model, ad_platform, impressions, clicks,
         platform_conversions, spend, orders_placed, orders_delivered, orders_cancelled, orders_returned,
         orders_with_failed_attempt, revenue_placed, revenue_delivered,
         CAST(CASE WHEN orders_placed > 0 THEN CAST(orders_cancelled AS DOUBLE) / orders_placed END
              AS DECIMAL(8,4)) AS cancel_rate,
         CAST(CASE WHEN orders_placed > 0 THEN CAST(orders_with_failed_attempt AS DOUBLE) / orders_placed END
              AS DECIMAL(8,4)) AS failed_attempt_rate,
         CAST(CASE WHEN orders_placed > 0 THEN CAST(orders_returned AS DOUBLE) / orders_placed END
              AS DECIMAL(8,4)) AS return_rate,
         CAST(CASE WHEN orders_placed > 0 THEN CAST(orders_open AS DOUBLE) / orders_placed END
              AS DECIMAL(8,4)) AS open_rate,
         new_customer_orders,
         CAST(CASE WHEN orders_placed > 0 THEN CAST(revenue_placed AS DOUBLE) / orders_placed END
              AS DECIMAL(12,2)) AS aov,
         CAST(CASE WHEN impressions > 0 THEN CAST(clicks AS DOUBLE) / impressions END AS DECIMAL(8,4)) AS ctr,
         CAST(CASE WHEN clicks > 0 THEN CAST(orders_placed AS DOUBLE) / clicks END AS DECIMAL(8,4))
           AS conversion_rate,
         CAST(CASE WHEN spend > 0 AND orders_placed > 0 THEN CAST(spend AS DOUBLE) / orders_placed END
              AS DECIMAL(12,2)) AS cpa,
         CAST(CASE WHEN spend > 0 AND orders_delivered > 0 THEN CAST(spend AS DOUBLE) / orders_delivered END
              AS DECIMAL(12,2)) AS cost_per_delivered_order,
         CAST(CASE WHEN spend > 0 THEN CAST(revenue_placed AS DOUBLE) / CAST(spend AS DOUBLE) END
              AS DECIMAL(12,2)) AS roas_placed,
         CAST(CASE WHEN spend > 0 THEN CAST(revenue_delivered AS DOUBLE) / CAST(spend AS DOUBLE) END
              AS DECIMAL(12,2)) AS roas_delivered
  FROM m
) AS s
ON t.campaign_id = s.campaign_id AND t.report_date = s.report_date AND t.attribution_model = s.attribution_model
WHEN MATCHED THEN UPDATE SET
  t.ad_platform = s.ad_platform, t.impressions = s.impressions, t.clicks = s.clicks,
  t.platform_conversions = s.platform_conversions, t.spend = s.spend, t.orders_placed = s.orders_placed,
  t.orders_delivered = s.orders_delivered, t.orders_cancelled = s.orders_cancelled,
  t.orders_returned = s.orders_returned, t.orders_with_failed_attempt = s.orders_with_failed_attempt,
  t.revenue_placed = s.revenue_placed, t.revenue_delivered = s.revenue_delivered, t.cancel_rate = s.cancel_rate,
  t.failed_attempt_rate = s.failed_attempt_rate, t.return_rate = s.return_rate, t.open_rate = s.open_rate,
  t.new_customer_orders = s.new_customer_orders, t.aov = s.aov, t.ctr = s.ctr,
  t.conversion_rate = s.conversion_rate, t.cpa = s.cpa, t.cost_per_delivered_order = s.cost_per_delivered_order,
  t.roas_placed = s.roas_placed, t.roas_delivered = s.roas_delivered
WHEN NOT MATCHED THEN INSERT (
  campaign_id, report_date, attribution_model, ad_platform, impressions, clicks, platform_conversions, spend,
  orders_placed, orders_delivered, orders_cancelled, orders_returned, orders_with_failed_attempt, revenue_placed,
  revenue_delivered, cancel_rate, failed_attempt_rate, return_rate, open_rate, new_customer_orders, aov, ctr,
  conversion_rate, cpa, cost_per_delivered_order, roas_placed, roas_delivered)
VALUES (
  s.campaign_id, s.report_date, s.attribution_model, s.ad_platform, s.impressions, s.clicks,
  s.platform_conversions, s.spend, s.orders_placed, s.orders_delivered, s.orders_cancelled, s.orders_returned,
  s.orders_with_failed_attempt, s.revenue_placed, s.revenue_delivered, s.cancel_rate, s.failed_attempt_rate,
  s.return_rate, s.open_rate, s.new_customer_orders, s.aov, s.ctr, s.conversion_rate, s.cpa,
  s.cost_per_delivered_order, s.roas_placed, s.roas_delivered)
WHEN NOT MATCHED BY SOURCE AND t.report_date BETWEEN DATE'{from_date}' AND DATE'{to_date}' THEN DELETE;
