-- agg_funnel_daily: same semantics as streaming/gold/funnel.py (spec 17.9), recomputed, never incremented.
-- Keys event_date, country, channel, platform. Distinct sessions per day; a session takes country, channel and
-- platform from its EARLIEST event of that date (ties by event_id); NULL becomes unknown. Late rows are
-- INCLUDED (spec 16.1). Rows without session_id are excluded (anonymous events are keyed by session, so every
-- event with a session_id counts). Abandon rates are NULL on a zero denominator, clamped to 0..1.
--
-- Bots (spec 16.5, same rule as streaming/gold/bots.py): is_bot is recomputed over the WHOLE silver_events rows of
-- every session that appears in the dates below, not taken from the stored is_bot column: a session is a bot when
-- some event has more than 100 events of the same session in the 10 seconds up to it. This is the CTE bots
-- below; nothing is persisted.
--
-- (WHEN clause order is fixed by the grammar: MATCHED, NOT MATCHED, NOT MATCHED BY SOURCE.)
-- Window handling, ONE atomic MERGE: the source holds the recomputed rows for these dates: the window, plus the
-- dates of every event of a session that has an event in the window (a session that crossed midnight can
-- become a bot), plus the dates of late events (event_date before from_date but kafka_timestamp, Cairo date,
-- inside the window). Matched keys get all measures updated, new keys are inserted, and WHEN NOT MATCHED BY
-- SOURCE deletes a target row whose event_date is inside the window and that no longer has sessions.
-- A failure rolls the statement back, the window is never left empty. Limit: a key that loses all its sessions
-- on a widened date OUTSIDE the window is not deleted (a MERGE condition cannot hold a subquery). In history
-- mode (a replay, kafka_timestamp is the replay time) every replayed date counts as late and is recomputed.
MERGE INTO {gold}.agg_funnel_daily AS t
USING (
  WITH win_sessions AS (
    SELECT DISTINCT e.session_id
    FROM {silver}.silver_events e
    WHERE e.session_id IS NOT NULL AND e.event_date BETWEEN DATE'{from_date}' AND DATE'{to_date}'
  ),
  dates AS (
    SELECT DISTINCT e.event_date
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
  ),
  ev AS (
    SELECT e.event_date, e.session_id, e.event_time, e.event_id, e.event_type, e.country, e.channel, e.platform
    FROM {silver}.silver_events e JOIN dates d ON d.event_date = e.event_date
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
  sess AS (
    SELECT e.event_date, e.session_id,
           MIN_BY(e.country, STRUCT(e.event_time, e.event_id)) AS country,
           MIN_BY(e.channel, STRUCT(e.event_time, e.event_id)) AS channel,
           MIN_BY(e.platform, STRUCT(e.event_time, e.event_id)) AS platform,
           MAX(CAST(e.event_type = 'product_view' AS INT)) AS pv,
           MAX(CAST(e.event_type = 'add_to_cart' AS INT)) AS cart,
           MAX(CAST(e.event_type = 'checkout_started' AS INT)) AS checkout,
           MAX(CAST(e.event_type = 'purchase' AS INT)) AS purchase
    FROM ev e LEFT ANTI JOIN bots b ON b.session_id = e.session_id
    GROUP BY e.event_date, e.session_id
  ),
  g AS (
    SELECT event_date,
           COALESCE(country, 'unknown') AS country,
           COALESCE(channel, 'unknown') AS channel,
           COALESCE(platform, 'unknown') AS platform,
           CAST(COUNT(1) AS BIGINT) AS sessions,
           CAST(SUM(pv) AS BIGINT) AS product_views,
           CAST(SUM(cart) AS BIGINT) AS cart_sessions,
           CAST(SUM(checkout) AS BIGINT) AS checkout_sessions,
           CAST(SUM(purchase) AS BIGINT) AS orders
    FROM sess
    GROUP BY event_date, COALESCE(country, 'unknown'), COALESCE(channel, 'unknown'),
             COALESCE(platform, 'unknown')
  )
  SELECT event_date, country, channel, platform, sessions, product_views, cart_sessions, checkout_sessions,
         orders,
         CAST(CASE WHEN cart_sessions > 0
                   THEN GREATEST(0.0D, LEAST(1.0D, 1 - orders / cart_sessions)) END AS DECIMAL(8,4))
           AS cart_abandon_rate,
         CAST(CASE WHEN checkout_sessions > 0
                   THEN GREATEST(0.0D, LEAST(1.0D, 1 - orders / checkout_sessions)) END AS DECIMAL(8,4))
           AS checkout_abandon_rate
  FROM g
) AS s
ON t.event_date = s.event_date AND t.country = s.country AND t.channel = s.channel AND t.platform = s.platform
WHEN MATCHED THEN UPDATE SET
  t.sessions = s.sessions, t.product_views = s.product_views, t.cart_sessions = s.cart_sessions,
  t.checkout_sessions = s.checkout_sessions, t.orders = s.orders, t.cart_abandon_rate = s.cart_abandon_rate,
  t.checkout_abandon_rate = s.checkout_abandon_rate
WHEN NOT MATCHED THEN INSERT (
  event_date, country, channel, platform, sessions, product_views, cart_sessions, checkout_sessions, orders,
  cart_abandon_rate, checkout_abandon_rate)
VALUES (
  s.event_date, s.country, s.channel, s.platform, s.sessions, s.product_views, s.cart_sessions,
  s.checkout_sessions, s.orders, s.cart_abandon_rate, s.checkout_abandon_rate)
WHEN NOT MATCHED BY SOURCE AND t.event_date BETWEEN DATE'{from_date}' AND DATE'{to_date}' THEN DELETE;
