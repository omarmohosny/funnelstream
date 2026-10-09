# FunnelStream Metric Dictionary

Source of truth for every KPI shown in Power BI. Each formula names its table, numerator, denominator and filter. Table names follow spec section 17 (rule 13): base facts `fact_`, summaries `agg_`, outputs `rec_product` and `customer_360`.

## Attribution model

- `agg_campaign_daily` uses the `last_touch` model and says so in its `attribution_model` column. `fact_attribution` also holds `first_touch` rows, keyed by `order_id` and `model`.
- Click: the first `page_view` or `product_view` event of a session that carries a `utm_campaign` (parsed from `page_url` in Silver). Bot sessions (`is_bot` recomputed over all events of the session, same rule as the funnel) have no click, and quarantined rows are not in `silver_events`, so they never make one. The click is attributed to `silver_events.campaign_id` when it is set, else to the `dim_campaign` row with the same `utm_campaign` (the unknown member never matches). A click whose UTM campaign cannot be resolved has no campaign and does not count as a click for attribution; the build reports their number (`unresolved_clicks`).
- Last touch is resolved in order: (1) the order's own session, the click whose `click_id` equals the `click_id` on `order_created` (no time condition); (2) if there is none and the order has a `customer_id`, the customer's most recent click from ANY of their sessions in the window `order_created_at - 7 days` (inclusive) to `order_created_at` (exclusive), so a click 6 days 23 hours before counts, one 7 days 1 hour before does not, and a click after the order is ignored; (3) otherwise direct.
- First touch uses the same steps; only step 2 differs: the customer's EARLIEST click in the same window. When the order's own session has a click, both models give that click.
- An order with no click in either step is direct: one row per model with NULL `campaign_id` and `click_id`. Anonymous orders (NULL `customer_id`) never use step 2; they are attributed only through their own session click.
- Ties: two clicks with the same click time go to the smallest `click_id` (NULL last), then the smallest `session_id`, so a rebuild always gives the same row. Output grain: exactly two rows per order, key `(order_id, model)`, `order_total` copied from the order. `fact_orders.campaign_id` is the `last_touch` campaign (NULL for direct orders).
- A click belongs to the `customer_id` carried by its own event. Linking an anonymous pre-login session to a customer after login is out of scope. Those orders can show as direct. This is a known limitation.
- Delivered revenue is counted on the ORDER date (cohort view). An order placed on day D that is delivered on day D+3 adds its revenue to day D, so recent days are incomplete until their orders settle.

## Order outcome metrics (`agg_campaign_daily`, `agg_orders_daily`)

| Metric | Formula | Table and columns | Filter | Notes |
|---|---|---|---|---|
| Spend | Sum of `spend` for the latest `pulled_at` per campaign per day | `silver_campaign_snapshots` | None | Earlier pulls are not summed |
| ROAS (delivered) | `revenue_delivered` / `spend` | `agg_campaign_daily` | Attributed orders with `is_delivered` | Headline metric. Lags spend by about 2 to 4 days |
| ROAS (placed) | `revenue_placed` / `spend` | `agg_campaign_daily` | Attributed orders, all statuses | Secondary. Overstates when COD orders fail |
| CPA | `spend` / `orders_placed` | `agg_campaign_daily` | Placed orders | Per campaign, per day |
| Cost per delivered order | `spend` / `orders_delivered` | `agg_campaign_daily` | Delivered orders | Sits next to CPA. Counted on the order date |
| CTR | `clicks` / `impressions` | `agg_campaign_daily` | API-reported clicks | Not the same clicks as attribution |
| Conversion rate | `orders_placed` / `clicks` | `agg_campaign_daily` | Session clicks (see click definition) | Labelled with the click source |
| Platform conversions | `platform_conversions` as reported by Meta or Google | `agg_campaign_daily` | API-reported | Shown beside `orders_placed` (attributed by us). The two differ by design |
| Cancel rate | `orders_cancelled` / `orders_placed` | `agg_campaign_daily` | Placed orders | A cancel counts only before shipment (spec 17.6) |
| Failed-attempt rate | `orders_with_failed_attempt` / `orders_placed` | `agg_campaign_daily` | Orders with at least one `delivery_failed` | Main COD cost. Replaces the retired failed-delivery rate |
| Return rate | `orders_returned` / `orders_placed` | `agg_campaign_daily` | Orders whose final status is `order_returned` | Failed attempts that never succeeded (spec 17.5) |
| Open rate | Orders not delivered, cancelled or returned after 14 days / `orders_placed` | `agg_campaign_daily` | Orders older than 14 days | Stuck orders. Defined in spec 17.8 |
| AOV | `revenue_placed` / `orders_placed` | `agg_campaign_daily` | Placed orders | EGP |
| New-customer orders | Count of attributed orders with `is_first_order` | `agg_campaign_daily`, `agg_orders_daily` | Attributed orders | Uses `order_seq` |
| Direct share | Orders with `channel = direct` / all orders | `agg_orders_daily` | Group by `date_key` | Computed here, not in `agg_campaign_daily` |
| Contribution (stretch) | Sum of `contribution_egp` | `agg_campaign_daily` | Delivered orders | Uses `cfg_cost_assumptions`. Assumption-based |

`agg_orders_daily` (keys `date_key`, `channel`, `payment_method`, `platform`) holds the order counts and revenue behind these ratios for every order, attributed or not: `orders_placed`, `orders_delivered`, `orders_cancelled`, `orders_returned`, `orders_with_failed_attempt`, `revenue_placed`, `revenue_delivered`, `new_customer_orders`. COD versus prepaid lives here, not in the funnel. Delivered, cancelled, returned and still-open orders add up to placed. The identity delivered + cancelled + returned + open = placed holds for the boolean flags; `current_status` can differ in the rare inconsistent case where a counted cancel and a delivered row both exist (the order is then `order_cancelled` with `is_delivered` true), and `report_cancel_then_shipped(...).delivered_after_cancel` counts those orders.

### `agg_campaign_daily` as built

- **Grain.** `(campaign_id, report_date, attribution_model)`; `attribution_model` is always `last_touch`. `report_date` is a Cairo calendar date. One row exists for every campaign-day that has ad data or attributed orders. Direct orders (no last-touch campaign) and the `unknown` campaign are not in the table; direct share lives in `agg_orders_daily`.
- **Ad data source and measures.** `ad_platform`, `impressions`, `clicks`, `platform_conversions` and `spend` come from `silver_campaign_snapshots`, from the single row with the latest `pulled_at` per `(campaign_id, report_date)`; earlier pulls are not summed. If two rows share the latest `pulled_at`, the one with the greatest `impressions` wins, then `clicks`, `platform_conversions`, `spend` and `ad_platform`. A row without `pulled_at` loses to any row that has one. Rows with a NULL `campaign_id` or `report_date` are ignored. A day with no ad row has NULL in all five columns (not 0), so "no ad data" differs from zero spend.
- **Order measures.** Orders of `fact_orders` joined to `fact_attribution` rows with `model = 'last_touch'` and a campaign that is not NULL and not `unknown`, counted on the Cairo date the order was placed (`date_key`), never on the delivery date. `orders_placed` counts the orders; `orders_delivered`, `orders_cancelled`, `orders_returned` and `orders_with_failed_attempt` count the flags `is_delivered`, `is_cancelled`, `is_returned` and `has_failed_attempt` (NULL counts as false); `revenue_placed` sums `order_total` and `revenue_delivered` sums it over delivered orders; `new_customer_orders` counts `is_first_order`. A day with ad data and no orders has 0 for the counts and 0.00 for the revenues.
- **Rates and ratios.** Rates (`cancel_rate`, `failed_attempt_rate`, `return_rate`, `open_rate`, `ctr`, `conversion_rate`) are `DECIMAL(8,4)`, rounded half up to 4 decimals from a double division. Money ratios (`aov`, `cpa`, `cost_per_delivered_order`, `roas_placed`, `roas_delivered`) are `DECIMAL(12,2)`, rounded half up to 2 decimals. A ratio is NULL when its denominator is NULL or 0: `cancel_rate`, `failed_attempt_rate`, `return_rate`, `open_rate`, `aov` use `orders_placed`; `ctr` uses `impressions`; `conversion_rate` uses `clicks` (API-reported clicks against orders attributed by us, so the two differ by design); `cpa` uses `orders_placed`, `cost_per_delivered_order` uses `orders_delivered`; `cpa` and `cost_per_delivered_order` are also NULL when `spend` is NULL or 0; `roas_placed` and `roas_delivered` use `spend` (NULL when it is NULL or 0) and are 0.00, not NULL, when spend is above 0 and the day has no (delivered) revenue.
- **Open rate.** Orders that are not delivered, cancelled or returned and were placed more than 14 days before the build time (`current_timestamp()` in SQL), divided by `orders_placed`. The value therefore changes with the wall clock even when no data changes; it is refreshed only when the row is recomputed by a window.
- **Window.** The build recomputes every campaign-day whose `report_date` is in the window and the order days of every order the `fact_orders` step recomputed, in one atomic MERGE, and deletes a campaign-day inside the window that has no ad data and no orders any more. A campaign-day that vanishes on a day outside the window is not deleted. A late snapshot pull for an old report day is picked up only when that day is in the window.

## Funnel metrics (`agg_funnel_daily`)

Keys: `event_date`, `country`, `channel`, `platform` (`app` or `web`). There is no `payment_method`: it is not known before checkout.

| Metric | Formula | Filter |
|---|---|---|
| Sessions | Distinct `session_id` | Per day, country, channel, platform |
| Cart abandon rate | 1 - `orders` / `cart_sessions` | Sessions with `add_to_cart` |
| Checkout abandon rate | 1 - `orders` / `checkout_sessions` | Sessions with `checkout_started` |
| Conversion rate by channel | `orders` / `sessions` | Per channel |

A session's country, channel and platform come from its earliest event of that `event_date` (ties by `event_id`). A NULL platform (events from before the field existed) shows as `unknown`. The platform split adds up to total sessions.

A session that spans midnight counts once on each Cairo `event_date`, so daily sessions do not sum across dates. Funnel `orders` counts distinct sessions with a purchase event and can differ from the count of `silver_orders` rows when a purchase event is missing or a session placed two orders.

## Delivery metrics (`fact_orders`)

| Metric | Formula | Filter |
|---|---|---|
| Delivery lead time | Average `delivery_lead_hours` | `is_delivered` |
| First-attempt delivery rate | Count of `first_attempt_delivered` / shipped orders | Orders with `order_shipped` |
| Cancel rate by carrier and city | Cancelled / placed, grouped by `carrier` and `city` | Placed orders |
| Return rate by carrier and city | `is_returned` / placed, grouped by `carrier` and `city` | Placed orders |

## Recommendation metrics (`rec_product`)

| Metric | Definition |
|---|---|
| Co-view sessions | Distinct sessions that viewed both `anchor_product_id` and `rec_product_id` in the 30-day window (`co_view_sessions`) |
| Segment fallback | City, then country, then global, when a city has fewer than 20 distinct sessions that viewed the anchor and the recommended product |
| Eligibility | `is_active` products only |

Recommendations are labelled "co-view", not "purchase". The threshold counts distinct sessions, not customers.

## Status precedence

Current order status is the status with the highest `status_rank` (see spec section 6). A tie at rank 5 (`out_for_delivery`, `delivery_failed`) or rank 7 (`order_completed`, `order_returned`) goes to the latest `status_time`. `order_cancelled` counts only if no `order_shipped` row is earlier than the cancel by `status_time`; a counted cancel is terminal and wins. Arrival order never matters.

## Filters that apply to every metric above

- Bot sessions (`is_bot`) and quarantined rows are excluded from funnel, attribution and spend (attribution recomputes the bot rule itself, like the funnel).
- `silver_campaign_snapshots` is used after quarantine (negative spend and clicks above impressions removed).
- "Placed" means every attributed order regardless of status.
- Dates are Cairo-local for orders and events; the ad platform's `report_date` is a UTC day.
- A fact row with no matching dimension version carries key -1 (the unknown member) and is still counted; the daily build repairs the key once the dimension row exists.

## Pipeline health (Postgres, written by the Silver job)

`pipeline_health_minute` has one row per UTC minute. The Silver job recomputes the whole row for every minute a batch touches, from the Silver tables, so the numbers are totals, not increments (spec 17.3).

| Column | Definition |
|---|---|
| `events` | Silver events whose Kafka arrival (`kafka_timestamp`) falls in the minute |
| `late_events` | of those, rows with `is_late = true` (arrival more than 600 seconds after the event time) |
| `quarantined` | `silver_quarantine` rows whose `received_at` falls in the minute |
| `silver_batches` | Silver micro-batches that finished in the minute (rows of `pipeline_batch_log`) |

- **Late-event rate** = `late_events` / `events` over the window (sum both over the window first; a window with `events` = 0 has no rate).
- **Quarantine share** = `quarantined` / (`events` + `quarantined`) over the window: the share of messages that did not become Silver events. Quarantine rows of the orders and delivery topics count in the numerator and not in `events`, so it is a rough share, not an exact one.
- **Seconds since the Silver job last reported / since the last Gold update** = now minus `last_seen_at` of the `silver_job` / `gold_build` rows of `pipeline_heartbeat`. The Silver job reports twice over: `batch N` after every micro-batch and `alive` every 15 seconds from a timer thread while the Silver query is active, so the age stays under about 15 seconds on a healthy but idle job (Spark runs no batch when the source is quiet) and grows only when the job is stopped, hung or cannot reach Postgres. Panel thresholds: amber at 30 s, red at 120 s.
- History-mode data is all late by rule, so the late-event rate is meaningful in `--live` runs only.

`live_metrics_minute` (events, active sessions, product views, add to cart, orders placed per minute and country) uses the rules of `streaming/gold/live.py`: late events and bot sessions are excluded, minutes are UTC, `orders_placed` counts `purchase` events (the simulator emits one per order). It can differ from `silver_orders` for the same minute when a purchase event is missing or a session places two orders.

## Gold build rules

How `warehouse/gold_build.py` produces the tables above (details in `docs/RUNNING.md`, "Gold build").

- **Window.** A run recomputes what a window of Cairo days selects (default: the last 7 days; `--from-date/--to-date`, `--days`, `--full`). Orders are recomputed when their order date or any of their status rows falls in the window, plus later orders of the same customers (`order_seq`, `is_first_order`). Nothing is incremented: every measure is recalculated from Silver or from `fact_orders`.
- **One atomic MERGE.** `agg_orders_daily`, `agg_funnel_daily` and `agg_campaign_daily` each use a single MERGE: recomputed rows update matching keys and insert new ones, and `WHEN NOT MATCHED BY SOURCE` deletes a key inside the window that has no rows left. A failure rolls the statement back, so the window is never left empty. A key that vanishes on a widened date outside the window (only when an order or session changes group there) is not deleted.
- **Attribution.** `fact_attribution` is one atomic MERGE over the orders of the `fact_orders` window, the orders without an attribution row and the orders whose click inputs changed (a click candidate of a session with an event in the window or a late event); an order that left `fact_orders` is deleted by the same MERGE. A second statement updates `fact_orders.campaign_id` (changed values only). A click row that arrives late for an old session (`event_date` and `kafka_timestamp` both before the window) is found only by a covering window or `--full`.
- **Time zone.** Every build step starts with `SET TIME ZONE 'UTC'`; Cairo dates use `from_utc_timestamp(ts, 'Africa/Cairo')` and are only right in a UTC session.
- **Late data.** Limit: `silver_order_status` has no ingestion time, so a late status row is only found when its `event_date` (Cairo date of `status_time`) is in the window; a row older than the window (default 7 days) leaves the order, its lines and its `agg_orders_daily` day stale until a covering window or `--full` (run `--full` periodically or use a wider `--days`); the funnel is not affected. A late status row inside the window changes that order and the day it was placed (cohort view: delivered revenue stays on the order date); `agg_funnel_daily` also recomputes dates of late events (`kafka_timestamp` in the window, `event_date` before it) and of sessions that cross the window edge. In history mode (a replay) every replayed day counts as late, so a run recomputes all replayed days.
- **Bots.** `is_bot` is not read from Silver: the funnel step recomputes it from `silver_events` (more than 100 events in 10 seconds in one session) over every event of the sessions it touches and excludes those sessions. Nothing is persisted.
- **Dimensions.** SCD2 `valid_to` of the current row is 9999-12-31 00:00:00 UTC. Several Silver versions on one Cairo day collapse to the last one. Only tracked attributes start a new version (customer: city, district, primary_device; product: category, price); other attributes keep the first value of the run and are not back-filled.
- **`dim_date`.** 2025-01-01 to 2027-12-31. The weekend is Friday and Saturday (`is_friday_saturday` carries the same value as `is_weekend`; both columns are in the table). Ramadan and Eid ranges in `warehouse/calendar_egypt.py` are APPROXIMATE (moon sighting), so a flag can be a day off. `is_holiday` is an Eid day or a fixed holiday (Jan 7, Jan 25, Apr 25, May 1, Jul 23, Oct 6). Days outside the range have no `dim_date` row.
- **Unknown members.** Orders with no matching dimension version get key -1; the key is repaired by the next build that recomputes the order. Orders without `order_created_at` are left out of `fact_orders` and counted in the build output.

## Address and tracking URL

- An address is country > city > district. `city` keeps the seven canonical values; `district` is always a child of its city (source of truth: `contracts/geo.py`, `DISTRICTS`). Cairo has 15 districts, Giza 8, Alexandria 10, Mansoura, Tanta and Assiut 2 each, and `Other` has the single district `Other`.
- A district that is missing, unrecognised or not under the row's city is `unknown` (the row is kept). `Other` is a real value, valid only under city `Other`. An `unknown` city always gives an `unknown` district. Spelling variants (case, spacing, hyphen, aliases such as "Madinet Nasr") map to the canonical name.
- UTM parameters (`utm_source`, `utm_medium`, `utm_campaign`) exist only inside the `page_url` of landing (`page_view` home) and product-view events of campaign sessions. Sessions without a campaign carry a plain URL with no query string. Order events keep `utm_source`, `utm_campaign` and `utm_medium` as order attribution fields.
- Silver rules: `district` is normalised with the row's normalised city (`normalize_district(city, value)`) on app events, `order_created` rows and `silver_customers`. `utm_source`, `utm_medium` and `utm_campaign` of an app event are parsed from `page_url` (lower-cased, trimmed, empty is NULL); no query string, a malformed URL or a missing UTM key gives NULL. Only when the event has no `page_url` (older Bronze data) the payload `utm_source` and `utm_campaign` are used and `utm_medium` is NULL. Order rows take the three fields from the order payload. `silver_events.page_url` keeps the URL as sent.
- `unknown` and `Other` are different: `Other` is a real city (and its only district) from the reference; `unknown` means the value could not be placed (missing, unrecognised, wrong city, or the city itself is `unknown`). Neither case quarantines a row.
- Reported violations: events and orders whose non-empty district is not valid for their city (before normalisation, `contracts.geo.district_violation`) are counted by `count_district_violations` and logged per batch by `silver_clean` as `district_violations`. After normalisation there are none by construction (they become `unknown`). Rows with a missing district are not violations.
- `dim_customer` tracks changes of `city`, `district` and `primary_device` (SCD2); `fact_orders`, `fact_order_lines` and `customer_360` carry `district` beside `city`; `agg_city_product_daily` stays at city grain. The unknown member (key -1) has district `unknown`.
