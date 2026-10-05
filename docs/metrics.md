# FunnelStream Metric Dictionary

Source of truth for every KPI shown in Power BI. Each formula names its table, numerator, denominator and filter. Table names follow spec section 17 (rule 13): base facts `fact_`, summaries `agg_`, outputs `rec_product` and `customer_360`.

## Attribution model

- `agg_campaign_daily` uses the `last_touch` model and says so in its `attribution_model` column. `fact_attribution` also holds `first_touch` rows, keyed by `order_id` and `model`.
- Last touch is resolved in two steps. The order's own session comes first (the `click_id` on `order_created`). If that session has no click, the order falls back to the customer's last click in the 7 days before the order (identified customers only).
- An order with no click in either step is direct: one row with NULL `campaign_id` and `click_id`.
- Linking an anonymous pre-login session to a customer after login is out of scope. Those orders can show as direct. This is a known limitation.
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

- Bot sessions (`is_bot`) and quarantined rows are excluded from funnel, attribution and spend.
- `silver_campaign_snapshots` is used after quarantine (negative spend and clicks above impressions removed).
- "Placed" means every attributed order regardless of status.
- Dates are Cairo-local for orders and events; the ad platform's `report_date` is a UTC day.
- A fact row with no matching dimension version carries key -1 (the unknown member) and is still counted; the daily build repairs the key once the dimension row exists.
