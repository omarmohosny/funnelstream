# FunnelStream Metric Dictionary

Source of truth for every KPI shown in Power BI. Each formula names its table, numerator, denominator and filter.

## Campaign metrics (`fact_campaign_daily`)

| Metric | Formula | Table and columns | Filter | Notes |
|---|---|---|---|---|
| Spend | Sum of `spend` for the latest `pulled_at` per campaign per day | `silver_campaign_snapshots` | None | Earlier pulls are not summed |
| ROAS (delivered) | `revenue_delivered` / `spend` | `fact_campaign_daily` | Attributed orders with `is_delivered` | Headline metric. Lags spend by about 2–4 days |
| ROAS (placed) | `revenue_placed` / `spend` | `fact_campaign_daily` | Attributed orders, all statuses | Secondary. Overstates when COD orders fail |
| CPA | `spend` / `orders_placed` | `fact_campaign_daily` | Placed orders | Per campaign, per day |
| CTR | `clicks` / `impressions` | `fact_campaign_daily` | API-reported clicks | Not the same clicks as attribution |
| Conversion rate | `orders_placed` / `clicks` | `fact_campaign_daily` | Session clicks (see click definition) | Labelled with the click source |
| Cancel rate | `orders_cancelled` / `orders_placed` | `fact_campaign_daily` | Placed orders | |
| Failed-delivery rate | `orders_failed_delivery` / `orders_placed` | `fact_campaign_daily` | Orders with at least one failed attempt | Main COD cost |
| AOV | `revenue_placed` / `orders_placed` | `fact_campaign_daily` | Placed orders | EGP |
| New-customer orders | Count of attributed orders with `is_first_order` | `fact_campaign_daily` | Attributed orders | Uses `order_seq` |
| Direct share | Direct orders / all orders | `fact_attribution` | `model = last_touch` | Direct = NULL `campaign_id` |
| Contribution (stretch) | Sum of `contribution_egp` | `fact_campaign_daily` | Delivered orders | Uses `cfg_cost_assumptions`. Assumption-based |

## Funnel metrics (`fact_funnel_daily`)

| Metric | Formula | Filter |
|---|---|---|
| Sessions | Distinct `session_id` | Per day, country, channel, payment method |
| Cart abandon rate | 1 − `orders` / `cart_sessions` | Sessions with `add_to_cart` |
| Checkout abandon rate | 1 − `orders` / `checkout_sessions` | Sessions with `checkout_started` |
| Conversion rate by channel | `orders` / `sessions` | Per channel |

A session that spans midnight counts once on each Cairo `event_date`, so daily sessions do not sum across dates. Funnel `orders` counts sessions with a purchase event and can differ from the count of `silver_orders` rows when a purchase event is missing or a session placed two orders.

## Delivery metrics (`fact_orders`)

| Metric | Formula | Filter |
|---|---|---|
| Delivery lead time | Average `delivery_lead_hours` | `is_delivered` |
| First-attempt delivery rate | Count of `first_attempt_delivered` / shipped orders | Orders with `order_shipped` |
| Cancel rate by carrier and city | Cancelled / placed, grouped by `carrier` and `city` | Placed orders |

## Recommendation metrics (`fact_product_recs`)

| Metric | Definition |
|---|---|
| Co-view count | Customers who viewed both `anchor_product_id` and `rec_product_id` in the 30-day window |
| Segment fallback | City, then country, then global, when a city has fewer than 20 co-viewing customers |
| Eligibility | `is_active` products only |

Recommendations are labelled "co-view", not "purchase".

## Status precedence

Current order status is the status with the highest `status_rank` (see spec section 6). `order_cancelled` is terminal and valid only before `order_shipped`.


## Filters that apply to every metric above

- Bot sessions (`is_bot`) and quarantined rows are excluded from funnel, attribution and spend.
- `silver_campaign_snapshots` is used after quarantine (negative spend and clicks above impressions removed).
- "Placed" means every attributed order regardless of status.
- Dates are Cairo-local for orders and events; the ad platform's `report_date` is a UTC day.
