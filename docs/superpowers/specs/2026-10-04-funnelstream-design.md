# FunnelStream: Design Spec (v3)

Date: 2026-10-04
Revision: v3, after data engineer and e-commerce expert reviews (v2 applied both, v3 closes the remaining gaps)
Defense: 2026-10-13

## 1. Objective

Show which campaigns turn ad spend into delivered orders, where shoppers drop off from product view to delivery, and where cash-on-delivery (COD) losses come from. The platform generates Egyptian-style synthetic data for an online store.

Business questions:
1. Spend, CPA and ROAS per campaign, per day. Headline ROAS uses delivered revenue. Placed-order ROAS is secondary.
2. Conversion rate and cart/checkout abandonment per channel and platform (Meta, Google), with payment method split (COD vs prepaid).
3. Funnel from product view to cart, checkout, order, delivery, with drop-off at each step.
4. Cancellation rate, failed-delivery rate and delivery lead time, per carrier and city.
5. Products browsed and bought per city, and which products to recommend next (labelled as co-view, not purchase).

## 2. Scope

**Required (must ship):**
- Python simulator: app and web events, order and delivery events, customer and product catalog export.
- Mock Meta and Google Ads API with on-demand pulls.
- Kafka topics: `app.events`, `orders`, `delivery.events`.
- Spark Structured Streaming: Bronze, Silver, funnel and order Gold tables.
- Airflow DAG (manual trigger): campaign pulls, catalog/customer batch loads, Gold batch tables, recommendations.
- Delta Lake on MinIO, Medallion layers.
- Postgres serving Gold tables.
- Power BI dashboard, Import mode, refreshed after each DAG run.
- Metric dictionary (`docs/metrics.md`) and SQL data-quality checks.
- Volume: pipeline runs end to end at 1–2 million events.
- Sample CSV exports (10,000 rows per Silver and Gold table).

**Stretch (build last, cut first if late):**
- Customer 360 table and Power BI page.
- 10 million event benchmark run (one-off scale proof).
- Contribution margin after COD and cancellation cost, using an assumptions table.
- Anomaly alert rules.

**Out of scope:**
- Grafana, Prometheus, live operational alerts.
- Schema Registry, Avro, Protobuf.
- Cloud deployment.
- ALS collaborative filtering and real-time recommendations inside the web app.
- Incrementality tests, view-through attribution, CAC payback.
- Real Meta or Google accounts.
- Refunds and partial cancellations.
- Great Expectations and dbt (replaced by SQL checks; optional if time allows).

## 3. Architecture

**Stream path (live):**
Simulator → Kafka (`app.events`, `orders`, `delivery.events`) → Spark `bronze_ingest` → Bronze Delta → Spark `silver_clean` → Silver Delta → Spark writes its Gold tables to Postgres.

**Batch path (manual Airflow trigger):**
- Ad API pull → Bronze campaign snapshots (`pulled_at`) → Silver → Gold → Postgres.
- Catalog and customer export → Bronze → Silver (with `updated_at`) → SCD2 dimensions → Postgres.
- Recommendations and city-product tables computed from Silver.
- Fallback: if Airflow is not stable by 11 Oct, the same steps run as a Python script (`scripts/run_batch.py`). The tables and rules do not change.

**Surrogate keys** are resolved at load time. Spark and Airflow look up `customer_key`, `product_key` and `campaign_key` from the dimension version valid at event time, and store them. Rows with no match go to `silver_quarantine`.

**Single writer per table.** Each Gold table has one owner:
- Spark stream owns: `fact_orders`, `fact_order_lines`, `fact_funnel_daily`.
- Airflow owns: `dim_customer`, `dim_product`, `dim_campaign`, `dim_date`, `fact_attribution`, `fact_campaign_daily`, `fact_product_recs`, `fact_city_product_daily`, `fact_customer_360` (stretch).

**Serving:** Postgres holds Gold only. Power BI reads Postgres in Import mode.

**Storage:** Delta tables on MinIO in three buckets: `bronze`, `silver`, `gold`. Checkpoints on MinIO under `checkpoints/`.

**Processing guarantee:** at-least-once delivery, with idempotent writes. Silver deduplicates on `event_id` and uses Delta `MERGE`. Gold upserts on its primary key. Results are exactly-once in effect, not by a broker switch.

**Compose:** one Compose file with memory limits per service. A second file adds nothing in the MVP.

## 4. Data Sources and Realism

**Producers:**
- App and web events: customer journeys with a funnel and drop-off at each step. Anonymous events have no `customer_id` and are keyed by `session_id`.
- Orders and delivery: created only after checkout. Delivery events follow the status path below.
- Catalog and customers: a daily export from the simulator (CSV), loaded by Airflow, carries `updated_at`. Product price and category changes are generated so SCD2 has history.
- Campaign data: mock API. Numbers are consistent with the simulator's `utm_campaign` values.

**Egyptian realism:**
- Names: Egyptian first and last names. Names and phones are not kept in Gold.
- Cities weighted by population: Cairo, Giza, Alexandria, Mansoura, Tanta, Assiut and others.
- Phones: `+20` followed by `10`, `11`, `12` or `15`, then 8 digits (Bronze and Silver only).
- Currency: EGP.
- Payment methods: cash on delivery (largest share), Fawry, Vodafone Cash, InstaPay, card.
- Carriers: Bosta, Aramex, J&T Express. Carrier failure rates differ by city.
- Catalog: fashion, beauty, electronics, home.
- Seasonality: Ramadan, Eid, Friday and Saturday peaks, evening traffic.
- Time zone: events stored in UTC. `event_date` derived in `Africa/Cairo`.

**Injected noise (seeded, rates configurable, counts written to `noise_manifest.json`):**

| Noise | Rate | Handled by |
|---|---|---|
| Duplicate event (same `event_id` re-sent) | 1% | Silver dedup on `event_id` |
| Malformed JSON or truncated message | 0.3% | Silver parse failure to `silver_quarantine` (`BAD_JSON`) |
| Missing required field (`event_id`, `event_type`, `session_id` or `order_id`) | 0.3% | Validator to quarantine (`MISSING_FIELD`) |
| Unknown event type or unknown `event_version` | 0.2% | Validator to quarantine (`UNKNOWN_EVENT_TYPE`, `BAD_VERSION`) |
| Negative or zero price | 0.2% | Validator to quarantine (`NEGATIVE_PRICE`) |
| Future timestamp (clock skew) | 0.1% | Validator to quarantine (`FUTURE_TIMESTAMP`) |
| Late event (10 to 120 minutes) and very late (days) | 2% and 0.3% | `is_late` flag, excluded from live metrics, fixed by batch |
| Timestamp with a non-UTC offset (`+02:00`) | 1% | Silver normalises to UTC |
| Null `product_id` on a product event | 0.5% | Quarantine (`MISSING_FIELD`) |
| `product_id` not in the catalog | 0.3% | Quarantine (`UNKNOWN_PRODUCT`) |
| Dirty text: `city` or `device` casing, spaces, aliases (`cairo `, `ALEX`) | 3% | Silver standardises with a lookup table |
| Number sent as a string (`"79.9"`) | 0.3% | Silver casts to decimal, quarantines if it fails |
| Order status out of order, or cancel after shipment | 0.5% and 0.3% | Status rank in Gold; the cancel goes to quarantine |
| Bot session (hundreds of events in seconds) | 0.1% of sessions | Silver flags `is_bot`, excluded from funnel and attribution |
| Ad API: duplicate pull, `clicks > impressions`, negative spend, HTTP 503 | small | Latest-pull rule, SQL checks, Airflow retries |
| Catalog and customer export: duplicate rows, null `city` | small | Dedup on key, default `unknown` |

Quality proof for the defense: the SQL check compares the quarantine rate and duplicate count in Silver with `noise_manifest.json`. They must match within a small tolerance.

**Limit:** synthetic data cannot be proven 100% realistic. The team documents these assumptions and checks distributions against them.

**UTM linking:** Ad URLs carry `utm_source` and `utm_campaign`. The first session event carrying UTM parameters defines `click_id`. Orders carry `session_id`, `channel`, `utm_source`, `utm_campaign` and `click_id` on `order_created`.

**Click definition:** a session click is the first `page_view` or `product_view` event in a session that carries `utm_campaign`. Attribution uses this session click. CTR uses the clicks reported by the ad API. The dashboard labels each one, because the two counts differ.

## 5. Kafka Topics

| Topic | Events | Key | Partitions | Retention |
|---|---|---|---|---|
| `app.events` | `app_open`, `page_view`, `product_view`, `product_click`, `product_view_end`, `product_search`, `add_to_cart`, `remove_from_cart`, `checkout_started`, `purchase` | `session_id` | 6 | 3 days |
| `orders` | `order_created`, `payment_authorised`, `order_packed`, `order_shipped`, `order_cancelled`, `order_completed` | `order_id` | 3 | 7 days |
| `delivery.events` | `out_for_delivery`, `delivered`, `delivery_failed` | `order_id` | 3 | 7 days |

Producers use `acks=all` and idempotence. Per-session order is kept for app events. Per-order order is kept for order and delivery events.

**Order payload (`order_created`):** `order_id`, `customer_id`, `session_id`, `channel`, `utm_source`, `utm_campaign`, `click_id`, `payment_method`, `city`, `country`, `items` (product_id, quantity, unit_price, line_total), `order_total`.

## 6. Order Lifecycle and Status Rank

Main path:
`order_created` → `payment_authorised` → `order_packed` → `order_shipped` → `out_for_delivery` → `delivered` → `order_completed`

Branches:
- `delivery_failed` returns to `out_for_delivery` on retry. Each failure increments `delivery_failed_count`.
- `order_cancelled` is valid only before `order_shipped`. A cancel after shipment is rejected to quarantine.

**Status rank** (used to resolve `current_status`, not arrival order):

| Status | Rank |
|---|---|
| `order_created` | 1 |
| `payment_authorised` | 2 |
| `order_packed` | 3 |
| `order_shipped` | 4 |
| `out_for_delivery` | 5 |
| `delivery_failed` | 5 |
| `delivered` | 6 |
| `order_completed` | 7 |
| `order_cancelled` | 2 (valid only before rank 4; terminal) |

`current_status` is the status with the highest rank. A cancelled order keeps `order_cancelled` as terminal.

`payment_method` is set on `order_created`. `carrier` is set on `order_shipped`.

## 7. Medallion Layers

All layers are Delta tables on MinIO.

### Bronze (raw, append only, kept forever)

| Table | Source | Columns |
|---|---|---|
| `bronze_app_events` | `app.events` | `raw_value`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingest_time`, `ingest_date` |
| `bronze_orders` | `orders` | Same as above |
| `bronze_delivery_events` | `delivery.events` | Same as above |
| `bronze_campaign_snapshots` | Ad API | `raw_json`, `pulled_at`, `platform`, `ingest_date` |
| `bronze_customers` | Catalog export | `raw_row`, `file_name`, `ingest_date` |
| `bronze_products` | Catalog export | `raw_row`, `file_name`, `ingest_date` |

Late events stay in Bronze. Nothing is dropped at Bronze.

### Silver (validated, typed, deduplicated)

| Table | Grain | Key | Columns |
|---|---|---|---|
| `silver_events` | One app or web event | `event_id` | `event_type`, `event_time` (UTC), `kafka_timestamp`, `is_late`, `event_version`, `session_id`, `customer_id` (nullable), `device`, `channel`, `country`, `city`, `campaign_id`, `utm_source`, `utm_campaign`, `click_id`, `product_id`, `element`, `dwell_seconds`, `cart_id`, `order_id`, `search_term`, `event_date` (Africa/Cairo) |
| `silver_order_status` | One status change | `event_id` | `order_id`, `customer_id`, `status`, `status_rank`, `status_time`, `payment_method`, `carrier`, `reason`, `event_date` |
| `silver_order_items` | One order line | `order_id`, `line_no` | `product_id`, `quantity`, `unit_price` (EGP), `line_total` (EGP), `currency`, `order_time` |
| `silver_campaign_snapshots` | One campaign per report date per pull | `campaign_id`, `report_date`, `pulled_at` | `utm_campaign`, `platform`, `impressions`, `clicks`, `spend` (EGP), `currency` |
| `silver_customers` | One customer version | `customer_id`, `updated_at` | `country`, `city`, `signup_date`, `primary_device`, `marketing_consent` |
| `silver_products` | One product version | `product_id`, `updated_at` | `name`, `category`, `price` (EGP), `is_active` |
| `silver_quarantine` | One rejected row | none | `raw_payload`, `source_topic`, `error_code`, `error_detail`, `received_at` |

Silver rules:
- Deduplicate on `event_id` (events) or the natural key (other tables). `MERGE` is used on every write.
- `is_late` is true when `kafka_timestamp` minus `event_time` exceeds 10 minutes. Late rows are kept in Silver with `is_late = true`. They are excluded from windowed live metrics and included in Gold through the batch correction.
- A batch of micro-batch rows is deduplicated on the key before `MERGE`, to avoid two rows with the same key in one `MERGE`.
- A cancel after `order_shipped` goes to quarantine.

### Gold (business-ready)

| Table | Owner | Grain | Key | Columns |
|---|---|---|---|---|
| `dim_customer` | Airflow | Customer version (SCD2) | `customer_key` | `customer_id`, `country`, `city` (tracked), `signup_date`, `primary_device` (tracked), `marketing_consent`, `valid_from`, `valid_to`, `is_current` |
| `dim_product` | Airflow | Product version (SCD2) | `product_key` | `product_id`, `name`, `category` (tracked), `price` (tracked), `is_active`, `valid_from`, `valid_to`, `is_current` |
| `dim_campaign` | Airflow | Campaign | `campaign_key` | `campaign_id`, `utm_campaign`, `platform`, `name`, `start_date`, `end_date` |
| `dim_date` | Airflow | Day | `date_key` (yyyymmdd) | `date`, `week`, `month`, `is_weekend`, `is_friday_saturday`, `is_ramadan`, `is_eid`, `is_holiday` |
| `fact_orders` | Spark | Order, current state | `order_id` | `customer_key`, `session_id`, `channel`, `utm_source`, `utm_campaign`, `click_id`, `campaign_id` (nullable), `payment_method`, `carrier`, `city`, `country`, `order_total` (EGP), `current_status`, `status_rank`, `order_created_at`, `delivered_at`, `cancelled_at`, `delivery_lead_hours`, `delivery_failed_count`, `first_attempt_delivered`, `is_cancelled`, `is_delivered`, `order_seq`, `is_first_order`, `date_key` |
| `fact_order_lines` | Spark | Order line | `order_id`, `line_no` | `customer_key`, `product_key`, `date_key`, `city`, `quantity`, `line_total` (EGP) |
| `live_metrics_minute` | Spark | One minute, country | `minute_start`, `country` | `events`, `active_sessions`, `product_views`, `add_to_cart`, `orders_placed`. Computed on 1-minute tumbling windows from non-late Silver events |
| `fact_funnel_daily` | Spark | Day, country, channel, payment method | `event_date`, `country`, `channel`, `payment_method` | `sessions`, `product_views`, `cart_sessions`, `checkout_sessions`, `orders`, `cart_abandon_rate`, `checkout_abandon_rate` |
| `fact_attribution` | Airflow | Order and attribution model | `order_id`, `model` | `campaign_id` (NULL for direct), `click_id` (NULL for direct), `order_total` (EGP), `model` (`last_touch` or `first_touch`) |
| `fact_campaign_daily` | Airflow | Campaign per report date | `campaign_id`, `report_date` | `platform`, `impressions`, `clicks`, `spend` (EGP), `orders_placed`, `orders_delivered`, `revenue_placed` (EGP), `revenue_delivered` (EGP), `orders_cancelled`, `orders_failed_delivery`, `cancel_rate`, `failed_delivery_rate`, `new_customer_orders`, `aov` (EGP), `ctr`, `conversion_rate`, `cpa` (EGP, placed), `roas_placed`, `roas_delivered`, `direct_share` |
| `fact_product_recs` | Airflow | Recommendation | `segment_level`, `segment_value`, `anchor_product_id`, `rank` | `rec_product_id`, `co_view_count`, `window_end` |
| `fact_city_product_daily` | Airflow | City, product, day | `city`, `product_id`, `date_key` | `sessions_viewed`, `add_to_cart`, `units`, `revenue` (EGP) |
| `fact_customer_360` (stretch) | Airflow | Customer, current state | `customer_id` | `customer_key`, `country`, `city`, `total_sessions`, `product_views`, `add_to_cart_count`, `checkouts_count`, `orders_count`, `delivered_count`, `cancelled_count`, `total_spend` (EGP), `avg_order_value` (EGP), `first_order_at`, `last_order_at`, `last_seen_at`, `days_since_last_order`, `top_category`, `preferred_payment_method`, `last_campaign_id`, `is_repeat_buyer` |

**Stretch: contribution margin.** `cfg_cost_assumptions` (Airflow, one row per product category): `margin_pct`, `carrier_fee_egp`, `failed_attempt_cost_egp`, `cod_fee_egp`. Adds `contribution_egp` to `fact_orders` and `fact_campaign_daily`. Labelled as assumption-based on the dashboard.

**Rules:**
- SCD2 applies only to `dim_customer` and `dim_product`. `valid_from` comes from `updated_at` in Silver. Facts join to the version valid at event time (`valid_from` ≤ event time < `valid_to`).
- `dim_campaign` and `dim_date` have no history columns.
- Campaign metrics use the latest `pulled_at` per campaign per `report_date`. Earlier pulls are kept in Silver and not summed.
- Attribution: last click within 7 days before the order (`last_touch`), and first click within 7 days (`first_touch`). Orders with no click are one row with NULL campaign and click (direct).
- `delivery_lead_hours` = `delivered_at` − `order_created_at`.
- `fact_product_recs`: co-view over a 30-day window, computed from `fact_city_product_daily` and `silver_events` clicks. Top 5 per segment. Segment is city, then country, then global when a city has fewer than 20 co-viewing customers for a product. Only `is_active` products are recommended. Labelled as co-view.

## 8. Metric Dictionary

Stored in `docs/metrics.md`. Summary:

| Metric | Formula | Filter |
|---|---|---|
| ROAS (delivered) | `revenue_delivered` / `spend` | Headline metric |
| ROAS (placed) | `revenue_placed` / `spend` | Secondary |
| CPA | `spend` / `orders_placed` | Placed orders |
| Conversion rate | `orders_placed` / `sessions` (campaign sessions) | Per channel |
| Cart abandon rate | 1 − `orders_placed` / `cart_sessions` | Sessions with add to cart |
| Checkout abandon rate | 1 − `orders_placed` / `checkout_sessions` | Sessions with checkout started |
| Cancel rate | `orders_cancelled` / `orders_placed` | Placed orders |
| Failed-delivery rate | `orders_failed_delivery` / `orders_placed` | Orders with ≥1 failed attempt |
| First-attempt delivery rate | `first_attempt_delivered` / shipped orders | Shipped orders |
| AOV | `revenue_placed` / `orders_placed` | Placed orders |
| Delivery lead time | Average `delivery_lead_hours` | Delivered orders |
| Direct share | Direct orders / all orders | Last-touch |

Delivered revenue lags spend by about 2–4 days. The dashboard shows this lag next to the campaign page.

**Attribution caveat:** last-touch favours bottom-funnel channels (search, retargeting). First-touch favours awareness channels. The dashboard shows both. Neither proves incrementality, and the report says so.

## 9. Power BI Pages

1. **Executive:** delivered revenue, placed revenue, AOV, ROAS (delivered), trend against last week.
2. **Campaign:** spend, CPA, ROAS (delivered and placed), CTR, conversion rate by campaign and platform. Platform-reported conversions shown beside attributed ones. Lag note. Single headline page: placed ROAS next to delivered ROAS, plus contribution per campaign once the stretch is done. This shows that a cheap-looking campaign can be the least profitable after COD failures.
3. **Funnel:** views, cart, checkout, orders, delivered, with abandonment rates. Split by channel and payment method (COD vs prepaid).
4. **Delivery and COD:** cancel rate, failed-delivery rate, first-attempt rate, lead time by carrier and city.
5. **City and product:** top products per city, co-view recommendations (labelled co-view).
6. **Customer 360** (stretch): segment mix, repeat-buyer rate, drill-through to one customer.

Power BI shows `customer_id` only. No names or phone numbers.

## 10. Data Quality (SQL checks)

Run after every DAG load. Failed checks stop the Gold load.

| Table | Check | Threshold |
|---|---|---|
| All tables | Duplicate primary key | 0 |
| `silver_events` | Null `event_id` or `event_time` | 0 |
| `silver_events` | `event_time` in the future | 0 |
| `silver_events` | `is_late` rate | Report; alert above 5% |
| `silver_events` | `product_id` not in `silver_products` | 0 |
| `silver_order_status` | Status not in the allowed list | 0 |
| `silver_order_status` | Cancel after `order_shipped` | Quarantined, reported |
| `silver_order_items` | `line_total` ≠ `quantity` × `unit_price` | 0 |
| `fact_orders` | `order_total` ≠ sum of `fact_order_lines.line_total` | 0 |
| `silver_campaign_snapshots` | `clicks` > `impressions` | 0 |
| `silver_campaign_snapshots` | Negative `spend` | 0 |
| `dim_customer`, `dim_product` | Overlapping validity, or not exactly one `is_current` per key | 0 |
| Facts → dimensions | Orphan keys | 0 |
| Silver → Gold | Row count reconciliation (orders, lines) | Match |
| `silver_quarantine` | Share of all rows | Report; alert above 2% |

## 11. Reliability

- Kafka to Spark: checkpoints on MinIO record committed offsets. A restarted job resumes without losing rows.
- Duplicates: removed in Silver by `event_id` or natural key. `MERGE` makes replays idempotent. Gold upserts on its key.
- Late events: 10-minute watermark for live metrics. Late rows kept in Silver and corrected in Gold by the batch run.
- Out-of-order status: resolved by status rank.
- Bad messages: quarantined with error code and reason.
- Airflow tasks: retries 2 times with exponential backoff.
- Rebuild: Silver and Gold can be rebuilt from Bronze.

## 12. Security

- Secrets in `.env`, never in Git. `.env.example` lists the names.
- Postgres roles: `analyst` (read Gold), `engineer` (write), `admin`. Power BI connects as `analyst`.
- Gold holds no names or phone numbers.
- Synthetic data only. No real personal data.

## 13. Success Criteria

- Stack starts with one command on each team laptop, within the memory limits in the Compose file.
- Pipeline runs end to end at 1–2 million events.
- One 10 million event benchmark run, recorded with timings.
- Campaign DAG run shows numbers up to the trigger time.
- Power BI shows the five business questions, with delivered ROAS as the headline.
- Failure demo: Spark job killed and restarted, with no duplicate rows in Gold.
- Metric dictionary and SQL checks committed.
- Sample CSVs committed for every Silver and Gold table.
- Defense delivered on 2026-10-13.

## 14. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| 9 days to deadline | Incomplete demo | Cut order: Customer 360, contribution margin, 10M benchmark, dbt, Great Expectations |
| Laptop memory with Kafka, Spark, MinIO, Postgres, Airflow | Crashes | Compose memory limits; run 1–2M events |
| SCD2 history and joins | Bugs in facts | Test with one product price change; keep rules in SQL checks |
| Synthetic realism questioned | Credibility at defense | Document assumptions and distributions |
| Delivered revenue lag | Recent days look worse | Show lag on dashboard |

## 15. Open Items

None. Decisions so far: SCD2 kept; Customer 360 stretch; 1–2M required with a 10M benchmark. Metric dictionary written to `docs/metrics.md`.
