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
- Delta Lake, Medallion layers: Bronze and Silver on local disk (streaming path), Gold in Databricks.
- Databricks (Free Edition or trial workspace) as the cloud lakehouse and SQL serving layer for Gold. Postgres stays as a fallback serving store behind the same loader interface.
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
- Cloud hosting of Kafka, Spark or Airflow (only the Databricks Gold layer is cloud).
- ALS collaborative filtering and real-time recommendations inside the web app.
- Incrementality tests, view-through attribution, CAC payback.
- Real Meta or Google accounts.
- Refunds and partial cancellations.
- Great Expectations and dbt (replaced by SQL checks; optional if time allows).

## 3. Architecture

**Stream path (live):**
Simulator → Kafka (`app.events`, `orders`, `delivery.events`) → Spark `bronze_ingest` → Bronze Delta (local disk) → Spark `silver_clean` → Silver Delta (local disk). The local streaming job also writes `live_metrics_minute` to local Delta.

**Publish to Databricks (new):** an export step uploads the Silver tables to a Databricks Unity Catalog volume as Parquet and loads them with `COPY INTO` or `MERGE` into Silver mirror tables. Databricks cannot reach the laptop's Kafka, so this upload is how data crosses; it runs on demand and after each stream session.

**Batch path (manual Airflow trigger):**
- Ad API pull → Bronze campaign snapshots (`pulled_at`) → Silver → Databricks Gold.
- Catalog and customer export → Bronze → Silver (with `updated_at`) → SCD2 dimensions in Databricks.
- Gold facts, attribution, campaign daily, recommendations and city-product tables are built in Databricks SQL or notebooks from the Silver mirror.
- Fallback: if Airflow is not stable by 10 Oct, the same steps run as a Python script (`scripts/run_batch.py`). The tables and rules do not change.
- Fallback: if Databricks is not usable by 10 Oct, the same Gold tables load into local Postgres through the same loader interface and Power BI reads Postgres.

**Surrogate keys** are resolved at load time. The Databricks Gold build looks up `customer_key`, `product_key` and `campaign_key` from the dimension version valid at event time, and store them. Rows with no match go to `silver_quarantine`.

**Single writer per table.** Every Gold table is written only by the Databricks Gold build (the job that runs after Silver is published). The local Spark stream writes only Bronze, Silver and `live_metrics_minute`. This is simpler than the earlier Spark/Airflow split and keeps one writer per table.

**Serving:** Databricks SQL warehouse holds Gold. Power BI connects with the Databricks connector in Import mode. `live_metrics_minute` stays local and is shown in the demo from the local table (not in Power BI).

**Storage:** Bronze and Silver are Delta folders under `data/lake/{bronze,silver}`. Spark checkpoints live under `data/checkpoints/`. No object store (MinIO is removed). Gold is Delta in Databricks (Unity Catalog `funnelstream.gold`).

**Access:** Databricks host, token and warehouse id live in `.env` (never committed). The team creates the workspace; the code reads `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`.

**Loader interface:** `warehouse/store.py` defines `GoldStore.upsert(table, rows, key_cols)` with two backends, `DatabricksStore` (SQL connector, `MERGE`) and `PostgresStore`. All batch code depends only on the interface.

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
| Dirty `district` (`nasr-city`, `MAADI`, `Madinet Nasr`) on app events | 2% | Silver maps it to the canonical district of the city (section 18) |
| UTM values in the page URL upper-cased or padded (`META`, `%20google`) | 5% of campaign URLs | Silver lower-cases and trims the parsed UTM values |
| Number sent as a string (`"79.9"`) | 0.3% | Silver casts to decimal, quarantines if it fails |
| Order status out of order, or cancel after shipment | 0.5% and 0.3% | Status rank in Gold; the cancel goes to quarantine |
| Bot session (hundreds of events in seconds) | 0.1% of sessions | Silver flags `is_bot`, excluded from funnel and attribution |
| Ad API: duplicate pull, `clicks > impressions`, negative spend, HTTP 503 | small | Latest-pull rule, SQL checks, Airflow retries |
| Catalog and customer export: duplicate rows, null `city` | small | Dedup on key, default `unknown` |

Quality proof for the defense: the SQL check compares the quarantine rate and duplicate count in Silver with `noise_manifest.json`. They must match within a small tolerance.

**Limit:** synthetic data cannot be proven 100% realistic. The team documents these assumptions and checks distributions against them.

**UTM linking:** Landing and product pages carry a `page_url` whose query string holds `utm_source`, `utm_medium` and `utm_campaign` (section 18); Silver parses them. The first session event carrying UTM parameters defines `click_id`. Orders carry `session_id`, `channel`, `utm_source`, `utm_campaign` and `click_id` on `order_created`.

**Click definition:** a session click is the first `page_view` or `product_view` event in a session that carries `utm_campaign`. Attribution uses this session click. CTR uses the clicks reported by the ad API. The dashboard labels each one, because the two counts differ.

## 5. Kafka Topics

| Topic | Events | Key | Partitions | Retention |
|---|---|---|---|---|
| `app.events` | `app_open`, `page_view`, `product_view`, `product_click`, `product_view_end`, `product_search`, `add_to_cart`, `remove_from_cart`, `checkout_started`, `purchase` | `session_id` | 6 | 3 days |
| `orders` | `order_created`, `payment_authorised`, `order_packed`, `order_shipped`, `order_cancelled`, `order_completed`, `order_returned` | `order_id` | 3 | 7 days |
| `delivery.events` | `out_for_delivery`, `delivered`, `delivery_failed` | `order_id` | 3 | 7 days |

Producers use `acks=all` and idempotence. Per-session order is kept for app events. Per-order order is kept for order and delivery events.

**Order payload (`order_created`):** `order_id`, `customer_id`, `session_id`, `channel`, `utm_source`, `utm_medium`, `utm_campaign`, `click_id`, `platform` (`app` or `web`), `payment_method`, `country`, `items` (product_id, quantity, unit_price, line_total), `order_total`. `city` and `district` of the delivery address sit at the top level of the order event, next to the envelope fields.

## 6. Order Lifecycle and Status Rank

Main path:
`order_created` → `payment_authorised` → `order_packed` → `order_shipped` → `out_for_delivery` → `delivered` → `order_completed`

Cash-on-delivery orders skip `payment_authorised` (section 17, rule 7); only prepaid methods emit it.

Branches:
- `delivery_failed` returns to `out_for_delivery` on retry. Each failure increments `delivery_failed_count`.
- `order_cancelled` is valid only before `order_shipped`, judged by event time (section 17, rule 6). A cancel after shipment is rejected to quarantine.
- After the last failed delivery attempt with no success, `order_returned` ends the order (section 17, rule 5).

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
| `order_returned` | 7 (tie with `order_completed` resolved by latest `status_time`) |
| `order_cancelled` | 2 (valid only before rank 4; terminal) |

`current_status` is the status with the highest rank. A cancel that counts keeps `order_cancelled` as terminal.

`payment_method` is set on `order_created`. `carrier` is set on `order_shipped`.

## 7. Medallion Layers

All layers are Delta tables: Bronze and Silver on local disk, Gold in Databricks (Silver is also mirrored into Databricks).

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
| `silver_events` | One app or web event | `event_id` | `event_type`, `event_time` (UTC), `kafka_timestamp`, `is_late`, `event_version`, `session_id`, `customer_id` (nullable), `device`, `channel`, `country`, `city`, `district`, `campaign_id`, `utm_source`, `utm_campaign`, `utm_medium`, `click_id`, `page_url`, `product_id`, `element`, `dwell_seconds`, `cart_id`, `order_id`, `search_term`, `event_date` (Africa/Cairo) |
| `silver_order_status` | One status change | `event_id` | `order_id`, `customer_id`, `status`, `status_rank`, `status_time`, `payment_method`, `carrier`, `reason`, `event_date` |
| `silver_order_items` | One order line | `order_id`, `line_no` | `product_id`, `quantity`, `unit_price` (EGP), `line_total` (EGP), `currency`, `order_time` |
| `silver_campaign_snapshots` | One campaign per report date per pull | `campaign_id`, `report_date`, `pulled_at` | `utm_campaign`, `platform`, `impressions`, `clicks`, `spend` (EGP), `currency` |
| `silver_customers` | One customer version | `customer_id`, `updated_at` | `country`, `city`, `district`, `signup_date`, `primary_device`, `marketing_consent` |
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
| `dim_customer` | Databricks Gold build | Customer version (SCD2) | `customer_key` | `customer_id`, `country`, `city` (tracked), `district` (tracked), `signup_date`, `primary_device` (tracked), `marketing_consent`, `valid_from`, `valid_to`, `is_current` |
| `dim_product` | Databricks Gold build | Product version (SCD2) | `product_key` | `product_id`, `name`, `category` (tracked), `price` (tracked), `is_active`, `valid_from`, `valid_to`, `is_current` |
| `dim_campaign` | Databricks Gold build | Campaign | `campaign_key` | `campaign_id`, `utm_campaign`, `platform`, `name`, `start_date`, `end_date` |
| `dim_date` | Databricks Gold build | Day | `date_key` (yyyymmdd) | `date`, `week`, `month`, `is_weekend`, `is_friday_saturday`, `is_ramadan`, `is_eid`, `is_holiday` |
| `fact_orders` | Databricks Gold build | Order, current state | `order_id` | `customer_key` (-1 for an unknown member), `customer_id`, `session_id`, `channel`, `platform`, `utm_source`, `utm_campaign`, `click_id`, `campaign_id` (nullable), `payment_method`, `carrier`, `city`, `district`, `country`, `order_total` (EGP), `current_status`, `status_rank`, `order_created_at`, `delivered_at`, `cancelled_at`, `returned_at`, `delivery_lead_hours`, `delivery_failed_count`, `has_failed_attempt`, `first_attempt_delivered`, `is_cancelled`, `is_delivered`, `is_returned`, `order_seq`, `is_first_order`, `date_key` |
| `fact_order_lines` | Databricks Gold build | Order line | `order_id`, `line_no` | `customer_key`, `customer_id`, `product_key`, `product_id`, `date_key`, `city`, `district`, `quantity`, `line_total` (EGP) |
| `live_metrics_minute` (local serving, not Gold) | Local Spark stream | One minute, country | `minute_start`, `country` | `events`, `active_sessions`, `product_views`, `add_to_cart`, `orders_placed`. Computed on 1-minute tumbling windows from non-late Silver events |
| `fact_attribution` | Databricks Gold build | Order and attribution model | `order_id`, `model` | `campaign_id` (NULL for direct), `click_id` (NULL for direct), `order_total` (EGP), `model` (`last_touch` or `first_touch`) |
| `agg_orders_daily` | Databricks Gold build | Day, channel, payment method, platform | `date_key`, `channel`, `payment_method`, `platform` | `orders_placed`, `orders_delivered`, `orders_cancelled`, `orders_returned`, `orders_with_failed_attempt`, `revenue_placed` (EGP), `revenue_delivered` (EGP), `new_customer_orders` |
| `agg_funnel_daily` | Databricks Gold build | Day, country, channel, platform | `event_date`, `country`, `channel`, `platform` | `sessions`, `product_views`, `cart_sessions`, `checkout_sessions`, `orders`, `cart_abandon_rate`, `checkout_abandon_rate` |
| `agg_campaign_daily` | Databricks Gold build | Campaign per report date and attribution model | `campaign_id`, `report_date`, `attribution_model` | `ad_platform` (`meta` or `google`), `impressions`, `clicks`, `platform_conversions`, `spend` (EGP), `orders_placed`, `orders_delivered`, `orders_cancelled`, `orders_returned`, `orders_with_failed_attempt`, `revenue_placed` (EGP), `revenue_delivered` (EGP), `cancel_rate`, `failed_attempt_rate`, `return_rate`, `open_rate`, `new_customer_orders`, `aov` (EGP), `ctr`, `conversion_rate`, `cpa` (EGP, placed), `cost_per_delivered_order` (EGP), `roas_placed`, `roas_delivered` |
| `agg_city_product_daily` | Databricks Gold build | City, product, day | `city`, `product_id`, `date_key` | `sessions_viewed`, `add_to_cart`, `units`, `revenue` (EGP) |
| `rec_product` | Databricks Gold build | Recommendation | `segment_level`, `segment_value`, `anchor_product_id`, `rank` | `rec_product_id`, `co_view_sessions`, `window_end` |
| `customer_360` (stretch) | Databricks Gold build | Customer, current state | `customer_id` | `customer_key`, `country`, `city`, `district`, `total_sessions`, `product_views`, `add_to_cart_count`, `checkouts_count`, `orders_count`, `delivered_count`, `cancelled_count`, `total_spend` (EGP), `avg_order_value` (EGP), `first_order_at`, `last_order_at`, `last_seen_at`, `days_since_last_order`, `top_category`, `preferred_payment_method`, `last_campaign_id`, `is_repeat_buyer` |

**Stretch: contribution margin.** `cfg_cost_assumptions` (Airflow, one row per product category): `margin_pct`, `carrier_fee_egp`, `failed_attempt_cost_egp`, `cod_fee_egp`. Adds `contribution_egp` to `fact_orders` and `agg_campaign_daily`. Labelled as assumption-based on the dashboard.

**Rules:**
- SCD2 applies only to `dim_customer` and `dim_product`. `valid_from` comes from `updated_at` in Silver. Facts join to the version valid at event time (`valid_from` ≤ event time < `valid_to`).
- `dim_campaign` and `dim_date` have no history columns.
- Campaign metrics use the latest `pulled_at` per campaign per `report_date`. Earlier pulls are kept in Silver and not summed.
- Attribution: `last_touch` (the order's own session click first, then the customer's last click in the previous 7 days; spec section 17, rule 11) and `first_touch` for `fact_attribution`. Orders with no click are one row with NULL campaign and click (direct). `agg_campaign_daily` uses `last_touch` and says so in `attribution_model`.
- `delivery_lead_hours` = `delivered_at` − `order_created_at`.
- Delivered revenue is dated on the ORDER date (cohort view). `current_status` follows the timed cancel rule (spec section 17, rule 6).
- `rec_product`: co-view over a 30-day window, computed from `agg_city_product_daily` and `silver_events` clicks. Top 5 per segment. Segment is city, then country, then global when a city has fewer than 20 distinct co-viewing sessions for a product. Only `is_active` products are recommended. Labelled as co-view.

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
| Failed-attempt rate | `orders_with_failed_attempt` / `orders_placed` | Orders with at least one failed attempt |
| Return rate | `orders_returned` / `orders_placed` | Final status `order_returned` |
| Open rate | open after 14 days / `orders_placed` | Not delivered, cancelled or returned after 14 days |
| Cost per delivered order | `spend` / `orders_delivered` | Delivered orders |
| First-attempt delivery rate | `first_attempt_delivered` / shipped orders | Shipped orders |
| AOV | `revenue_placed` / `orders_placed` | Placed orders |
| Delivery lead time | Average `delivery_lead_hours` | Delivered orders |
| Direct share | Direct orders / all orders | `agg_orders_daily`, channel `direct` |

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

- Kafka to Spark: checkpoints on local disk record committed offsets. A restarted job resumes without losing rows.
- Duplicates: removed in Silver by `event_id` or natural key. `MERGE` makes replays idempotent. Gold upserts on its key.
- Late events: 10-minute watermark for live metrics. Late rows kept in Silver and corrected in Gold by the batch run.
- Out-of-order status: resolved by status rank.
- Bad messages: quarantined with error code and reason.
- Airflow tasks: retries 2 times with exponential backoff.
- Rebuild: Silver and Gold can be rebuilt from Bronze.

## 12. Security

- Secrets in `.env`, never in Git. `.env.example` lists the names.
- Databricks: Power BI connects with a read-only token or a service principal limited to `funnelstream.gold`. The write token is used only by the load job. Tokens live in `.env`. Fallback Postgres roles: `analyst` (read), `engineer` (write), `admin`.
- Gold holds no names or phone numbers.
- Synthetic data only. No real personal data.

## 13. Success Criteria

- Stack starts with one command on each team laptop, within the memory limits in the Compose file.
- Pipeline runs end to end at 1–2 million events.
- One 10 million event benchmark run, recorded with timings.
- Campaign DAG run shows numbers up to the trigger time.
- Power BI shows the five business questions, with delivered ROAS as the headline.
- Failure demo: the local Silver job is killed and restarted with no duplicate `event_id` in Silver; Gold is unchanged after a republish and rebuild.
- Metric dictionary and SQL checks committed.
- Sample CSVs committed for every Silver and Gold table.
- Defense delivered on 2026-10-13.

## 14. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| 9 days to deadline | Incomplete demo | Cut order: Customer 360, contribution margin, 10M benchmark, dbt, Great Expectations |
| Laptop memory with Kafka, Spark, Postgres, Airflow | Crashes | Compose memory limits; run 1–2M events |
| Databricks access, limits or upload time | No cloud serving | Create the workspace on 5 Oct; test one upload on 6 Oct; fall back to Postgres by 10 Oct |
| SCD2 history and joins | Bugs in facts | Test with one product price change; keep rules in SQL checks |
| Synthetic realism questioned | Credibility at defense | Document assumptions and distributions |
| Delivered revenue lag | Recent days look worse | Show lag on dashboard |

## 15. Open Items

None. Decisions so far: SCD2 kept; Customer 360 stretch; 1–2M required with a 10M benchmark. Metric dictionary written to `docs/metrics.md`.


## 16. Compatibility rulings (v4, after the Databricks and noise reviews)

These rules win over any older wording above or in the plan.

1. **Late rows.** `is_late` excludes a row only from `live_metrics_minute`. Funnel, attribution and every other Gold table include late rows. `is_late` is meaningful in `--live` runs only (history mode publishes old events, so all rows are late by the rule); the manifest "late" counts are verified only on live runs.
2. **Gold build.** One job, `warehouse/gold_build.py`, runs after `publish_silver` in fixed order: dimensions, `dim_date`, `fact_orders`, `fact_order_lines`, `fact_attribution`, `agg_orders_daily`, `agg_funnel_daily`, `agg_campaign_daily`, `agg_city_product_daily`, `rec_product`, SQL checks. Row-scale tables are `MERGE ... SELECT` SQL files under `warehouse/gold/*.sql` run through `store.query`. Only small tables (for example `agg_campaign_daily`) go through `upsert(rows)`. The Python builders in Tasks 12 to 14 stay as reference implementations with a parity test.
3. **Silver owns quarantine.** `silver_quarantine.error_code` values: `BAD_JSON, MISSING_FIELD, UNKNOWN_EVENT_TYPE, BAD_VERSION, NEGATIVE_PRICE, FUTURE_TIMESTAMP, BAD_VALUE, UNKNOWN_PRODUCT, CANCEL_AFTER_SHIPMENT, NEGATIVE_SPEND, CLICKS_GT_IMPRESSIONS`. `null_product_id` maps to `MISSING_FIELD`. The campaign Silver step quarantines negative spend and clicks above impressions. `silver_products` is loaded before the stream starts and passed to the validator as `known_products`.
4. **Silver shape.** Silver flattens the payload into the columns listed in section 7 (`product_id`, `utm_source`, `utm_campaign`, `click_id`, `order_id`, and so on) and adds `is_bot`. A new table `silver_orders` (one row per `order_created`) carries `order_id, customer_id, session_id, channel, utm_source, utm_campaign, click_id, payment_method, city, country, items (array), order_total, order_created_at`. `fact_orders` joins `silver_orders` to `silver_order_status`.
5. **Bots.** `is_bot` is recomputed over the whole Silver events table in the Gold build (not per micro-batch) and excludes bot sessions from the funnel, attribution and live metrics.
6. **Dates.** `event_date` and `date_key` come from `from_utc_timestamp(ts, 'Africa/Cairo')`. The ad API `report_date` is a UTC day; the campaign daily table keeps `report_date` as given and the report notes the offset.
7. **Placed orders.** Placed means every attributed order regardless of status, matching `docs/metrics.md`. Cancel and failed-delivery rates therefore never exceed 100%.
8. **Databricks types.** Delta DDL uses `STRING`, `TIMESTAMP`, `DECIMAL`, `BIGINT GENERATED ALWAYS AS IDENTITY` for surrogate keys, informational `PRIMARY KEY` only, no `UNIQUE`. SCD2 `valid_to` for the current row is `9999-12-31 00:00:00 UTC`. `MERGE INTO` replaces `ON CONFLICT`. The Postgres fallback keeps its own DDL.
9. **Loader.** `GoldStore` has `upsert(table, rows, key_cols)` and `query(sql)`. No batch, check or export code imports psycopg or reads `PG_DSN` directly; they take a `store` argument defaulting to `get_store()`. Tests use a fake store.
10. **Checks.** Add a Python check type that reads `noise_manifest.json`: the expected Silver quarantine count is the sum of `malformed_json, missing_field, unknown_type, bad_version, negative_price, future_timestamp, null_product_id, unknown_product_id, cancel_after_ship`; duplicates must match `duplicate`; tolerance 2% of the manifest count. Add reported (non-failing) check: negative `delivery_lead_hours`, caused by the out-of-order status noise.
11. **Failure demo.** It reads local Silver Delta with Spark and asserts zero duplicate `event_id` after a kill and restart.
12. **Host networking.** Kafka advertises `localhost:9092` for host clients (simulator, Spark) and `kafka:19092` internally. The Airflow container installs the Databricks connector and requests, mounts `warehouse`, `contracts` and `data`, and reaches the mock ad API at `http://host.docker.internal:8000`. `publish_silver` runs on the host before the DAG.
13. **Publish allow-list.** `publish_silver` uploads only an explicit column allow-list. Names, phones and raw Bronze rows never leave the machine. Free Edition may lack service principals, so Power BI uses a read-only user token.
14. **City labels.** `Other` is a real value (smaller cities). `unknown` means missing or unrecognised. Power BI shows both.
15. **Campaign sessions.** `agg_city_product_daily.sessions_viewed` counts distinct sessions, not customers.
16. **Spark runs in Docker.** Spark does not start on Windows without Hadoop's `winutils.exe` (an unsigned community binary), so the Spark jobs and Spark tests run in a `spark` Compose service (Python 3.11, Java 17, PySpark, Delta) with the repo mounted at `/app` and Kafka at `kafka:19092`. The simulator, mock ad API and non-Spark tests stay on the host. Bronze has three tables (`bronze_app_events`, `bronze_orders`, `bronze_delivery_events`), one streaming query per topic. Container memory is limited to 3 GB; Docker has about 8 GB in total, so keep other services within their limits.
17. **Risks.** Databricks Free Edition limits and upload time for 1 to 2 million rows: test one upload early, fall back to Postgres by 10 Oct.

## 17. Amendment C: model fixes and live monitoring (after external review, 2026-10-05)

These rulings win over any older wording in this spec, the plan or the documents.

**Ownership and loading**
1. **Gold ownership.** Every Gold table is written only by the Databricks Gold build. It recomputes fact and summary tables for the affected dates from Silver, so late data is corrected by rebuilding, never by increments.
2. **Unknown members.** `dim_customer`, `dim_product` and `dim_campaign` each hold a row with key `-1` (natural id `unknown`, name `Unknown`, `is_current` true, `valid_from` 1900-01-01, `valid_to` 9999-12-31). A fact row with no matching dimension version (a brand-new customer or product, an anonymous or null customer) stores key `-1` and is NOT quarantined. Facts keep the natural ids (`customer_id`, `product_id`) beside the keys, so each daily Gold build re-joins and repairs `-1` keys once the dimension row exists. Surrogate keys are `BIGINT GENERATED BY DEFAULT AS IDENTITY` (not ALWAYS) so the `-1` rows can be seeded with an explicit key by `apply_ddl`. The loader never supplies identity values for normal rows.
3. **Spark to Postgres (live path).** The local Silver job, in `foreachBatch`, recomputes the full per-minute totals for every minute touched by the batch from Silver and upserts them into Postgres with `INSERT ... ON CONFLICT (minute_start, country) DO UPDATE SET <all measures> = EXCLUDED.<measure>`. Totals are always recalculated, never incremented.
4. **Daily SCD2.** The batch is daily, so a dimension attribute that changes twice in one day yields only one new version for that day.

**Lifecycle and definitions**
5. **Final statuses.** New terminal status `order_returned` (event type on topic `orders`): emitted after the last failed delivery attempt when no attempt succeeded (COD refused or unreachable). Rank 7, shared with `order_completed`; the two are mutually exclusive and ties resolve by latest `status_time`.
6. **Cancel judged by event time.** A cancel counts only if no `order_shipped` row has `status_time` earlier than the cancel's `status_time`. A counted cancel is terminal. Arrival order never matters. A cancel after shipment is quarantined in Silver (`CANCEL_AFTER_SHIPMENT`). A reported check counts orders that have both a counted cancel and a later shipment.
7. **COD skips `payment_authorised`.** Cash-on-delivery orders go `order_created` to `order_packed`; only prepaid methods (fawry, vodafone_cash, instapay, card) emit `payment_authorised`. Payment for COD is collected at `delivered`.
8. **Failure definitions.** `failed_attempt_rate` = orders with at least one `delivery_failed` / orders placed. `return_rate` = orders whose final status is `order_returned` / orders placed. `open_rate` = orders not delivered, cancelled or returned after 14 days / orders placed. `failed_delivery_rate` is retired in favour of these three.
9. **Funnel grain.** `agg_funnel_daily` is keyed by `event_date`, `country`, `channel`, `platform` (`app` or `web`, a new event field). `payment_method` is removed from the funnel because it is not known before checkout. COD versus prepaid lives in `agg_orders_daily`.
10. **Attribution model.** `agg_campaign_daily` uses the `last_touch` model and carries an `attribution_model` column. `direct_share` is removed from it; direct share is computed from `agg_orders_daily` (channel `direct`).
11. **Anonymous sessions.** Attribution first uses the order's own session (`click_id` on `order_created`). If the order has no click in its own session, it falls back to the customer's clicks in the previous 7 days (identified customers only). Linking an anonymous pre-login session to a customer after login is out of scope and is documented as a limitation.
12. **Revenue dating.** Delivered revenue is counted on the ORDER date (cohort view), so recent days are incomplete. `cost_per_delivered_order` = `spend` / `orders_delivered` is added next to `cpa`.
13. **Table roles and names.** Base facts keep `fact_` (`fact_orders`, `fact_order_lines`, `fact_attribution`). Summaries use `agg_`: `agg_funnel_daily`, `agg_campaign_daily`, `agg_city_product_daily`, new `agg_orders_daily` (keys `date_key`, `channel`, `payment_method`, `platform`; measures: orders placed, delivered, cancelled, returned, with failed attempt, revenue placed and delivered, new-customer orders). Outputs: `rec_product` (was `fact_product_recs`) and `customer_360` (was `fact_customer_360`, stretch). Dimensions keep `dim_`.
14. **City and carrier** are plain columns on purpose: low cardinality, no attributes beyond the name, so a dimension adds a join and no information.
15. **Platform-reported conversions.** `silver_campaign_snapshots` gains `platform_conversions` (bigint); `agg_campaign_daily` shows `platform_conversions` beside `orders_placed` (attributed).
16. **Recommendation threshold** counts distinct sessions: a city is used for an anchor product only if at least 20 distinct sessions viewed it and the recommended product.

**Monitoring (Grafana required, Prometheus out of scope)**
17. **Live store.** Postgres (Docker) holds three small tables: `live_metrics_minute` (events, active_sessions, product_views, add_to_cart, orders_placed per minute and country), `pipeline_health_minute` (per minute: events, late_events, quarantined rows, silver batches), `pipeline_heartbeat` (component, last_seen_at; components: silver_job, gold_build, publish). The Silver job writes the first two in `foreachBatch`; the publish and Gold build scripts write their heartbeat.
18. **Grafana** runs in Docker Compose (about 0.5 GB), reads Postgres directly with a 5 second refresh, and is provisioned from files under `monitoring/grafana/` (datasource, dashboard JSON, alert rules). Panels: events per minute, active sessions, orders per minute, late-event rate, quarantine rows per hour, seconds since the last Gold update and last Silver batch.
19. **Alerts** (Grafana alert rules, evaluated every minute): late-event rate above 5% over 5 minutes; quarantine share above 2% over 5 minutes; no events for 2 minutes. They show as firing in Grafana and notify a log or webhook contact point. History mode data is all late by rule, so the late-rate alert is meaningful in `--live` runs only.
20. **Failure demo uses Grafana:** kill the Silver job, the event line drops to zero and the no-events alert fires; restart it, the line recovers and a duplicate check shows no duplicate keys.
21. **Compose memory.** The Airflow container is removed from the default Compose file (the Python script `scripts/run_batch.py` is the batch entry point; the Airflow DAG file stays for reference). Docker has about 8 GB: Kafka 1 GB, Postgres 0.5 GB, Grafana 0.5 GB, Spark 3 GB.

**Clean-ups**
22. The Inmon claim is dropped; Silver is the cleaned layer, Gold the Kimball-style marts.
23. One term: **quarantine table**. The `*.dlq` Kafka topics (created but never written) are removed.
24. Silver tables `silver_events`, `silver_orders` and `silver_order_status` are partitioned by `event_date` (done).
25. The "J&T Express" typo in the PDF is a character-entity bug and is fixed.
26. The Airflow-or-Python fallback decision date is 10 Oct, not 11 Oct. Python script is primary; Airflow DAG only if time allows.

## 18. Amendment D: district under city, UTM in the page URL (2026-10-06)

These rulings win over any older wording.

1. **Address hierarchy.** An address is country > city > district. `city` keeps the existing canonical values (Cairo, Giza, Alexandria, Mansoura, Tanta, Assiut, Other). `district` is a new column, always a child of its city. Source of truth: `contracts/geo.py` (`DISTRICTS`: city to ordered tuple of districts). The district names are the official subdivisions (qisms / ahya) of Egypt:
   - **Cairo:** Nasr City, Heliopolis (Masr El Gedida), Maadi, New Cairo, Shubra, Zamalek, Downtown, Garden City, El Moqattam, Helwan, El Matareya, El Marg, El Zaytoun, Sayeda Zeinab, El Shorouk.
   - **Giza:** Dokki, Agouza, Haram (Pyramids), Imbaba, Omraniya, Bulaq El Dakrour, El Warraq, Giza (centre). Mohandessin and Faisal are neighbourhoods inside these qisms, not districts, so they are not used. 6th of October and Sheikh Zayed are separate cities (not under Giza city) and fall under city `Other`.
   - **Alexandria:** Montaza, Raml, Sidi Gaber, Moharam Bek, Karmouz, Amreya, Dekhela, Attarin, Manshiya, Labban.
   - **Mansoura:** Mansoura First, Mansoura Second. **Tanta:** Tanta First, Tanta Second. **Assiut:** Assiut First, Assiut Second.
   - **Other:** the single value `Other` (smaller cities have no modelled districts).
2. **Values.** `district` of a known pair is the canonical spelling. A district that is missing, unrecognised, or does not belong to the row's city is stored as `unknown` (the row is kept, never quarantined). `Other` is a real value (city `Other` only). `unknown` city gives `unknown` district. A reported (non-failing) check counts hierarchy violations (district given but not under the city).
3. **Where it lives.** `district` sits directly after `city` on customers (`silver_customers`, `dim_customer`, tracked SCD2 attribute with city), app events and order events (`silver_events`, `silver_orders`), and in Gold on `fact_orders`, `fact_order_lines`, `customer_360`. Unknown members carry `unknown`. `agg_city_product_daily` stays at city grain on purpose.
4. **Tracking URL.** Landing and product pages carry a real URL in the payload field `page_url`, for example `https://shop.example.eg/p/prod_0042?utm_source=meta&utm_medium=paid_social&utm_campaign=ramadan_offer_1`. The simulator no longer writes `utm_source` / `utm_campaign` into the payload; they exist only inside `page_url`. Sessions with no campaign get a plain URL (organic, direct, email) with no UTM parameters.
5. **UTM parsing.** Silver parses `page_url` (urllib) and fills `utm_source`, `utm_medium`, `utm_campaign` (lower-cased, trimmed) and keeps `page_url`. For rows that still carry the old payload fields (older Bronze data) the payload value is the fallback. A malformed URL is kept with NULL UTM fields. `silver_events` gains `utm_medium` and `page_url`; `silver_orders` gains `utm_medium`. Campaigns gain `utm_medium` (`cpc` for google, `paid_social` for meta).
6. **Noise.** Two new noise kinds with manifest counts: `dirty_district` (case, spacing, hyphen and common alias variants such as "nasr-city", "Madinet Nasr", "MAADI") and `dirty_utm` (upper case or padded UTM values inside the URL). Both are cleaned by Silver, not quarantined.
7. **Privacy.** District is not personal data on its own; it is on the publish allow-list. `page_url` is published (synthetic data; review before real data). Names and phones stay off.
