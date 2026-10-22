# FunnelStream: live inventory for operations and D-1 marketing (design)

Date: 10 October 2026. Status: draft for review, not committed. Defense: 13 October 2026.

## 1. Purpose

FunnelStream becomes one platform for two end users.

1. **Operations user**, as in a quick-commerce app such as Breadfast or noon Minutes. This person watches, in Grafana and in real time, the traffic on the app and the real inventory of the dark stores: what is in stock, what is out of stock, and how much customer demand is hitting empty shelves.
2. **Marketing user.** This person reads, each morning, the report for day minus one: yesterday's campaign details and yesterday's sales performance, including how much of the advertising spend sent customers to products that were out of stock.

The story that only a joined platform can tell, and the main point of the defense, is the bridge between the two: **advertising spend exposed to out-of-stock products**. Operations sees it as lost demand now. Marketing sees it the next morning as estimated exposed spend.

Success criteria:

- Inventory events flow through the same Kafka, Bronze, Silver and live-table path as orders, with the same guarantees (checkpoints, idempotent MERGE, quarantine, replay after a crash). The one exception is the out-of-stock flag of the demand table (section 7): it is the stock when the minute was written and is not replay-safe. That is a stated limitation, not a guarantee.
- The Grafana dashboard "Operations: live inventory" shows stock, out-of-stock and lost demand per store, and an alert fires when demand keeps arriving at zero stock.
- A repeatable demo scenario shows a stock-out, the lost demand, the alert and the restock in about three minutes.
- A Grafana page "Marketing D-1" shows yesterday's campaign numbers and the exposed-spend estimate.
- The data is simulated and is labeled as simulated everywhere it is shown.

## 2. Scope and cuts

In scope: two new event types, a new Kafka topic, Bronze and Silver tables, a stock model in the simulator, live Postgres tables, one Grafana dashboard for operations, one alert, a D-1 marketing mart in Postgres and its Grafana page.

Cut, listed as future work: reservations (`reserved_qty` exists and is 0 by assumption), shrinkage and fulfillment panels, district-level stock, noise injection on inventory events, publishing the stock tables to the Databricks mirror and a Gold table `agg_oos_demand_daily` (done only if time remains after the live path and the D-1 mart work).

## 3. Architecture

```
simulator --> Kafka inventory.events (3 partitions, key = store_id)
          --> Bronze  bronze_inventory_events
          --> Silver  silver_stock_movements  (key event_id)
                      silver_stock_snapshots  (key event_id)
          --> live sink --> Postgres live_stock_now, live_oos_demand_minute (all served demand, flag oos_at_write)
                            (dim_store, dim_store_district, dim_product_current are filled by apply_live_ddl.py)
          --> Grafana "Operations: live inventory"

daily job: Databricks agg_campaign_daily (D-1 spend, ROAS, CPA, orders)
         + Postgres live_oos_demand_minute (served and out-of-stock landing clicks and add-to-carts,
           flagged with the stock when the minute was written)
         --> Postgres marketing_d1_campaign, marketing_d1_product --> Grafana "Marketing D-1"
```

The new Bronze table joins the existing stream union and the pipeline job. `silver_clean.py` builds `SILVER_TABLES` from `TABLE_COLUMNS`, so each new Silver table gets its MERGE automatically, and since the Silver fix those MERGEs run concurrently.

Data freshness: Postgres lags Kafka by about one Silver batch (20 to 60 s). The dashboard shows "stock as of N seconds ago" so this is visible, not hidden.

## 4. Event contracts (version 1)

Stock events use the standard envelope. A flat event would be quarantined, because `contracts/validate.py` requires `event_id, event_type, event_version, event_time` and a `payload` object (`REQUIRED_ALL`, line 13), and `streaming/silver/transform.py` (lines 213 to 215) sends every non-app event to the order parser.

Top-level fields: `event_id`, `event_type`, `event_version` (1), `event_time`, `store_id`, `product_id`, `payload`.

- `stock_snapshot` payload: `on_hand` (integer, 0 or more), `reserved_qty` (integer, 0 by assumption), `reorder_point` (integer).
- `stock_movement` payload: `qty_delta` (non-zero integer), `reason` (`received`, `sold`, `shrinkage`, `adjustment`), `order_id` (required when the reason is `sold`), `requested_qty` (on `sold`, the quantity the order asked for).

Required changes in the contract layer (each with a failing test first):

- `contracts/topics.yaml`: add `inventory_event_types` and the topic `inventory.events`; `contracts/validate.py` adds them to `KNOWN_TYPES` and requires `store_id` and `product_id` for them.
- `streaming/silver/transform.py`: an explicit stock branch; any unknown type raises, so a stock event can never be parsed as an order.
- Both event ids are deterministic: derived from the run seed, the event kind and a counter. A replay of the same run (same seed) produces the same key and the MERGE inserts nothing twice; a different run (another seed) produces different ids.

Definitions: `available = on_hand - reserved_qty`. A product is **out of stock** when `available <= 0` and **low stock** when `0 < available < reorder_point`.

## 5. Stores and the district map

One source: `contracts/stores.py`, built on `contracts/geo.py`. Eight dark stores: Cairo 3 (Nasr City, Maadi, New Cairo), Giza 1, Alexandria 2, Mansoura 1, Tanta 1. Each district has exactly one primary store. Assiut and "Other" have no store; they are "not served" and are excluded from out-of-stock metrics. A real system uses delivery zones, and the defense says so.

The map is written to Postgres as `dim_store` and `dim_store_district` from this one source. Demand events carry a city and district but no store, so the join to stock is done in SQL only. The mapping is never reimplemented in Spark or Databricks SQL.

## 6. Simulator stock model

Deterministic from the run seed and the existing seeded generator; no other random source.

- A `StockLedger` holds stock per store and product and never lets it go below zero. The model is reserve-on-order: a sale is booked when the order is created and nothing is put back when an order is cancelled, returned or fails delivery (section 14).
- It emits an opening `stock_snapshot` for every store and product, then a baseline snapshot every five minutes.
- Opening stock is 200 to 500 units per product, about 200 products.
- Each order line emits a `sold` movement at the order's store with `order_id`. The movement is clamped to the available stock, and `requested_qty` records what was asked. A line that asked for more than was available is oversold, which is what an app without reservations does.
- Demand events (views, add-to-carts) are not stopped by empty stock. That is the lost demand.
- `--scenario hero-stockout` picks three hero products at the busiest store with 40 units each, boosts their views and add-to-carts (the campaign push), and emits a `received` restock about 90 seconds after the stock-out. The same seed gives the same demo.
- Also present in the scenario: one low-stock product that never runs out, one slow `shrinkage` and one `adjustment`, so every reason code appears.

## 7. Silver and live serving

Silver: `silver_stock_movements` and `silver_stock_snapshots`, both keyed by `event_id`. The key list, partition list, column list, schemas, publish allow-list and DDL generation are updated together. The tests that pin counts are updated with them (the reviewer found `tests/test_warehouse_tables.py` lines 118, 174, 189 and 281, `tests/test_bronze.py` line 55, `tests/test_c1_changes.py` line 243, `tests/test_checks.py` line 110, `tests/test_silver_transform.py` line 357 and `tests/test_silver_spark_schemas.py` line 69; the list is verified again when the work starts).

Postgres (DDL in `warehouse/live_ddl.sql`, read-only role `grafana_ro` gets SELECT):

- `dim_store`, `dim_store_district`.
- `dim_product_current(product_id, name, category, price)`: the latest version of every product from `silver_products`, filled by the same job that fills `dim_store` (`scripts/apply_live_ddl.py`), so Grafana tables show product names.
- `live_stock_now(store_id, product_id, on_hand, reserved_qty, available, reorder_point, updated_at)`, primary key `(store_id, product_id)`.
- `live_oos_demand_minute(minute_start, store_id, product_id, campaign_id, utm_campaign, views, add_to_cart, landing_clicks, oos_at_write)`, primary key `(minute_start, store_id, product_id, campaign_id)`. It stores ALL served demand of a minute; nothing is filtered when it is written. `campaign_id` is resolved from `utm_campaign` through the static campaign list (`'direct'` when the event has no campaign, `'unknown'` for a campaign that is not listed); `utm_campaign` is kept only as an attribute. `landing_clicks` counts product views that carry a campaign. `oos_at_write` is true when the pair's available stock was zero at the moment the sink wrote the minute.

How the sink fills them, after each Silver batch, only for the `(store, product)` pairs and minutes the batch touched:

- Stock now is a full recompute: the latest snapshot at or before the cut plus the sum of the movements after it, ties broken by `event_id`. It does not depend on arrival order, so late or replayed events cannot corrupt it, and the failure demo still holds.
- Demand: `product_view` and `add_to_cart` events of the touched minutes in served districts are counted per minute, store, product and campaign and the touched minutes are upserted (a total replaces the previous total of that minute). The flag `oos_at_write` is then set from `live_stock_now` as it is when the minute is written: "stock when the minute was written". This is accurate to about one batch, and the panels say so.
- **Limitation (stated on every dashboard and document):** the flag is not replay-safe. A replay, a late event or a rewrite of a minute after a restock can flag the minute differently, and a stock-out that starts in the middle of a minute credits the whole minute. The table is therefore not a pure function of Silver. The fix is future work: compute zero-stock intervals from the Silver stock tables and join demand by event time, with one function shared by the sink and the daily job.

## 8. Grafana: Operations, live inventory

Seven panels, top row first: out-of-stock products now, low-stock products, "stock as of N seconds ago"; out-of-stock demand per minute (the bridge panel) and live traffic; a store by top-twenty-products stock heatmap; a "lost demand now" table (top products by add-to-carts in minutes flagged as zero stock, last 15 minutes, with product names from `dim_product_current`).

One new alert, `fs-oos-demand`: at least 5 add-to-carts in minutes flagged `oos_at_write` for the same store and product (summed over campaigns) in each of two consecutive minutes (the hero scenario produces about 1 per second, so it fires; normal traffic does not). The existing alerts (heartbeat silent, no events, quarantine, late events) stay.

## 9. Marketing D-1

A daily job `scripts/build_d1_marketing.py` for a given report date (default: yesterday on the Cairo calendar).

**Day rule (one rule everywhere).** Every campaign measure uses the Cairo day. `agg_campaign_daily.report_date` is the Cairo ad day and its order columns are the orders placed on that Cairo date. The D-1 job takes yesterday in Cairo, reads the demand minutes of that Cairo day and the ad row with that `report_date`. (Changed 22 October 2026; before, the ad report was a UTC day and the 3-hour mismatch was accepted.)

1. Read that day's spend, impressions, clicks, orders placed, revenue placed and delivered, CPA, ROAS placed and ROAS delivered per campaign from the Databricks `agg_campaign_daily` (`attribution_model = 'last_touch'`).
2. From Postgres `live_oos_demand_minute` (the Cairo day), count per campaign the served landing clicks (product views that carry a campaign, in served districts) and how many of them were in minutes flagged `oos_at_write`; and per store and product the flagged views and add-to-carts. Prices come from `dim_product_current`. There is no read of local Silver and no Spark in this job.
3. Write `marketing_d1_campaign` (key `report_date, campaign_id`) and `marketing_d1_product` (key `report_date, store_id, product_id`) to Postgres.

Definitions shown on the page:

- `oos_click_share = oos_clicks / served_clicks`.
- `exposed_spend = spend x oos_clicks / served_clicks`. Both counts come from the same table (`live_oos_demand_minute`, equal to the sums of `oos_demand_daily`), for served districts only. The ad API gives spend per campaign per day only, so the metric is an estimate, assuming spend is spread evenly over served landing clicks. It is titled "estimated spend exposed to out-of-stock"; it is not called an upper bound and not "wasted", because a customer may buy a substitute.
- "Out of stock" means the stock when the minute was written (section 7): not replay-safe.
- `revenue_at_risk = add-to-carts on out-of-stock products x price`. `marketing_d1_product` keeps EVERY out-of-stock product of the day per store (a row has no rank and no limit); Top N is done by the visual.

The Grafana page "Marketing D-1" shows nine numbers (spend, clicks, orders placed, CPA, ROAS placed, ROAS delivered, out-of-stock click share, exposed spend, out-of-stock add-to-carts), a table per campaign sorted by exposed spend, and the top five products by revenue at risk with their store and name. ROAS is a period ratio (docs/metrics.md): a single day is a cohort-by-order-date view. Recent days are marked provisional for ROAS delivered.

If time remains: publish the stock tables to the Databricks mirror and move step 2 into a Gold SQL step `agg_oos_demand_daily`.

## 10. Testing

Test first for every step. Host tests cover the contract, validator, transform, ledger and store map. Spark tests (in Docker, per file) cover Silver and the live sink, including replay, a crash in the middle of a batch, late and duplicate movements, and "stock never negative". Provisioning tests check the Grafana JSON and alert rules. The existing 63 data quality checks, the duplicate check and the failure demo must still pass.

## 11. Order of work and honest risks

Order: finish and commit the Silver speed fix; pass the failure demo (G3) twice; then the inventory steps in this order: contract and validator, simulator ledger and scenario, topic and Bronze and Silver tables, live sink and DDL, Grafana operations dashboard and alert; then the D-1 job and page; then the T17 samples, README and a 1 million event benchmark.

Risks: the defense is on 13 October and G3, the Silver fix commit and T17 are still open, so the D-1 page and the Databricks Gold step are the first to move to "future work". A scripted stock-out can look circular; the answer is to show that every number is computed by the pipeline from raw events, and to say clearly that the inventory is simulated.

## 12. Assumptions to confirm

- Out of stock means `available <= 0`; low stock means below the reorder point.
- The D-1 mart uses the demand the live sink recorded minute by minute with the stock when the minute was written (section 7), not an exact per-event stock and not an hourly stock state.
- An unmapped or "unknown" district is not counted as out-of-stock demand.

## 13. Update of 10 October, late evening: Power BI, Grafana insights and the express promise

The product owner asked for three changes. They are part of the same design and are built in the plan `docs/superpowers/plans/2026-10-10-inventory-two-users.md`.

**Inventory in Power BI.** Power BI reads two sources: the Databricks Gold tables and the Postgres inventory marts. Four dimensions (`Date`, `Campaign`, `Store`, `Product`), ten fact-like tables, Import mode, all relationships many-to-one and single direction. The model, relationships, five pages and the measures are in `dashboards/powerbi/MODEL.md` and `dashboards/powerbi/measures.dax`. New Postgres tables for Power BI and Grafana: `inventory_daily`, `oos_demand_daily`, `express_daily` (Task 9A, all written by `scripts/build_inventory_daily.py`, which rebuilds the last two Cairo days by default) next to the live and D-1 tables of the earlier sections. `oos_demand_daily` relates to `Campaign` on `campaign_id` only and carries both `served_clicks` and `oos_clicks`; `Marketing D1 Product` relates to `Store` and `Product`. In all, the Postgres side has eight tables that matter for the report: the seven loaded by Power BI plus `dim_product_current`, which only Grafana uses.

**Inventory insights in Grafana.** Besides the live operations dashboard (section 8) there is a second dashboard, "Inventory insights, daily" (closing stock by store, units sold, received and shrunk, oversold units, products that ended the day out of stock, lowest days of cover, demand on empty shelves, express on-time rate). The marketing D-1 mart (section 9) is still the stretch; the daily inventory mart is core.

**The live-inventory section is an express service with a 20-minute promise.** Orders placed from it carry `service = express` and their own `promised_minutes` (20). The simulator makes a normal express order take 8 to 17.5 minutes and never more than 19.5. The pipeline records each express order in `silver_express_orders`, the live sink writes `live_express_orders`, and Grafana shows at-risk and breached orders and the on-time rate. The alert `fs-express-sla` fires on ANY breach: a delivery later than the promise, or an undelivered order with `now() - created_at > promised_minutes + 1 minute` (a one-minute grace for a `delivered` event that reaches the sink a batch late). "Undelivered past its promise" excludes orders whose latest status is cancelled or returned. The SLA clock compares the `order_created` time with the `delivered` event time; both are simulator event times, so there is no clock skew between the customer side and the courier side. `express_daily` measures an undelivered order's age at build time and `build_inventory_daily.py` rebuilds the last two Cairo days by default, so an order that was still open at midnight is settled by the next run. The platform cannot enforce delivery; it makes every breach visible within a minute. Express timing is never time-compressed in live mode; a rehearsal can shorten the promise (`--express-promise-minutes 2`) and make every K-th order late (`--late-express-every K`) to show the alert in about three minutes.

Cuts that stay: reservations, the Databricks mirror of the stock tables and a Gold table for inventory, noise injection on inventory events.

## 14. Known limitations (stated in the documents and on the dashboards)

- **Out-of-stock flag.** `oos_at_write` is the stock when the minute was written. It is not replay-safe, a stock-out that starts mid-minute credits the whole minute, and a late event is judged against the stock at write time (section 7). Exposed spend, `oos_demand_daily` and the D-1 marts inherit this.
- **Two day definitions.** All dates, including the ad report day, are Cairo days (section 9).
- **Exposed spend is an estimate.** It assumes spend is spread evenly over served landing clicks; Assiut and "Other" are not served and are excluded from both counts.
- **Express and Gold.** Express KPIs exist only in `express_daily` and `live_express_orders`; the Gold delivery metrics include express orders together with standard ones, and there is no Gold path from a sale to a store.
- **No stock reversal.** The model is reserve-on-order: stock is not put back when an order is cancelled, returned or fails delivery, so stock drifts down with every cancellation.
- **Store is conformed across the Postgres marts only.** Gold has City (`fact_orders.city`), not Store; "orders and stock for the same store" is answered in Postgres only.
- **Campaign key.** Traffic without a campaign is stored as `campaign_id = 'direct'`, which the Power BI Campaign dimension has no row for (it shows as blank).
