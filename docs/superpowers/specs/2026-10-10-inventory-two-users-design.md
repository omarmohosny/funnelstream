# FunnelStream: live inventory for operations and D-1 marketing (design)

Date: 10 October 2026. Status: draft for review, not committed. Defense: 13 October 2026.

## 1. Purpose

FunnelStream becomes one platform for two end users.

1. **Operations user**, as in a quick-commerce app such as Breadfast or noon Minutes. This person watches, in Grafana and in real time, the traffic on the app and the real inventory of the dark stores: what is in stock, what is out of stock, and how much customer demand is hitting empty shelves.
2. **Marketing user.** This person reads, each morning, the report for day minus one: yesterday's campaign details and yesterday's sales performance, including how much of the advertising spend sent customers to products that were out of stock.

The story that only a joined platform can tell, and the main point of the defense, is the bridge between the two: **advertising spend exposed to out-of-stock products**. Operations sees it as lost demand now. Marketing sees it the next morning as estimated exposed spend.

Success criteria:

- Inventory events flow through the same Kafka, Bronze, Silver and live-table path as orders, with the same guarantees (checkpoints, idempotent MERGE, quarantine, replay after a crash).
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
          --> live sink --> Postgres live_stock_now, live_oos_demand_minute
          --> Grafana "Operations: live inventory"

daily job: Databricks agg_campaign_daily (D-1 spend, ROAS, CPA, orders)
         + local Silver (out-of-stock clicks and add-to-carts, hourly stock state)
         --> Postgres marketing_d1_campaign --> Grafana "Marketing D-1"
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
- Both event ids are deterministic (derived from store, product, kind and time), so a replay produces the same key and the MERGE inserts nothing twice.

Definitions: `available = on_hand - reserved_qty`. A product is **out of stock** when `available <= 0` and **low stock** when `0 < available < reorder_point`.

## 5. Stores and the district map

One source: `contracts/stores.py`, built on `contracts/geo.py`. Eight dark stores: Cairo 3 (Nasr City, Maadi, New Cairo), Giza 1, Alexandria 2, Mansoura 1, Tanta 1. Each district has exactly one primary store. Assiut and "Other" have no store; they are "not served" and are excluded from out-of-stock metrics. A real system uses delivery zones, and the defense says so.

The map is written to Postgres as `dim_store` and `dim_store_district` from this one source. Demand events carry a city and district but no store, so the join to stock is done in SQL only. The mapping is never reimplemented in Spark or Databricks SQL.

## 6. Simulator stock model

Deterministic from the run seed and the existing seeded generator; no other random source.

- A `StockLedger` holds stock per store and product and never lets it go below zero.
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
- `live_stock_now(store_id, product_id, on_hand, reserved_qty, available, reorder_point, updated_at)`, primary key `(store_id, product_id)`.
- `live_oos_demand_minute(minute_start, store_id, product_id, views, add_to_cart)`, primary key on the three keys.

How the sink fills them, after each Silver batch, only for the `(store, product)` pairs and minutes the batch touched:

- Stock now is a full recompute: the latest snapshot at or before the cut plus the sum of the movements after it, ties broken by `event_id`. It does not depend on arrival order, so late or replayed events cannot corrupt it, and the failure demo still holds.
- Out-of-stock demand counts `product_view` and `add_to_cart` events of the touched minutes whose store (from the district) and product have `available <= 0` in the current stock. This is accurate to about one batch, and the panel says so.

## 8. Grafana: Operations, live inventory

Seven panels, top row first: out-of-stock products now, low-stock products, "stock as of N seconds ago"; out-of-stock demand per minute (the bridge panel) and live traffic; a store by top-twenty-products stock heatmap; a "lost demand now" table (top products by add-to-carts on zero stock in the last 15 minutes).

One new alert, `fs-oos-demand`: at least 5 add-to-carts on zero stock for the same store and product in each of two consecutive minutes (the hero scenario produces about 1 per second, so it fires; normal traffic does not). The existing alerts (heartbeat silent, no events, quarantine, late events) stay.

## 9. Marketing D-1

A daily job `scripts/build_d1_marketing.py` for a given report date (default yesterday, UTC day, as in the ad report):

1. Read that day's spend, impressions, clicks, orders placed, revenue placed and delivered, CPA, ROAS placed and ROAS delivered per campaign from the Databricks `agg_campaign_daily` (`attribution_model = 'last_touch'`).
2. From local Silver, count per campaign the clicks (sessions landing on a product page with a campaign) and how many of them were on a product with `available <= 0` at that time (hourly stock state from the movements), plus add-to-carts on zero stock and their value.
3. Write `marketing_d1_campaign` to Postgres.

Definitions shown on the page:

- `oos_click_share = clicks on out-of-stock products / all clicks`.
- `exposed_spend = spend * oos_click_share`. Spend is allocated per click, because the ad API gives spend per campaign per day only. The metric is titled "estimated spend exposed to out-of-stock". It is an estimate and an upper bound, since a customer may buy a substitute.
- `revenue_at_risk = add-to-carts on out-of-stock products * price`.

The Grafana page "Marketing D-1" shows nine numbers (spend, clicks, orders placed, CPA, ROAS placed, ROAS delivered, out-of-stock click share, exposed spend, out-of-stock add-to-carts), a table per campaign sorted by exposed spend, and the top five products by revenue at risk. Recent days are marked provisional for ROAS delivered.

If time remains: publish the stock tables to the Databricks mirror and move step 2 into a Gold SQL step `agg_oos_demand_daily`.

## 10. Testing

Test first for every step. Host tests cover the contract, validator, transform, ledger and store map. Spark tests (in Docker, per file) cover Silver and the live sink, including replay, a crash in the middle of a batch, late and duplicate movements, and "stock never negative". Provisioning tests check the Grafana JSON and alert rules. The existing 63 data quality checks, the duplicate check and the failure demo must still pass.

## 11. Order of work and honest risks

Order: finish and commit the Silver speed fix; pass the failure demo (G3) twice; then the inventory steps in this order: contract and validator, simulator ledger and scenario, topic and Bronze and Silver tables, live sink and DDL, Grafana operations dashboard and alert; then the D-1 job and page; then the T17 samples, README and a 1 million event benchmark.

Risks: the defense is on 13 October and G3, the Silver fix commit and T17 are still open, so the D-1 page and the Databricks Gold step are the first to move to "future work". A scripted stock-out can look circular; the answer is to show that every number is computed by the pipeline from raw events, and to say clearly that the inventory is simulated.

## 12. Assumptions to confirm

- Out of stock means `available <= 0`; low stock means below the reorder point.
- The D-1 mart may use hourly stock state, not exact per-event stock.
- An unmapped or "unknown" district is not counted as out-of-stock demand.
