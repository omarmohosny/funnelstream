# Power BI model and pages (version 2: campaigns, funnel and inventory)

This folder holds what is needed to build the FunnelStream report in Power BI Desktop: the measures (`measures.dax`) and this guide. The Gold part of the model (seven Databricks tables, six relationships, 49 measures, three empty pages) is generated as a Power BI project (the 49 measures are the Gold part only; the full `measures.dax` has 84, the other 35 being the inventory, marketing and express measures): open `FunnelStream.pbip` in Power BI Desktop (File, Options, Preview features: enable "Power BI Project (.pbip) save option" if needed), sign in to Databricks, Refresh, and build the visuals. `scripts/make_pbip.py` regenerates the project from `samples/` and `measures.dax`. The Postgres and inventory tables are added after the inventory pipeline exists.

The report serves two end users with one model:

- **Operations** (a quick-commerce style app): where stock stands, which products ran out, how much demand met an empty shelf.
- **Marketing**: yesterday's campaigns and sales, including the estimated advertising spend exposed to out-of-stock products.

Inventory is simulated (`docs/superpowers/specs/2026-10-10-inventory-two-users-design.md`) and every inventory visual says so.

## 1. Sources and tables

Two sources, both Import mode with read-only credentials. The write token of the load jobs never goes into a Power BI file.

**Databricks** (connector "Databricks", catalog `funnelstream`, schema `gold`):

| Gold table | Power BI name | Notes |
|---|---|---|
| `dim_date` | `Date` | Mark as date table on `date`. Keep `date_key` |
| `dim_campaign` | `Campaign` | Includes the unknown member |
| `dim_product` | `Product` | SCD2: in Power Query keep only current rows (`valid_to` is the end-of-time date) so `product_id` is unique, and keep `product_id`, `name`, `category`, `price` |
| `agg_campaign_daily` | `Campaign Daily` | Keep only `attribution_model = "last_touch"` |
| `agg_funnel_daily` | `Funnel Daily` | |
| `agg_orders_daily` | `Orders Daily` | Every order, attributed or not |
| `fact_orders` | `Orders` | For drill-through and the Delivery page |

**Postgres** (connector "PostgreSQL database", database `funnelstream`, read-only role `grafana_ro`): the inventory and marketing marts that the live sink and the daily job write (inventory plan, Tasks 7, 9A and 9B). Grafana reads the same tables, so both tools show the same numbers. There are eight Postgres tables in all: the seven in the table below, which Power BI loads, plus `dim_product_current(product_id, name, category, price)`, which only Grafana uses to show product names (Power BI takes names from the Gold `Product`).

| Postgres table | Power BI name | Grain | Refreshed by |
|---|---|---|---|
| `dim_store` | `Store` | one row per dark store (8) | `apply_live_ddl.py` (the same job fills `dim_product_current`) |
| `live_stock_now` | `Stock Now` | store and product, current | live sink, every Silver batch |
| `inventory_daily` | `Inventory Daily` | store, product, Cairo day | `build_inventory_daily.py` (rebuilds the last 2 days by default) |
| `oos_demand_daily` | `OOS Demand Daily` | Cairo day, store, product, campaign (`campaign_id`); `served_clicks` (all) and `oos_clicks` (flagged) | `build_inventory_daily.py` |
| `express_daily` | `Express Daily` | store and day: express orders and the 20-minute promise | `build_inventory_daily.py` |
| `marketing_d1_campaign` | `Marketing D1 Campaign` | campaign and ad day (`served_clicks`, `oos_clicks`) | `build_d1_marketing.py` |
| `marketing_d1_product` | `Marketing D1 Product` | store, product and day: every out-of-stock product (Top N is done by the visual) | `build_d1_marketing.py` |

Do not load `dim_customer` until the Customer 360 page.

## 2. Relationships (star schema)

Four dimensions: `Date`, `Campaign`, `Store`, `Product`. Every relationship is many-to-one from a fact table to a dimension, single direction, active. No many-to-many, no bidirectional filter.

| From (many) | To (one) |
|---|---|
| `Campaign Daily[report_date]`, `Funnel Daily[event_date]`, `Inventory Daily[report_date]`, `OOS Demand Daily[report_date]`, `Marketing D1 Campaign[report_date]`, `Marketing D1 Product[report_date]`, `Express Daily[report_date]` | `Date[date]` |
| `Orders Daily[date_key]`, `Orders[date_key]` | `Date[date_key]` |
| `Campaign Daily[campaign_id]`, `Marketing D1 Campaign[campaign_id]`, `OOS Demand Daily[campaign_id]`, `Orders[campaign_id]` | `Campaign[campaign_id]` |
| `Stock Now[store_id]`, `Inventory Daily[store_id]`, `OOS Demand Daily[store_id]`, `Express Daily[store_id]`, `Marketing D1 Product[store_id]` | `Store[store_id]` |
| `Stock Now[product_id]`, `Inventory Daily[product_id]`, `OOS Demand Daily[product_id]`, `Marketing D1 Product[product_id]` | `Product[product_id]` |

`OOS Demand Daily` relates to `Campaign` on `campaign_id` only; `utm_campaign` is a plain attribute column there and no relationship uses it. Traffic without a campaign has `campaign_id = 'direct'`, which `Campaign` has no row for, so it shows as (Blank) in a campaign slicer. `Orders Daily` and `Funnel Daily` have no campaign, store or product key, so they filter by `Date` only (plus their own channel and platform columns). A visual must never mix measures from tables that share no dimension on the page.

Hide: every `*_key` column and every `*_id` column except those used in relationships' dimension tables, and all pre-computed rate columns (`ctr`, `conversion_rate`, `cpa`, `roas_*`, `cancel_rate`, `failed_attempt_rate`, `return_rate`, `open_rate`, `aov`, `cart_abandon_rate`, `checkout_abandon_rate`, `oos_click_share`). The measures replace them: a rate column cannot be summed or averaged correctly.

## 3. Model review (against the Power BI design checklist)

| Check | Result |
|---|---|
| Star schema | Yes: four dimensions, ten fact-like tables of five different grains, no snowflake |
| Grain | Campaign and day; day, country, channel, platform; day, channel, payment method, platform; store, product and day; store and product (current). A measure may combine two tables only through a shared dimension |
| Cardinality and direction | All many-to-one, single direction |
| Storage mode | Import everywhere: a few thousand to a few hundred thousand rows. `Inventory Daily` is the largest: 8 stores x about 190 products x days, so about 1,500 rows a day |
| Measures | Explicit; all ratios from sums; `DIVIDE` everywhere; semi-additive stock handled by measure (see below) |
| Documentation | Every measure has a comment line; tables are renamed to business names |

Findings that shaped the model:

1. **Spend exists on few days.** The ad API is pulled once a day, so `spend` is filled only on pulled days. ROAS and CPA use the "on Spend Days" measures, which count revenue and orders only on rows that carry spend. Without that, ROAS would divide all revenue by two days of spend.
2. **The newest days are incomplete.** Delivered revenue is counted on the order date and orders settle for 2 to 4 days. `Is Provisional` marks days within three days of today and the Campaign page prints the maturity label next to every headline number.
3. **Stock is semi-additive.** Adding closing stock over days is meaningless. `Closing Stock` takes the last report date inside the filter context; flow measures (`Units Sold`, `Units Received`) add normally.
4. **Sessions do not add across days.** A session that spans midnight counts once on each Cairo day.
5. **Two click counts exist.** `Ad Clicks` are API-reported (CTR); attribution uses the session click. The page labels both.
6. **Exposed spend is an estimate.** `Exposed Spend = spend x oos_clicks / served_clicks`, both counts from the same table (`oos_demand_daily` / `live_oos_demand_minute`), served districts only, the same landing-click definition on both sides. Spend is known per campaign and day only, so the estimate assumes spend is spread evenly over served landing clicks. The title says "estimated spend exposed to out-of-stock", never "wasted", and nothing calls it an upper bound.
7. **Out-of-stock means the stock when the minute was written.** The live sink stores all served demand per minute with the flag `oos_at_write` (stock was zero when it wrote the minute, accurate to about one Silver batch). The daily marts keep that decision. Limitation: the flag is not replay-safe; a replay or a late event can change it, and a stock-out that starts mid-minute credits the whole minute.
8. **Not served.** Assiut and "Other" have no store; their demand is excluded from every inventory measure and the report says so.
9. **Attribution.** Last touch favours bottom-funnel channels; nothing here proves incrementality.
10. **The 20-minute promise.** Every express order carries its own promise (`promised_minutes`, 20 by default); the on-time test always uses the order's promise, never a constant. A breach is a delivery later than the promise, or an undelivered order older than the promise plus a one-minute grace; orders whose latest status is cancelled or returned are not counted as undelivered past the promise. The SLA clock compares the `order_created` time with the `delivered` event time, both simulator event times. The report must show zero breaches as 0, not blank, so `Express Breaches` returns 0 when there are none.
11. **Day rule.** Ad spend and ad clicks are the ad API's UTC day; orders and demand are the Cairo day; `Campaign Daily[report_date]` is the ad day and its order columns are the orders of the Cairo date with the same date value. The mismatch is at most 3 hours and is accepted. The D-1 mart takes yesterday in Cairo for the demand and the ad row with the same date value.
12. **ROAS is a period ratio.** A day's ROAS divides that day's spend by revenue credited to that order date, whose clicks can be up to 7 days older; read it over a week or a campaign flight (the Campaign page defaults to a weekly granularity), not as a per-day truth.
13. **Known limitations.** Express KPIs exist only in `Express Daily` (and the live table); the Gold delivery measures include express orders. Stock is not put back when an order is cancelled (reserve-on-order). `Store` is conformed across the Postgres marts only: Gold has City, not Store.

## 4. Pages

Canvas 16:9 (1280 x 720). Read order: filters, headline numbers, the main chart, then detail. One hue for magnitude, a second only for "delivered", red or orange only for problems, never red and green alone. Each visual says what it shows and the unit (EGP, units).

### Page 1: Executive
- Slicers: date range, channel, platform.
- Cards: `Revenue Delivered`, `Revenue Placed`, `AOV`, `Delivered Share of Revenue`, `Orders Placed`, `Cancel Rate`.
- Line: `Revenue Placed` and `Revenue Delivered` by day, with `Revenue Placed vs Last Week`.
- Bars: `Orders Placed` by channel. Table: outcome rates by payment method.

### Page 2: Campaign (the headline page)
- Slicers: date range, campaign, platform.
- Side by side: `ROAS Placed` and `ROAS Delivered` per campaign (same axis) and the `ROAS Gap` card.
- Cards: `Spend`, `CPA`, `Cost per Delivered Order`, `CTR`, `Conversion Rate`.
- Columns: `Platform Conversions` next to `Orders on Spend Days`.
- Maturity label in the header; footnotes for lag, the two click counts and last touch.

### Page 3: Funnel
- Slicers: date range, channel, platform.
- Funnel chart over `Sessions`, `Product View Sessions`, `Cart Sessions`, `Checkout Sessions`, `Funnel Orders`; cards `Cart Abandon Rate`, `Checkout Abandon Rate`, `Session Conversion`; bars by channel and platform; footnote on sessions.

### Page 4: Inventory (operations)
- Slicers: date range, store, category.
- Cards: `Out of Stock Lines`, `Low Stock Lines`, `Out of Stock Rate`, `Stock Units Available` (current, from `Stock Now`), and `Stock Freshness`.
- Line: `Closing Stock` by day and store. Columns: `Units Sold` against `Units Received` by day.
- Table: products that `Ended Out of Stock`, with `Days of Cover` and `Oversold Units`.
- Delivery promise row (the live-inventory section is an express service with a 20-minute promise): cards `Express Orders`, `Express On-time Rate`, `Express Breaches`, `Slowest Express Delivery (min)`; line `Slowest Express Delivery (min)` by day against the promise line; bars `Express On-time Rate` by store. Any breach turns the card red.
- Bars: `Shrinkage Rate` by store. Footnote: simulated stock; reservations out of scope; Assiut and "Other" not served.

### Page 5: Marketing and inventory (the bridge)
- Slicers: date range, campaign.
- Cards: `Exposed Spend`, `OOS Click Share` (with `Served Clicks` as its denominator), `Exposed Share of Spend`, `Revenue at Risk`, `OOS Add to Cart`.
- Bars: `Exposed Spend` per campaign next to `Spend` (same axis). Table: products by `Revenue at Risk` with their store and `Days of Cover`, with a Top N = 5 filter set in the visual (`Marketing D1 Product` keeps every out-of-stock product, related to `Store` and `Product`).
- Line: `OOS Add to Cart` per day, with the days the hero products ran out visible.
- Footnote: the metric is an estimate that assumes spend is spread evenly over served landing clicks; out-of-stock is the stock when the minute was written; spend is the ad UTC day and demand the Cairo day (at most 3 hours apart); recent days are provisional.

Pages 6 to 8 (Delivery and COD, City and product, Customer 360) follow the same pattern after these five.

## 5. Refresh and the quality gate

Refresh after the batch, never before. The batch (`scripts/run_batch.py`) ends with the data quality checks and prints `BATCH OK` or `BATCH FAILED at <phase>`; a failed check means Gold may be wrong, so the refresh must not run. The Postgres marts are rebuilt by two daily jobs: `scripts/build_inventory_daily.py` writes `inventory_daily`, `oos_demand_daily` and `express_daily` (it rebuilds the last 2 Cairo days by default) and `scripts/build_d1_marketing.py` writes `marketing_d1_campaign` and `marketing_d1_product`; refresh Power BI after both succeed.

## 6. How to finish this in Power BI Desktop (about 2 hours)

1. Get data from Databricks (seven tables, section 1), rename them, keep only `last_touch` in `Campaign Daily` and current rows in `Product`.
2. Get data from PostgreSQL (seven tables, section 1; `dim_product_current` is not loaded) and rename them.
3. Model view: create the relationships (section 2), mark `Date` as a date table, hide the columns listed there.
4. Create the empty table `_Measures` and add every measure of `measures.dax` (display folders as named in the comments).
5. Build the five pages (section 4). Save as `dashboards/powerbi/funnelstream.pbix` and export one PNG per page into `dashboards/powerbi/screenshots/`.
6. Check: no error icons; `ROAS Delivered` is blank (not an error) on a campaign with no spend; `Closing Stock` for a date range equals the last day's total, not a sum over days; the Marketing and inventory page shows the estimate wording.

The inventory pages (4 and 5) can be built only after the inventory work has produced data (inventory plan, Tasks 5 to 9); until then the model loads without them.
