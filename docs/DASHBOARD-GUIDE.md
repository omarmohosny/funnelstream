# FunnelStream dashboard guide: every Grafana panel and every Power BI visual

This guide explains each chart, number and table on the six Grafana dashboards and the seven Power BI pages. For each one it says what it shows, how it is calculated, and how to read it. It is based on the dashboard definitions in the repository (`monitoring/grafana/dashboards/*.json`, `scripts/make_pbip.py`, `dashboards/powerbi/measures.dax`) and on the screenshots in `docs/images`.

All numbers in the screenshots come from simulated data. The Grafana screenshots were taken during the rehearsal of 13 October 2026; the Power BI screenshots are from the redesigned report.

---

## 1. How the two tools fit together

| | Grafana | Power BI |
|---|---|---|
| Audience | Engineers and the operations user | The marketing user and managers |
| Question | Is the platform healthy, and what needs action now? | What happened, and what should we decide? |
| Data source | Small Postgres tables written by the live sink after each Silver batch, and the daily marts | Gold tables in Databricks (11) and Postgres marts (7), imported |
| Freshness | Refreshes every 5 seconds (live dashboards) or every minute (daily dashboards) | Refreshed by hand after a Gold batch |
| Alerts | 8 alert rules | None |

**Colour conventions in Grafana.** Stat tiles and gauges change colour by thresholds. Green means healthy, orange means watch, red means act. Blue tiles are plain counts with no good or bad. A neutral dash means there is no data. On rate charts, a gap means no data in that minute, not zero.

**One day definition.** Every date is a Cairo day: ad spend, ad clicks, orders, stock and demand.

**What "out of stock when the minute was written" means.** Demand on empty shelves is flagged with the stock level at the moment the live sink wrote that minute, not at the event time. A replay or a late event can flag it differently. It is an estimate.

---

## 2. Grafana

Six dashboards are provisioned as code. Each panel below is listed in the order it appears on screen.

### 2.1 FunnelStream: live pipeline (`fs-live`)

**Purpose.** Shows whether the streaming pipeline is alive and clean. Refresh 5 seconds, last 30 minutes.

![Live pipeline](images/grafana/pipeline_live.png)

*Captured at the end of the 13 October rehearsal. The generator had just stopped, so events per minute is 0.*

| Panel | What it shows and how it is calculated | How to read it |
|---|---|---|
| Events, last full minute (stat) | Non-late, non-bot events in the last completed minute, all countries. | Red at 0, green at 1 or more. Zero means the live line is flat: the stream stopped or the Silver job is not writing. |
| Events per minute, all countries (time series) | The same events per minute from `live_metrics_minute`, summed over countries. | The stream's heartbeat. A smooth plateau is healthy traffic. A drop to zero means the stream stopped. In the screenshot the line climbs to about 3,000 per minute and falls to zero when the generator ended. |
| Active sessions per minute | Distinct sessions that had an event in the minute. | How many shoppers are present. It should follow the events line. |
| Orders placed per minute | `order_created` rows per minute. | Conversion made visible: orders are far fewer than sessions and rise and fall with traffic. |
| Product views and add-to-cart per minute | Two lines: product view events and add-to-cart events per minute. | The top of the funnel in real time. The gap between the lines is shoppers who looked but did not add to the cart. |
| Late-event rate, last 5 min (gauge) | Late events divided by all events over the last 5 minutes, as a percentage. | Green below 5%, red at 5% or more. An event is late when it reaches Kafka more than 10 minutes after its event time. The alert fires above 5%. In the screenshot it reads 2.5%. |
| Late-event rate %, 5 min window (time series) | The same late rate on a sliding 5-minute window over time, with a red line at the 5% threshold. | Shows whether late data is a spike or a steady level. Read it only in live runs: history replays are late by rule. |
| Quarantine share, last 5 min (gauge) | Quarantined rows divided by events plus quarantined rows over the last 5 minutes. | Green below 2%, red at 2% or more. This is the share of messages the validator rejected. The alert fires above 2%. It reads 1.4% in the screenshot. The simulator injects bad rows on purpose, so a small steady share is expected. |
| Quarantine share %, 5 min window (time series) | The same share on a sliding window, with a red line at 2%. | A jump above the injected noise level means a producer started sending bad data. |
| Quarantine rows per hour, rolling 60 min | Rows written to `silver_quarantine` in the trailing 60 minutes, evaluated every minute. | The volume of rejected rows. It climbs steadily during a run because the noise is constant. Group `silver_quarantine` by reason to see what is being rejected. |
| Seconds since Silver job last reported (stat) | Now minus the `silver_job` heartbeat. The job reports every 15 seconds while alive and after each batch. | Green under 30 s, orange 30 to 120 s, red above 120 s. This is time since the job last reported, not since data arrived. It is the number that drives the "Silver job silent" alert. In the screenshot it is 7 s. |
| Seconds since last Gold update | Now minus the `gold_build` heartbeat. | Green under 24 hours, orange 24 to 48 hours, red above 48 hours. The batch lane runs on demand, so this ages between runs. It shows "1 days" in the screenshot. |
| Seconds since last publish | Now minus the `publish` heartbeat (the Silver upload to Databricks). | The same thresholds. It tells you how stale the Databricks mirror, and therefore Power BI, is. |
| Alert rules and what to do (text) | A table of the eight alert rules, when each fires and what to do. | The runbook, on the same screen as the numbers. |

### 2.2 FunnelStream: Operations, live inventory (`fs-inventory-ops`)

**Purpose.** The operations user's page: stock now, demand that met an empty shelf, and the express delivery promise. Refresh 5 seconds, last 30 minutes.

![Operations live inventory](images/grafana/operations_live_inventory.png)

*The express section shows breaches because the rehearsal used a 2-minute promise and delivered some orders late on purpose.*

| Panel | What it shows and how it is calculated | How to read it |
|---|---|---|
| Out-of-stock products now (stat) | Number of store and product pairs in `live_stock_now` with `available <= 0`. | Green at 0, orange from 1, red from 5. Each one is a shelf with nothing to sell. |
| Low-stock products now | Pairs with stock above zero but below their reorder point. | Green below 10, orange from 10, red from 30. The warning before a stock-out. |
| Seconds since the last stock change | Age of the newest row in `live_stock_now`. Stock is recomputed after each Silver batch, so it is about one batch behind. | Green under 30 s, orange to 60 s, red above. A high number means stock is no longer updating. |
| About this data (text) | States that all stock numbers are simulated and that demand flags are estimates. | Read before quoting numbers. |
| Demand on out-of-stock products per minute (time series) | Product views and add-to-carts that arrived for a store and product with zero stock when the minute was written. | This is the bridge panel. A spike means shoppers are asking for something the shelf cannot serve. The "Demand on out-of-stock products" alert is built on it. |
| App traffic: events per minute | Non-late, non-bot events per minute, all countries. | Context: demand on empty shelves matters more when traffic is high. |
| Lowest stock now, 20 products (table) | The twenty store and product pairs with the least available stock, with their reorder point. | Where to restock first. A row at 0 is out of stock. |
| Lost demand now, last 15 minutes (table) | Products with the most add-to-carts while out of stock in the last 15 minutes, with views. | Which empty shelf is costing the most shoppers right now. |
| Express orders at risk now (stat) | Undelivered express orders that used 75% of their promise and are not yet breached. | Green at 0, orange from 1, red from 3. Act on these orders before they breach. |
| Express breaches, last hour | Orders created in the last hour that were delivered late, or are overdue by the promise plus 1 minute of grace. | Green at 0, red from 1. Any breach is a missed promise. |
| On-time rate, last hour | On-time orders divided by delivered or breached orders created in the last hour. Cancelled or returned orders with no delivery are in neither count. | Red below 90%, orange 90 to 95%, green at 95% or more. The target is 95%. |
| Slowest express delivery, last hour (minutes) | The longest elapsed time: delivery time for delivered orders, age for open orders. | Green under 15 min, orange 15 to 20, red above 20 against a 20-minute promise. |
| Slowest express orders, last hour (table) | The ten express orders with the longest elapsed time, with promise, elapsed minutes and a status. | Names the exact orders that are late or closest to late. Closed orders sort last. |
| Express definitions (text) | Defines on time, breached, at risk and closed without delivery. | The meaning of every express number on the page. |

### 2.3 FunnelStream: Express delivery SLA (`fs-express-sla`)

**Purpose.** Dedicated to the 20-minute promise. It has a Store filter. Refresh 5 seconds, last 3 hours.

![Express SLA](images/grafana/express_sla.png)

*Rehearsal of 13 October 2026 with the promise shortened to 2 minutes. The dashed line in the first chart is the 95% target.*

| Panel | What it shows and how it is calculated | How to read it |
|---|---|---|
| On-time rate, last hour (stat) | On-time orders divided by delivered or breached orders created in the last hour. | Red below 90%, orange 90 to 95%, green at 95% or more. The screenshot shows 94.0%, just under target, so it is orange. |
| Breaches, last hour | Orders delivered late or overdue (promise plus 1 minute of grace). | Green at 0, red from 1. The screenshot shows 6. |
| At risk now | Undelivered orders that used 75% of their promise and are not yet breached. | Green at 0, orange from 1, red from 3. |
| Open express orders | Created, not delivered, not cancelled or returned, in the last 3 hours. | Blue, no good or bad. The current queue. |
| Median delivery time, last hour (min) | The p50 of delivered orders created in the last hour. | Green under 15 min, orange to 20, red above. Half the orders were faster than this. |
| On-time rate by order creation time, 5-minute buckets (time series) | The share of resolved orders delivered inside their promise, by the time the order was created, with a dashed 95% target line. | Shows when service dipped. Buckets with no orders are gaps, not 0%. |
| On-time and breached orders per 5 minutes | Two lines: orders delivered on time and orders breached, by creation time. | Shows the volume behind the rate: a low rate on 2 orders means little, on 30 means a lot. |
| Delivery time p50 and p90 (minutes) | Median and 90th percentile delivery time of delivered orders by creation time. | Compare both lines with the promise. The p90 is what the slowest tenth of customers experienced. |
| SLA by store, last hour (table) | Per dark store: orders, on-time rate, breaches, at risk and p90 delivery time, worst first. | Finds the store that is failing. In the screenshot Nasr City has 88 orders and 6 breaches, and the other stores have none. |
| Open orders closest to their promise (table) | Undelivered orders, oldest first, with promise, age and minutes left. | A negative "minutes left" means the order is already overdue. Chase these first. |
| Express definitions (text) | On time, breached, at risk and closed without delivery. | Defines every number on the page. |

### 2.4 FunnelStream: Live stock monitor (`fs-live-stock`)

**Purpose.** Stock health of the eight dark stores, live. It has a Store filter. Refresh 5 seconds, last hour.

![Live stock monitor](images/grafana/live_stock_monitor.png)

*Rehearsal of 13 October 2026. All stock numbers are simulated.*

| Panel | What it shows and how it is calculated | How to read it |
|---|---|---|
| Out-of-stock products (stat) | Pairs with no available stock in the selected stores. | Green at 0, orange from 1, red from 5. |
| Low-stock products | Stock above zero but below the reorder point. | Green below 10, orange from 10, red from 30. |
| In-stock rate | The share of tracked products with available stock. | Red below 90%, orange 90 to 97%, green at 97% or more. The screenshot shows 100%. |
| Units on hand | Total on-hand units in the selected stores. | Blue, a size indicator. The screenshot shows 537,542 units. |
| Seconds since the last stock write | Age of the newest stock row, about one Silver batch behind. | Green under 30 s, orange to 60 s, red above. |
| Out-of-stock products per store (time series) | Out-of-stock pairs at each minute from the per-minute snapshots kept 24 hours. | Shows when and where a stock-out began and ended. The screenshot has two short spikes at Nasr City. |
| Units on hand per store | On-hand units per store at each minute. | Falls with sales and jumps with restocks. A line that only falls means no restock. |
| Units added and removed per minute | Change in on-hand units between minute snapshots: added is restock, removed is sales and shrinkage. | Restock waves and sales pressure side by side. |
| Demand on out-of-stock products per minute | Views and add-to-carts on pairs with zero stock when the minute was written. | Demand that the shelf could not serve. |
| Stock health by store (table) | Per store: tracked products, out of stock, low, in-stock percentage and units, worst first. | One row per dark store: the quick comparison. |
| Restock watchlist (table) | The 20 pairs furthest below their reorder point, with a status: out of stock, restock now or ok. | The to-do list for the stock team. In the screenshot "Bed sheets 20" at Nasr City has 1 unit against a reorder point of 3. |
| About this data (text) | States that history comes from per-minute snapshots, so a minute with no sink batch has no point. | Explains gaps in the lines. |

### 2.5 FunnelStream: Inventory insights, daily (`fs-inventory-daily`)

**Purpose.** The daily view of stock and the express promise, built from the daily marts. Refresh 1 minute, last 14 days.

![Inventory insights, daily](images/grafana/inventory_daily.png)

| Panel | What it shows and how it is calculated | How to read it |
|---|---|---|
| Closing stock, latest day (units) | Units on hand at the end of the latest Cairo day in `inventory_daily`. | Blue, a level. Stock is simulated and never below zero. |
| Products out of stock at day end | Store and product pairs whose stock was zero or less at the end of the latest day. | Green at 0, orange from 1, red from 5. |
| Oversold units, latest day | Units asked for beyond the shelf. A lower bound, because a sale of zero units emits no movement. | Green at 0, orange from 1, red from 20. It means the app promised stock it did not have. |
| Shrinkage share of units out, latest day | Shrunk units divided by shrunk plus sold units. | Green below 3%, orange 3 to 6%, red above 6%. Shrinkage is stock lost without a sale. |
| Closing stock by store (units) | Stock at the end of each Cairo day, summed per dark store. | Each store's stock trend over two weeks. |
| Units sold, received and shrunk per day | Daily movement totals over all stores and products, three lines. | Compare sold with received: if sales exceed receipts, stock is draining. |
| Lowest days of cover, latest day, 20 products (table) | Closing stock divided by the average units sold per day over the last 7 days. Products that did not sell are left out. | A low number means the product runs out soon at the current sales pace. |
| Demand on empty shelves per day | Product views and add-to-carts that arrived while the product was out of stock, per day. | The daily size of lost demand. |
| Express on-time rate, latest day | On-time orders divided by delivered or breached orders on the latest day in `express_daily`. Shows 100 when there is nothing to measure. | Red below 90%, orange 90 to 95%, green at 95% or more. |
| Slowest express delivery per day (minutes) | The longest delivery time of each Cairo day, against a flat line at the 20-minute promise. | A point above the promise line is a day with a missed promise. |
| About this data (text) | Notes that stock is simulated, oversold units are a lower bound and cancelled orders do not return stock. | The limits of the numbers above. |

### 2.6 FunnelStream: Marketing, D-1 (`fs-marketing-d1`)

**Purpose.** Yesterday's campaign results next to the estimated spend that was sent to out-of-stock products. Refresh 1 minute, last 14 days. It reads the `marketing_d1_campaign` and `marketing_d1_product` marts.

![Marketing D-1](images/grafana/marketing_d1.png)

| Panel | What it shows and how it is calculated | How to read it |
|---|---|---|
| Spend (EGP) | Ad spend of the latest D-1 day, all campaigns. This is the Cairo day. | Blue, the budget used. |
| Orders placed | Orders attributed to the campaigns by last touch on the latest D-1 day. | Blue. |
| ROAS placed | Placed revenue divided by spend. | Blue. It is the optimistic return, because cancelled and failed orders are still counted. |
| ROAS delivered (provisional) | Delivered revenue divided by spend. | Blue. The headline return. Delivery matures one to three days after the order day, so recent days read low. |
| Exposed spend (EGP) | Spend multiplied by the share of landing clicks that went to out-of-stock products. | Green at 0, orange from 1, red from 1,000. An estimate that assumes spend is spread evenly over served landing clicks. |
| Out-of-stock click share | Landing clicks in minutes flagged out of stock, divided by all landing clicks in served districts. | Green below 5%, orange 5 to 15%, red from 15%. |
| Campaigns, sorted by exposed spend (table) | One row per campaign with spend, orders, CPA, ROAS placed and delivered, served and out-of-stock clicks, the click share, exposed spend and a provisional flag. | Which campaign sent the most shoppers to empty shelves. Provisional days can still change. |
| Top 5 products by revenue at risk (table) | Out-of-stock add-to-carts multiplied by price, by store and product, on the latest D-1 day. | Which products to restock first, or to stop advertising. The mart keeps every out-of-stock product. The limit of five is this panel's. |
| About this data (text) | Defines exposed spend and notes the Cairo day and the provisional days. | Read before quoting the exposed spend. |

### 2.7 The eight alert rules

Alerts notify on a change of state, to a local alert sink.

| Rule | Fires when | What it means |
|---|---|---|
| Late-event rate above 5% | Late events exceed 5% over 5 minutes, with at least 20 events. | Clock skew or old events were replayed. |
| Quarantine share above 2% | Quarantined rows exceed 2% over 5 minutes, with at least 20 rows. | A producer is sending bad data. |
| Silver job silent for 2 minutes | The Silver heartbeat is older than 2 minutes (no data also fires). | The streaming job is down or stuck. |
| No events for 5 minutes | No Silver event arrived in the last 5 minutes (no data also fires). | The producers or the job stopped. |
| Demand on out-of-stock products | At least 5 add-to-carts on an out-of-stock product of one store in each of two consecutive minutes. | Shoppers are hitting an empty shelf. |
| An express order missed its delivery promise | Any breach. | A customer waited past the promise. |
| Express on-time rate below the 95% target | On-time rate under 95% over 30 minutes with at least 5 orders, for 2 minutes. | The service level is slipping. |
| Many products out of stock in one store | More than 20% of a store's products are out of stock, for 1 minute. | One dark store is running empty. |

---

## 3. Power BI

The report has seven pages. On every page, the navy rail on the left holds the page navigator and the slicers. The banner states the question the page answers. The row of cards shows the headline numbers, and the charts and tables below explain them. Slicers filter the whole page.

Definitions of the measures are in `dashboards/powerbi/measures.dax`. Three rules apply everywhere: ratios are calculated as a ratio of sums (never an average of a rate), empty slices show blank and not an error, and all stock data is simulated.

### 3.1 Executive: is revenue on track, and where do orders leak?

![Executive](images/powerbi/01_executive.jpeg)

**Slicers:** date range, channel, platform.

| Visual | What it shows and how it is calculated | How to read it |
|---|---|---|
| Card: Revenue Placed | Sum of revenue of all placed orders, attributed or not (`Orders Daily`). | Total sales value. The screenshot shows 2,410,979. |
| Card: Revenue Delivered | Revenue of orders that were delivered, counted on the order date (a cohort view). | The cash that actually came in. 1,914,103 in the screenshot. |
| Card: Orders Placed | Count of placed orders. | 1,495. |
| Card: AOV | Average order value: revenue placed divided by orders placed. | 1,613. |
| Card: Delivered Share | Revenue delivered divided by revenue placed. | The share of sales that turned into cash: 79.4%. |
| Card: Cancel Rate | Cancelled orders divided by placed orders. | 5.8%. |
| Line chart: Revenue by day | Two lines, revenue placed and revenue delivered, by day. | Shows the trend and the gap between placed and delivered. One day, 9 October, holds about 55% of all revenue in the simulated data, and the title says so. |
| Bar chart: Orders by channel | Placed orders per channel, ranked. | Which channel brings orders: organic 859, paid social 387, paid search 249. |
| Bar chart: Where orders leak, by payment method | Three bars per payment method: cancel rate, return rate and failed-attempt rate, each over placed orders. | Where orders are lost. The failed-attempt rate is highest for vodafone_cash (27.6%) and fawry (25.1%). |
| Bar chart: Orders by payment method | Placed orders per payment method, ranked. | Cash on delivery is the largest, 832 orders. Read it next to the leak chart. |

### 3.2 Campaigns: where should the next budget go?

![Campaigns](images/powerbi/02_campaigns.jpeg)

**Slicers:** date range, campaign, platform.

| Visual | What it shows and how it is calculated | How to read it |
|---|---|---|
| Card: Spend | Sum of ad spend from the ad API. | 11,730. Ad days are Cairo days. |
| Card: CPA | Cost per acquisition: spend divided by attributed placed orders on days that had spend. | 93. Placed orders only. |
| Card: CTR | Click-through rate: API-reported clicks divided by impressions. | 2.4%. This is the platform's click count, not the click used for attribution. |
| Card: Conversion Rate | Attributed orders divided by API clicks. | 1.0%. |
| Card: ROAS Delivered | Return on ad spend: delivered revenue on spend days divided by spend. | The headline, 8.39. It is what the cash actually returns. |
| Bar chart: Return on ad spend by campaign | Two bars per campaign: ROAS delivered and ROAS placed, ranked by delivered. | The gap between the bars is the revenue lost to cancelled, returned and failed cash-on-delivery orders. For eid_sale_1 it is 11.67 delivered against 21.55 placed. |
| Bar chart: Cost per delivered order by campaign | Spend divided by delivered orders, per campaign. | Lower is better. ramadan_offer_1 costs 377, retarget_cart_1 184 and eid_sale_1 119. |
| Table: Campaign scorecard | Per campaign: spend, orders on spend days, CPA, ROAS delivered and CTR, with a total row. | The summary used to compare campaigns. The total row matches the cards. The scorecard CPA (171 for ramadan_offer_1) differs from the cost per delivered order (377) by definition. |

**Reading note.** ROAS is a period ratio. Read it over a week or a campaign flight, not as a daily truth. The latest days are still settling because delivered revenue lags spend by about two to four days.

### 3.3 Marketing and stock: which campaign sent shoppers to empty shelves?

![Marketing and stock](images/powerbi/03_marketing_and_stock.jpeg)

**Slicers:** date range, campaign. This page reads the D-1 marketing marts.

| Visual | What it shows and how it is calculated | How to read it |
|---|---|---|
| Card: Exposed Spend | Sum of the exposed spend of each campaign and day: spend multiplied by out-of-stock clicks over served clicks. | An estimate of spend that landed on products that were out of stock. 378 in the screenshot. |
| Card: OOS Click Share | Out-of-stock landing clicks divided by served landing clicks. | 10.2% of clicks met an empty shelf. |
| Card: Exposed Share of Spend | Exposed spend divided by campaign spend. | 10.1% of spend. |
| Card: Revenue at Risk | Add-to-carts on out-of-stock products multiplied by price. | 71,653. Revenue the shelf could not serve, not lost revenue. |
| Card: OOS Add to Cart | Count of add-to-cart events on out-of-stock products. | 349. |
| Bar chart: Exposed spend and campaign spend (D-1) | Two bars per campaign: exposed spend and total spend of that day. | The exposed bar is a small part of the spend bar. Compare campaigns: ramadan_offer_1 has 174 exposed of 1,758 spend. |
| Column chart: Out-of-stock add to cart by category | Add-to-carts on empty shelves per product category. | Which category is losing shoppers: home 177 and fashion 157. |
| Table: Products by revenue at risk (all campaigns) | Product, store and revenue at risk, sorted by revenue at risk. | The product list to restock or stop advertising. In the screenshot all three are at the Nasr City dark store. |
| Bar chart: Revenue at risk by category | Revenue at risk per category. | The money view of the category chart: fashion about 50K and home 22K. |

**Reading note.** This page is an estimate. Spend, orders and demand all use the Cairo day. The out-of-stock flag is the stock when the minute was written.

### 3.4 Operations: what to restock, and is the 20-minute promise holding?

![Operations](images/powerbi/04_operations.jpeg)

**Slicers:** date range, store, product category.

| Visual | What it shows and how it is calculated | How to read it |
|---|---|---|
| Card: Out of Stock Lines | Store and product pairs with available stock of zero or less, in the current snapshot of the eight dark stores. | 0 in the screenshot. Not filtered by date. |
| Card: Low Stock Lines | Pairs above zero but below their reorder point. | 0. |
| Card: Out of Stock Rate | Out-of-stock lines divided by all stock lines. | 0.0%. |
| Card: Express Orders | Count of express orders in the period. | 385. |
| Card: Express On-time Rate | On-time orders divided by on-time plus breached orders. | 98.1%. The target is 95%. |
| Card: Express Breaches | Orders delivered after their promise or undelivered beyond the promise plus 1 minute. Always a number, zero shows as 0. | 4. |
| Bar chart: Oversold units by store | Units customers asked for beyond the stock on the shelf, per store. | The app promised stock it did not have. All 7 units are at Nasr City. A lower bound. |
| Column chart: Units sold and received by day | Two columns per day: units sold and units received. | Stock is draining when sold exceeds received. |
| Column chart: Shrinkage rate by store | Shrunk units divided by sold plus shrunk units, per store. | Stock lost without a sale. New Cairo is highest at 10.5% in the screenshot. |
| Table: Products with oversold units | Product, oversold units and days of cover, sorted by oversold units. | The products behind the oversold chart. Days of cover is closing stock over the average daily sales, capped at 90. |
| Column chart: Express breaches by store | Breaches per dark store. | Which store missed the promise: Nasr City has 3. |
| Bar chart: Delivery hours by carrier (all orders) | Average hours from order to delivery, per carrier, over delivered orders with a known delivery time. | Standard delivery, not express: aramex 21.3, bosta 20.7 and jt_express 25.1 hours. |

### 3.5 Funnel: which step and which channel should we fix first?

![Funnel](images/powerbi/05_funnel.jpeg)

**Slicers:** date range, channel, platform.

| Visual | What it shows and how it is calculated | How to read it |
|---|---|---|
| Card: Cart Abandon Rate | `1 - orders / cart sessions`, blank for an empty slice, clamped between 0 and 1. | Of sessions that added to the cart, how many did not order. 69.1%. |
| Card: Checkout Abandon Rate | `1 - orders / checkout sessions`, blank for an empty slice, clamped between 0 and 1. | Of sessions that started checkout, how many did not order. 30.7%. |
| Card: Session Conversion | Funnel orders divided by sessions. | 10.4%. |
| Funnel: Funnel stages | Five stages: sessions, product-view sessions, cart sessions, checkout sessions and orders. | The width shows how many survive each step. Sessions 16,617, product views 16,120, cart 5,578, checkout 2,486, orders 1,723. The biggest drop is between product view and cart. |
| Bar chart: Cart and checkout abandon rate by channel | Two bars per channel. | Which channel loses the most shoppers. Direct has the highest cart abandon rate, 70.9%. The channels are close to each other. |
| Bar chart: Sessions by channel | Sessions per channel, ranked. | The size of each channel. Paid social brings the most, 4.5K. |

**Reading note.** Sessions are counted per Cairo day, so a session that crosses midnight counts once on each day. Daily sessions do not add up exactly across days.

### 3.6 Products and cities: what should we stock and promote, and where?

![Products and cities](images/powerbi/06_products_and_cities.jpeg)

**Slicers:** date range, product category, city.

| Visual | What it shows and how it is calculated | How to read it |
|---|---|---|
| Card: Product Revenue | Sum of revenue by city, product and day (`agg_city_product_daily`). | 2,410,979, the same as placed revenue on the Executive page. |
| Card: Product Units | Units sold. | 4,535. |
| Card: Product Page Views | Sessions that viewed a product, summed per city, product and day. | 54,626. A session that views three products counts three times, so it is larger than the funnel's sessions. |
| Card: Product Add to Carts | Sessions that added a product to the cart. | 5,578. |
| Card: Product Cart Rate | Add-to-carts divided by product page views. | 10.2%. |
| Bar chart: Revenue by product category | Revenue per category, ranked. | Electronics earns about 1.3M, far more than fashion, home and beauty. |
| Bar chart: Revenue by city | Revenue per city, ranked. | Cairo leads with 0.72M, then Other, Giza and Alexandria. |
| Column chart: Add to cart by category | Add-to-carts per category. | Almost equal across categories (1,359 to 1,454), so electronics earns more per cart, not more carts. |
| Table: Top products | Product, category, units, revenue, page views and cart rate, sorted by revenue. | The best sellers and how well each converts from view to cart. |

### 3.7 Customers: who buys again, and who should we win back?

![Customers](images/powerbi/07_customers.jpeg)

**Slicers:** city, favourite category, preferred payment method. This page has no date filter: it shows one row per customer, in the current state.

| Visual | What it shows and how it is calculated | How to read it |
|---|---|---|
| Card: Customers on File | Distinct customers in `customer_360`. | 5,000. |
| Card: Buyers | Customers with at least one placed order. | 1,295. |
| Card: Repeat Buyer Rate | Customers with two or more placed orders divided by buyers. | 13.9%. |
| Card: Lifetime Spend | Spend of orders that were neither cancelled nor returned, in EGP. | 2,084,020. |
| Card: Average Spend per Buyer | Lifetime spend divided by buyers. | 1,609. |
| Card: Quiet Buyers (7+ days) | Buyers whose last order is 7 or more days old. | 106. The data spans about 15 days, so a 30-day rule would always be empty. |
| Bar chart: Buyers by favourite category | Buyers per category, ranked. | Electronics has the most, 583. |
| Bar chart: Buyers by preferred payment | Buyers per preferred payment method, ranked. | Cash on delivery leads with 740. |
| Column chart: Average spend per buyer by category | Lifetime spend divided by buyers, per favourite category. | Electronics buyers spend the most, about 2.5K. A per-buyer measure avoids a misleading "(Blank)" bar for customers who never ordered. |
| Bar chart: Lifetime spend by city | Lifetime spend per city, ranked. | Cairo leads with 0.62M. |
| Table: Top customers by lifetime spend | Customer id, city, favourite category, orders, lifetime spend and days since last order. | Meant to list the highest spenders first. **Check on the screen:** in the screenshot the table is sorted by days since the last order, so customers with 0 orders appear first, not the top spenders. If you see that, click the "Lifetime spend" header until the largest values come first. The report only holds the customer id, never names or phone numbers. |

---

## 4. Glossary of the measures used in the pages

| Term | Meaning |
|---|---|
| ROAS | Return on ad spend: revenue divided by spend. "Delivered" counts only revenue from delivered orders, "placed" counts all placed orders. |
| CPA | Spend divided by placed orders. |
| Cost per delivered order | Spend divided by delivered orders only. |
| CTR | Clicks divided by impressions, as reported by the ad API. |
| AOV | Average order value, placed revenue divided by placed orders. |
| Delivered share | Delivered revenue divided by placed revenue. |
| Cancel, return, failed-attempt rate | Cancelled, returned, or with at least one failed delivery attempt, over placed orders. |
| Funnel stages | Sessions, product views, cart, checkout, order. Abandon rate is one minus the next step over the previous one. |
| Out of stock | A product with `available <= 0` in a store. Low stock is above zero but below the reorder point. |
| Oversold units | Units customers asked for beyond the stock on the shelf. A lower bound. |
| Shrinkage | Units lost without a sale. Rate is shrunk divided by sold plus shrunk. |
| Days of cover | Closing stock divided by average daily units sold, capped at 90. |
| Express on time, breached, at risk | On time: delivered within the order's own promise. Breached: delivered later, or undelivered beyond the promise plus 1 minute. At risk: undelivered with at least 75% of the promise used. |
| Exposed spend | Spend multiplied by the share of landing clicks that went to out-of-stock products. An estimate. |
| Revenue at risk | Add-to-carts on out-of-stock products multiplied by price. Not lost revenue. |
| Provisional or "Settling" | A recent day whose delivered revenue can still grow, so ROAS delivered reads low. |
| Late event | An event that reaches Kafka more than 10 minutes after its event time. |
| Quarantine | The table of rows rejected by the validator, each with an error code. |
