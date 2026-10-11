"""Builders for the provisioned Grafana dashboards. Plain functions returning dicts, so the JSON files are generated
(scripts/gen_dashboards.py) and a test fails when a file drifts from its builder."""

DS = {"type": "grafana-postgresql-datasource", "uid": "fs-postgres"}


def _target(sql, fmt):
    return [{"refId": "A", "datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True, "rawSql": sql}]


def _pos(x, y, w, h):
    return {"x": x, "y": y, "w": w, "h": h}


def stat(pid, title, desc, sql, pos, steps, unit="short", decimals=0):
    return {"id": pid, "type": "stat", "title": title, "description": desc, "datasource": DS, "gridPos": _pos(*pos),
            "targets": _target(sql, "table"),
            "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "thresholds"},
                                         "thresholds": {"mode": "absolute", "steps": steps},
                                         "decimals": decimals, "noValue": "no data"}, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "background", "graphMode": "none"}}


def timeseries(pid, title, desc, sql, pos, unit="short"):
    return {"id": pid, "type": "timeseries", "title": title, "description": desc, "datasource": DS,
            "gridPos": _pos(*pos), "targets": _target(sql, "time_series"),
            "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "palette-classic"},
                                         "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10,
                                                    "showPoints": "never", "spanNulls": False}, "min": 0},
                            "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}}


def table(pid, title, desc, sql, pos):
    return {"id": pid, "type": "table", "title": title, "description": desc, "datasource": DS, "gridPos": _pos(*pos),
            "targets": _target(sql, "table"), "fieldConfig": {"defaults": {}, "overrides": []},
            "options": {"showHeader": True}}


def text(pid, title, markdown, pos):
    return {"id": pid, "type": "text", "title": title, "gridPos": _pos(*pos),
            "options": {"mode": "markdown", "content": markdown}}


def dashboard(uid, title, panels):
    return {"uid": uid, "title": title, "tags": ["funnelstream"], "timezone": "Africa/Cairo", "refresh": "5s",
            "time": {"from": "now-30m", "to": "now"}, "schemaVersion": 39, "editable": True, "graphTooltip": 1,
            "templating": {"list": []}, "annotations": {"list": []}, "links": [], "panels": panels}


def _steps(*pairs):
    colors = ["green", "orange", "red"]
    return [{"color": colors[i], "value": v} for i, v in enumerate(pairs)]


def row(pid, title, y):
    return {"id": pid, "type": "row", "title": title, "collapsed": False, "gridPos": _pos(0, y, 24, 1), "panels": []}


# Express SLA definitions, shared by every express panel (the alert in alerting.yaml repeats them by hand).
# The promise is always the order's own promised_minutes; the breach test has one minute of grace.
_ON_TIME = "delivered_at IS NOT NULL AND delivered_at - created_at <= promised_minutes * interval '1 minute'"
_LATE = "delivered_at IS NOT NULL AND delivered_at - created_at > promised_minutes * interval '1 minute'"
_OVERDUE = ("delivered_at IS NULL AND NOT closed_without_delivery AND "
            "now() - created_at > promised_minutes * interval '1 minute' + interval '1 minute'")
_BREACHED = "(%s) OR (%s)" % (_LATE, _OVERDUE)
_AT_RISK = ("delivered_at IS NULL AND NOT closed_without_delivery AND "
            "now() - created_at >= promised_minutes * interval '45 seconds' AND "
            "now() - created_at <= promised_minutes * interval '1 minute' + interval '1 minute'")
_ELAPSED = "EXTRACT(EPOCH FROM (coalesce(delivered_at, now()) - created_at)) / 60.0"
_EXPRESS_DEFINITIONS = (
    "**On time**: delivered within the order's own promise (`delivered_at - created_at <= promised_minutes`). "
    "**Breached**: delivered later than the promise, or still undelivered more than the promise plus a 1-minute grace "
    "after it was created (the grace keeps a `delivered` event that reaches the sink a batch late from raising a false "
    "alarm). **At risk**: undelivered and at least 75% of the promise has passed (15 minutes for a 20-minute promise), "
    "not yet breached. An order that was cancelled or returned without a delivery is **closed without delivery**: it "
    "has no delivery to be late, so it is in no count. Times are simulator event times; the state is computed with "
    "now(), so it moves between Silver batches.")


def build_inventory_ops():
    panels = [
        stat(1, "Out-of-stock products now",
             "Store and product pairs with available stock of zero (simulated stock, latest sink write).",
             "SELECT count(*) AS \"out of stock\" FROM live_stock_now WHERE available <= 0",
             (0, 0, 6, 5), _steps(None, 1, 5)),
        stat(2, "Low-stock products now",
             "Pairs with stock above zero but below their reorder point.",
             "SELECT count(*) AS \"low stock\" FROM live_stock_now WHERE available > 0 AND available < reorder_point",
             (6, 0, 6, 5), _steps(None, 10, 30)),
        stat(3, "Seconds since the last stock change",
             "Age of the newest stock row. Stock is recomputed after each Silver batch, so it is about one batch behind.",
             "SELECT coalesce(round(extract(epoch from (now() - max(updated_at))))::int, 0) AS \"seconds\" FROM live_stock_now",
             (12, 0, 6, 5), _steps(None, 30, 60), unit="s"),
        text(4, "About this data",
             "All stock numbers are **simulated**: the simulator sells and restocks eight dark stores. "
             "Available stock is on-hand stock; order reservations are out of scope. Demand on empty shelves is flagged "
             "with the stock when the minute was written: an estimate that a replay can change.", (18, 0, 6, 5)),
        timeseries(5, "Demand on out-of-stock products per minute",
                   "Product views and add-to-carts that arrived for a store and product with zero stock when the minute "
                   "was written (the stock when the minute was written, not at event time). Demand the shelf could not serve.",
                   "SELECT $__timeGroupAlias(minute_start, '1m', 0), sum(views) AS \"views\", "
                   "sum(add_to_cart) AS \"add to cart\" FROM live_oos_demand_minute "
                   "WHERE oos_at_write AND $__timeFilter(minute_start) GROUP BY 1 ORDER BY 1", (0, 5, 12, 8)),
        timeseries(6, "App traffic: events per minute",
                   "Non-late, non-bot events per minute from live_metrics_minute (all countries).",
                   "SELECT $__timeGroupAlias(minute_start, '1m', 0), sum(events) AS \"events\" FROM live_metrics_minute "
                   "WHERE $__timeFilter(minute_start) GROUP BY 1 ORDER BY 1", (12, 5, 12, 8)),
        table(7, "Lowest stock now (20 products)",
              "The twenty store and product pairs with the least available stock.",
              "SELECT s.store_name AS \"store\", coalesce(p.name, l.product_id) AS \"product\", "
              "l.available AS \"available\", l.reorder_point AS \"reorder point\" FROM live_stock_now l "
              "JOIN dim_store s ON s.store_id = l.store_id "
              "LEFT JOIN dim_product_current p ON p.product_id = l.product_id "
              "ORDER BY l.available ASC, l.store_id, l.product_id LIMIT 20", (0, 13, 12, 9)),
        table(8, "Lost demand now (last 15 minutes)",
              "Products with the most add-to-carts while out of stock in the last 15 minutes (stock when the minute was written).",
              "SELECT s.store_name AS \"store\", coalesce(p.name, d.product_id) AS \"product\", "
              "sum(d.add_to_cart) AS \"add to cart\", sum(d.views) AS \"views\" FROM live_oos_demand_minute d "
              "JOIN dim_store s ON s.store_id = d.store_id "
              "LEFT JOIN dim_product_current p ON p.product_id = d.product_id "
              "WHERE d.oos_at_write AND d.minute_start >= now() - interval '15 minutes' "
              "GROUP BY d.store_id, s.store_name, d.product_id, p.name ORDER BY 3 DESC, 4 DESC LIMIT 10",
              (12, 13, 12, 9)),
        row(9, "Express delivery, 20-minute promise", 22),
        stat(10, "Express orders at risk now",
             "Undelivered express orders that used 75% of their promise and are not yet breached.",
             "SELECT count(*) AS \"orders\" FROM live_express_orders WHERE %s" % _AT_RISK,
             (0, 23, 6, 4), _steps(None, 1, 3)),
        stat(11, "Express breaches, last hour",
             "Orders created in the last hour that were delivered late or are overdue (promise plus 1 minute of grace).",
             "SELECT count(*) AS \"breaches\" FROM live_express_orders "
             "WHERE created_at >= now() - interval '1 hour' AND (%s)" % _BREACHED,
             (6, 23, 6, 4), [{"color": "green", "value": None}, {"color": "red", "value": 1}]),
        stat(12, "On-time rate, last hour",
             "On-time orders divided by delivered or breached orders created in the last hour. Cancelled and returned "
             "undelivered orders are in neither count.",
             "SELECT round(100.0 * count(*) FILTER (WHERE %s) / nullif(count(*) FILTER (WHERE (%s) OR (%s)), 0), 1) "
             "AS \"on time %%\" FROM live_express_orders WHERE created_at >= now() - interval '1 hour'"
             % (_ON_TIME, _ON_TIME, _BREACHED),
             (12, 23, 6, 4), [{"color": "red", "value": None}, {"color": "green", "value": 100}],
             unit="percent", decimals=1),
        stat(13, "Slowest express delivery, last hour (minutes)",
             "Longest elapsed time: the delivery time of delivered orders, the age of open orders that are not closed.",
             "SELECT round(max(%s)::numeric, 1) AS \"minutes\" FROM live_express_orders "
             "WHERE created_at >= now() - interval '1 hour' AND (delivered_at IS NOT NULL OR NOT closed_without_delivery)"
             % _ELAPSED,
             (18, 23, 6, 4), _steps(None, 15, 20), decimals=1),
        table(14, "Slowest express orders, last hour",
              "The ten express orders of the last hour with the longest elapsed time (closed orders sort last).",
              "SELECT coalesce(s.store_name, o.store_id) AS \"store\", o.order_id AS \"order\", promised_minutes AS \"promise (min)\", "
              "CASE WHEN delivered_at IS NULL AND closed_without_delivery THEN NULL "
              "ELSE round((EXTRACT(EPOCH FROM (coalesce(delivered_at, now()) - created_at)) / 60.0)::numeric, 1) "
              "END AS \"elapsed (min)\", "
              "CASE WHEN delivered_at IS NULL AND closed_without_delivery THEN 'closed without delivery' "
              "WHEN (%s) THEN 'breached' "
              "WHEN delivered_at IS NULL AND now() - created_at >= promised_minutes * interval '45 seconds' "
              "THEN 'at risk' ELSE 'on time' END AS \"state\" "
              "FROM live_express_orders o LEFT JOIN dim_store s ON s.store_id = o.store_id "
              "WHERE created_at >= now() - interval '1 hour' "
              "ORDER BY 4 DESC NULLS LAST, o.order_id LIMIT 10"
              % _BREACHED,
              (0, 27, 24, 9)),
        text(15, "Express definitions", _EXPRESS_DEFINITIONS, (0, 36, 24, 4)),
    ]
    return dashboard("fs-inventory-ops", "FunnelStream: Operations, live inventory", panels)


def build_inventory_daily():
    latest = "(SELECT max(report_date) FROM inventory_daily)"
    panels = [
        stat(1, "Closing stock, latest day (units)",
             "Units on hand at the end of the latest Cairo day in the mart (simulated stock, never below zero).",
             "SELECT coalesce(sum(closing_on_hand), 0) AS \"units\" FROM inventory_daily WHERE report_date = %s" % latest,
             (0, 0, 6, 5), [{"color": "blue", "value": None}]),
        stat(2, "Products out of stock at day end",
             "Store and product pairs whose stock minus reservations was zero or less at the end of the latest day.",
             "SELECT count(*) AS \"products\" FROM inventory_daily WHERE ended_out_of_stock AND report_date = %s" % latest,
             (6, 0, 6, 5), _steps(None, 1, 5)),
        stat(3, "Oversold units, latest day",
             "Units asked for beyond the shelf on the latest day. A lower bound: a sale of zero units emits no movement.",
             "SELECT coalesce(sum(oversold_units), 0) AS \"units\" FROM inventory_daily WHERE report_date = %s" % latest,
             (12, 0, 6, 5), _steps(None, 1, 20)),
        stat(4, "Shrinkage share of units out, latest day",
             "Shrunk units divided by shrunk plus sold units on the latest day.",
             "SELECT coalesce(round(100.0 * sum(units_shrunk) / nullif(sum(units_sold) + sum(units_shrunk), 0), 1), 0) "
             "AS \"percent\" FROM inventory_daily WHERE report_date = %s" % latest,
             (18, 0, 6, 5), _steps(None, 3, 6), unit="percent", decimals=1),
        timeseries(5, "Closing stock by store (units)",
                   "Stock at the end of each Cairo day, summed per dark store.",
                   "SELECT i.report_date::timestamp AS \"time\", s.store_name AS \"metric\", "
                   "sum(i.closing_on_hand) AS \"units\" FROM inventory_daily i JOIN dim_store s ON s.store_id = i.store_id "
                   "WHERE $__timeFilter(i.report_date::timestamp) GROUP BY 1, 2 ORDER BY 1", (0, 5, 12, 8)),
        timeseries(6, "Units sold, received and shrunk per day",
                   "Daily movement totals over all stores and products.",
                   "SELECT report_date::timestamp AS \"time\", sum(units_sold) AS \"sold\", "
                   "sum(units_received) AS \"received\", sum(units_shrunk) AS \"shrunk\" FROM inventory_daily "
                   "WHERE $__timeFilter(report_date::timestamp) GROUP BY 1 ORDER BY 1", (12, 5, 12, 8)),
        table(7, "Lowest days of cover, latest day (20 products)",
              "Closing stock divided by the average units sold per day over the last 7 days; products that did not sell "
              "are left out.",
              "SELECT s.store_name AS \"store\", coalesce(p.name, i.product_id) AS \"product\", "
              "i.closing_on_hand AS \"closing stock\", round(a.per_day, 1) AS \"sold per day\", "
              "round(i.closing_on_hand / nullif(a.per_day, 0), 1) AS \"days of cover\" FROM inventory_daily i "
              "JOIN dim_store s ON s.store_id = i.store_id "
              "LEFT JOIN dim_product_current p ON p.product_id = i.product_id "
              "JOIN (SELECT store_id, product_id, sum(units_sold)::numeric / 7 AS per_day FROM inventory_daily "
              "WHERE report_date > %s - 7 GROUP BY 1, 2) a ON a.store_id = i.store_id AND a.product_id = i.product_id "
              "WHERE i.report_date = %s AND a.per_day > 0 ORDER BY 5 ASC NULLS LAST LIMIT 20" % (latest, latest),
              (0, 13, 12, 9)),
        timeseries(8, "Demand on empty shelves per day",
                   "Product views and add-to-carts that arrived while the product was out of stock (the stock when the "
                   "minute was written).",
                   "SELECT report_date::timestamp AS \"time\", sum(oos_views) AS \"views\", "
                   "sum(oos_add_to_cart) AS \"add to cart\" FROM oos_demand_daily "
                   "WHERE $__timeFilter(report_date::timestamp) GROUP BY 1 ORDER BY 1", (12, 13, 12, 9)),
        row(12, "Express delivery, 20-minute promise", 22),
        stat(10, "Express on-time rate, latest day",
             "On-time orders divided by delivered or breached orders on the latest day in express_daily. 100 when "
             "there is nothing to measure.",
             "SELECT coalesce(round(100.0 * sum(on_time) / nullif(sum(on_time) + sum(breached), 0), 1), 100) "
             "AS \"percent\" FROM express_daily WHERE report_date = (SELECT max(report_date) FROM express_daily)",
             (0, 23, 6, 8), [{"color": "red", "value": None}, {"color": "green", "value": 100}],
             unit="percent", decimals=1),
        timeseries(11, "Slowest express delivery per day (minutes)",
                   "The longest delivery time of each Cairo day against the 20-minute promise.",
                   "SELECT report_date::timestamp AS \"time\", max(max_delivery_minutes) AS \"slowest\", "
                   "20 AS \"promise\" FROM express_daily WHERE $__timeFilter(report_date::timestamp) "
                   "GROUP BY 1 ORDER BY 1", (6, 23, 18, 8), unit="m"),
        text(9, "About this data",
             "Simulated stock. Closing stock is the end of the Cairo day; oversold units are a lower bound; cancelled "
             "orders do not return stock (reserve-on-order); demand on empty shelves is flagged with the stock when the "
             "minute was written, which a replay can change.", (0, 31, 24, 3)),
    ]
    d = dashboard("fs-inventory-daily", "FunnelStream: Inventory insights, daily", panels)
    d["time"] = {"from": "now-14d", "to": "now"}  # daily rows: the 30-minute default of the live pages would be empty
    d["refresh"] = "1m"
    return d


def build_marketing_d1():
    latest = "(SELECT max(report_date) FROM marketing_d1_campaign)"

    def card(expr, alias):
        return "SELECT %s AS \"%s\" FROM marketing_d1_campaign WHERE report_date = %s" % (expr, alias, latest)
    blue = [{"color": "blue", "value": None}]
    panels = [
        stat(1, "Spend (EGP)", "Ad spend of the latest D-1 day (the ad platform's UTC day), all campaigns.",
             card("coalesce(sum(spend), 0)", "spend"), (0, 0, 4, 5), blue, unit="short"),
        stat(2, "Orders placed", "Orders attributed to the campaigns (last touch) on the latest D-1 day.",
             card("coalesce(sum(orders_placed), 0)", "orders"), (4, 0, 4, 5), blue),
        stat(3, "ROAS placed", "Placed revenue divided by spend on the latest D-1 day.",
             card("coalesce(sum(revenue_placed) / nullif(sum(spend), 0), 0)", "roas placed"),
             (8, 0, 4, 5), blue, decimals=2),
        stat(4, "ROAS delivered (provisional for recent days)",
             "Delivered revenue divided by spend. Delivery matures one to three days after the order day, so recent "
             "days read low.",
             card("coalesce(sum(revenue_delivered) / nullif(sum(spend), 0), 0)", "roas delivered"),
             (12, 0, 4, 5), blue, decimals=2),
        stat(5, "Estimated spend exposed to out-of-stock (EGP)",
             "Spend x out-of-stock landing clicks / served landing clicks. An estimate that assumes spend is spread "
             "evenly over served landing clicks.",
             card("coalesce(sum(exposed_spend), 0)", "exposed spend"), (16, 0, 4, 5), _steps(None, 1, 1000)),
        stat(6, "Out-of-stock click share",
             "Landing clicks in minutes flagged out of stock, divided by all landing clicks in served districts.",
             card("coalesce(sum(oos_clicks)::numeric / nullif(sum(served_clicks), 0), 0)", "share"),
             (20, 0, 4, 5), _steps(None, 0.05, 0.15), unit="percentunit", decimals=1),
        table(7, "Campaigns, sorted by exposed spend",
              "One row per campaign with spend on the latest D-1 day; provisional marks days whose delivered revenue "
              "can still grow.",
              "SELECT campaign_id, utm_campaign, spend, orders_placed, cpa, roas_placed, roas_delivered, "
              "served_clicks, oos_clicks, oos_click_share, exposed_spend, provisional FROM marketing_d1_campaign "
              "WHERE report_date = %s ORDER BY exposed_spend DESC NULLS LAST" % latest, (0, 5, 24, 8)),
        table(8, "Top 5 products by revenue at risk",
              "Out-of-stock add-to-carts times price on the latest D-1 day. The mart keeps every out-of-stock "
              "product; the limit is this panel's.",
              "SELECT s.store_name AS \"store\", coalesce(p.name, m.product_id) AS \"product\", m.oos_add_to_carts, "
              "m.revenue_at_risk FROM marketing_d1_product m JOIN dim_store s ON s.store_id = m.store_id "
              "LEFT JOIN dim_product_current p ON p.product_id = m.product_id "
              "WHERE m.report_date = (SELECT max(report_date) FROM marketing_d1_product) "
              "ORDER BY m.revenue_at_risk DESC NULLS LAST LIMIT 5", (0, 13, 24, 8)),
        text(9, "About this data",
             "Simulated data. Exposed spend = spend x out-of-stock landing clicks / served landing clicks, both counted "
             "in served districts; it is an estimate that assumes spend is spread evenly over served landing clicks. "
             "The out-of-stock flag is the stock when the minute was written, which a replay can change. Spend is the "
             "ad platform's UTC day, orders and demand are the Cairo day: they differ by at most 3 hours. Recent days "
             "are provisional: ROAS delivered matures after 1 to 3 days.", (0, 21, 24, 4)),
    ]
    d = dashboard("fs-marketing-d1", "FunnelStream: Marketing, D-1", panels)
    d["time"] = {"from": "now-14d", "to": "now"}
    d["refresh"] = "1m"
    return d
