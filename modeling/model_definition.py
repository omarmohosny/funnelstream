"""Baseline model: build(style) returns the model spec for a star, snowflake or galaxy Gold schema.

Star: 1 fact + 3 dimensions (4 tables). Galaxy (default): 3 facts sharing 4 conformed dimensions (7 tables).
Snowflake: galaxy plus the outriggers dim_city and dim_category (9 tables).
Every column cites a real Silver column (source_column) or carries a Spark SQL expression. Dimension joins
are written as @gold.<table> so the same select renders for Databricks or dbt.
"""
CATALOG = "funnelstream"
SCHEMA = "gold"
MONEY = "DECIMAL(12,2)"
RATE = "DECIMAL(8,4)"


def col(name, data_type, role, source="", expr="", ref=""):
    return {"column": name, "data_type": data_type, "role": role, "source_column": source,
            "expression": expr, "references": ref}


def _scd2_dimension(name, key, bk, silver, attrs, snowflake_refs=()):
    """SCD2 dimension over a Silver versions table. attrs: (name, type, role) of the descriptive columns."""
    cols = [col(key, "BIGINT", "pk", "%s.%s" % (silver, bk), "xxhash64(v.%s, v.updated_at)" % bk),
            col(bk, "STRING", "bk", "%s.%s" % (silver, bk), "v.%s" % bk)]
    for a, typ, role in attrs:
        cols.append(col(a, typ, role, "%s.%s" % (silver, a), "v.%s" % a))
    for ref_col, ref in snowflake_refs:
        cols.append(col(ref_col, "BIGINT", "fk", "", "xxhash64(v.%s)" % ref[1], ref[0]))
    cols += [col("valid_from", "TIMESTAMP", "scd_meta", "%s.updated_at" % silver, "v.updated_at"),
             col("valid_to", "TIMESTAMP", "scd_meta", "%s.updated_at" % silver,
                 "coalesce(v.next_ts, TIMESTAMP'9999-12-31')"),
             col("is_current", "BOOLEAN", "scd_meta", "%s.updated_at" % silver, "v.next_ts IS NULL")]
    return {"name": name, "type": "dimension", "scd": 2, "grain": "one row per %s version" % bk,
            "pk": [key],
            "ctes": ["v AS (SELECT *, lead(updated_at) OVER (PARTITION BY %s ORDER BY updated_at) AS next_ts "
                     "FROM %s)" % (bk, silver)],
            "from_sql": "v", "group_by": [], "columns": cols}


def dim_customer(snowflake=False):
    attrs = [("signup_date", "DATE", "attribute"), ("primary_device", "STRING", "attribute"),
             ("marketing_consent", "BOOLEAN", "attribute")]
    if snowflake:
        # city and country move to the dim_city outrigger
        t = _scd2_dimension("dim_customer", "customer_key", "customer_id", "silver_customers", attrs,
                            snowflake_refs=[("city_key", ("dim_city.city_key", "city"))])
    else:
        attrs = [("country", "STRING", "attribute"), ("city", "STRING", "attribute"),
                 ("district", "STRING", "attribute")] + attrs
        t = _scd2_dimension("dim_customer", "customer_key", "customer_id", "silver_customers", attrs)
    return t


def dim_product(snowflake=False):
    attrs = [("name", "STRING", "attribute"), ("price", "DECIMAL(10,2)", "attribute"),
             ("is_active", "BOOLEAN", "attribute")]
    if snowflake:
        return _scd2_dimension("dim_product", "product_key", "product_id", "silver_products", attrs,
                               snowflake_refs=[("category_key", ("dim_category.category_key", "category"))])
    return _scd2_dimension("dim_product", "product_key", "product_id", "silver_products",
                           [attrs[0], ("category", "STRING", "attribute")] + attrs[1:])


def dim_campaign():
    return {"name": "dim_campaign", "type": "dimension", "scd": 1, "grain": "one row per campaign", "pk": ["campaign_key"],
            "ctes": [], "from_sql": "silver_campaign_snapshots",
            "group_by": ["campaign_id", "utm_campaign", "ad_platform"],
            "columns": [
                col("campaign_key", "BIGINT", "pk", "silver_campaign_snapshots.campaign_id", "xxhash64(campaign_id)"),
                col("campaign_id", "STRING", "bk", "silver_campaign_snapshots.campaign_id", "campaign_id"),
                col("utm_campaign", "STRING", "attribute", "silver_campaign_snapshots.utm_campaign", "utm_campaign"),
                col("platform", "STRING", "attribute", "silver_campaign_snapshots.ad_platform", "ad_platform"),
                col("start_date", "DATE", "attribute", "silver_campaign_snapshots.report_date", "min(report_date)"),
                col("end_date", "DATE", "attribute", "silver_campaign_snapshots.report_date", "max(report_date)")]}


_RAMADAN = ("(d BETWEEN DATE'2025-03-01' AND DATE'2025-03-29' OR d BETWEEN DATE'2026-02-18' AND DATE'2026-03-19' "
            "OR d BETWEEN DATE'2027-02-08' AND DATE'2027-03-09')")
_EID = ("(d BETWEEN DATE'2025-03-30' AND DATE'2025-04-01' OR d BETWEEN DATE'2026-03-20' AND DATE'2026-03-22' "
        "OR d BETWEEN DATE'2027-03-10' AND DATE'2027-03-12')")


def dim_date():
    return {"name": "dim_date", "type": "dimension", "scd": 1, "grain": "one row per calendar day 2025-2027",
            "pk": ["date_key"], "ctes": [],
            "from_sql": "(SELECT explode(sequence(DATE'2025-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS d)",
            "group_by": [],
            "columns": [
                col("date_key", "INT", "pk", "", "CAST(date_format(d, 'yyyyMMdd') AS INT)"),
                col("date", "DATE", "attribute", "", "d"),
                col("week", "INT", "attribute", "", "weekofyear(d)"),
                col("month", "INT", "attribute", "", "month(d)"),
                col("is_weekend", "BOOLEAN", "attribute", "", "dayofweek(d) IN (6, 7)"),
                col("is_friday_saturday", "BOOLEAN", "attribute", "", "dayofweek(d) IN (6, 7)"),
                col("is_ramadan", "BOOLEAN", "attribute", "", _RAMADAN),
                col("is_eid", "BOOLEAN", "attribute", "", _EID),
                col("is_holiday", "BOOLEAN", "attribute", "", _EID + " OR (month(d) = 10 AND day(d) = 6)")]}


def dim_city():
    return {"name": "dim_city", "type": "outrigger", "scd": 1, "grain": "one row per city", "pk": ["city_key"],
            "ctes": [], "from_sql": "silver_customers", "group_by": ["city", "country"],
            "columns": [col("city_key", "BIGINT", "pk", "silver_customers.city", "xxhash64(city)"),
                        col("city", "STRING", "bk", "silver_customers.city", "city"),
                        col("country", "STRING", "attribute", "silver_customers.country", "country")]}


def dim_category():
    return {"name": "dim_category", "type": "outrigger", "scd": 1, "grain": "one row per product category",
            "pk": ["category_key"], "ctes": [], "from_sql": "silver_products", "group_by": ["category"],
            "columns": [col("category_key", "BIGINT", "pk", "silver_products.category", "xxhash64(category)"),
                        col("category_name", "STRING", "attribute", "silver_products.category", "category")]}


_CUSTOMER_JOIN = ("LEFT JOIN @gold.dim_customer dc ON dc.customer_id = o.customer_id "
                  "AND o.order_created_at >= dc.valid_from AND o.order_created_at < dc.valid_to")


def fact_orders():
    o = "silver_orders."
    return {"name": "fact_orders", "type": "fact", "scd": 0, "grain": "one row per order (current state)",
            "pk": ["order_id"],
            "ctes": [
                "cur AS (SELECT order_id, status, status_rank, carrier FROM (SELECT *, row_number() OVER "
                "(PARTITION BY order_id ORDER BY status_rank DESC, status_time DESC) AS rn "
                "FROM silver_order_status) WHERE rn = 1)",
                "ev AS (SELECT order_id, min(CASE WHEN status = 'delivered' THEN status_time END) AS delivered_at, "
                "count_if(status = 'delivery_failed') AS failed_count FROM silver_order_status GROUP BY order_id)"],
            "from_sql": "silver_orders o LEFT JOIN cur ON cur.order_id = o.order_id LEFT JOIN ev ON ev.order_id = "
                        "o.order_id " + _CUSTOMER_JOIN + " LEFT JOIN @gold.dim_campaign dg ON dg.utm_campaign = "
                        "o.utm_campaign",
            "group_by": [],
            "columns": [
                col("order_id", "STRING", "pk", o + "order_id", "o.order_id"),
                col("customer_key", "BIGINT", "fk", o + "customer_id", "coalesce(dc.customer_key, -1)",
                    "dim_customer.customer_key"),
                col("campaign_key", "BIGINT", "fk", o + "utm_campaign", "coalesce(dg.campaign_key, -1)",
                    "dim_campaign.campaign_key"),
                col("date_key", "INT", "fk", o + "order_created_at",
                    "CAST(date_format(from_utc_timestamp(o.order_created_at, 'Africa/Cairo'), 'yyyyMMdd') AS INT)",
                    "dim_date.date_key"),
                col("channel", "STRING", "attribute", o + "channel", "o.channel"),
                col("platform", "STRING", "attribute", o + "platform", "o.platform"),
                col("payment_method", "STRING", "attribute", o + "payment_method", "o.payment_method"),
                col("carrier", "STRING", "attribute", "silver_order_status.carrier", "cur.carrier"),
                col("city", "STRING", "attribute", o + "city", "o.city"),
                col("district", "STRING", "attribute", o + "district", "o.district"),
                col("country", "STRING", "attribute", o + "country", "o.country"),
                col("order_total", MONEY, "measure", o + "order_total", "o.order_total"),
                col("current_status", "STRING", "attribute", "silver_order_status.status", "cur.status"),
                col("status_rank", "INT", "attribute", "silver_order_status.status_rank", "cur.status_rank"),
                col("order_created_at", "TIMESTAMP", "attribute", o + "order_created_at", "o.order_created_at"),
                col("delivered_at", "TIMESTAMP", "attribute", "silver_order_status.status_time", "ev.delivered_at"),
                col("delivery_lead_hours", "DECIMAL(8,2)", "measure", "silver_order_status.status_time",
                    "CAST((unix_timestamp(ev.delivered_at) - unix_timestamp(o.order_created_at)) / 3600 AS DECIMAL(8,2))"),
                col("delivery_failed_count", "INT", "measure", "silver_order_status.status",
                    "CAST(coalesce(ev.failed_count, 0) AS INT)"),
                col("is_cancelled", "BOOLEAN", "measure", "silver_order_status.status",
                    "cur.status = 'order_cancelled'"),
                col("is_delivered", "BOOLEAN", "measure", "silver_order_status.status",
                    "ev.delivered_at IS NOT NULL"),
                col("is_returned", "BOOLEAN", "measure", "silver_order_status.status",
                    "cur.status = 'order_returned'")]}


def fact_order_lines():
    i = "silver_order_items."
    return {"name": "fact_order_lines", "type": "fact", "scd": 0, "grain": "one row per order line",
            "pk": ["order_id", "line_no"], "ctes": [],
            "from_sql": "silver_order_items i JOIN silver_orders o ON o.order_id = i.order_id " + _CUSTOMER_JOIN +
                        " LEFT JOIN @gold.dim_product dp ON dp.product_id = i.product_id AND o.order_created_at >= "
                        "dp.valid_from AND o.order_created_at < dp.valid_to",
            "group_by": [],
            "columns": [
                col("order_id", "STRING", "pk", i + "order_id", "i.order_id"),
                col("line_no", "INT", "pk", i + "line_no", "i.line_no"),
                col("customer_key", "BIGINT", "fk", "silver_orders.customer_id", "coalesce(dc.customer_key, -1)",
                    "dim_customer.customer_key"),
                col("product_key", "BIGINT", "fk", i + "product_id", "coalesce(dp.product_key, -1)",
                    "dim_product.product_key"),
                col("date_key", "INT", "fk", i + "order_time",
                    "CAST(date_format(from_utc_timestamp(i.order_time, 'Africa/Cairo'), 'yyyyMMdd') AS INT)",
                    "dim_date.date_key"),
                col("city", "STRING", "attribute", "silver_orders.city", "o.city"),
                col("quantity", "INT", "measure", i + "quantity", "i.quantity"),
                col("line_total", MONEY, "measure", i + "line_total", "i.line_total")]}


def fact_funnel_daily():
    e = "silver_events."
    return {"name": "fact_funnel_daily", "type": "fact", "scd": 0,
            "grain": "one row per day, country, channel, platform (bots excluded)",
            "pk": ["date_key", "country", "channel", "platform"],
            "ctes": ["agg AS (SELECT CAST(date_format(event_date, 'yyyyMMdd') AS INT) AS date_key, country, channel, "
                     "platform, count(DISTINCT session_id) AS sessions, "
                     "count(DISTINCT CASE WHEN event_type = 'product_view' THEN session_id END) AS product_views, "
                     "count(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) AS cart_sessions, "
                     "count(DISTINCT CASE WHEN event_type = 'checkout_started' THEN session_id END) AS "
                     "checkout_sessions, count(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) AS "
                     "orders FROM silver_events WHERE NOT is_bot GROUP BY 1, 2, 3, 4)"],
            "from_sql": "agg", "group_by": [],
            "columns": [
                col("date_key", "INT", "pk", e + "event_date", "agg.date_key", "dim_date.date_key"),
                col("country", "STRING", "pk", e + "country", "agg.country"),
                col("channel", "STRING", "pk", e + "channel", "agg.channel"),
                col("platform", "STRING", "pk", e + "platform", "agg.platform"),
                col("sessions", "BIGINT", "measure", e + "session_id", "agg.sessions"),
                col("product_views", "BIGINT", "measure", e + "session_id", "agg.product_views"),
                col("cart_sessions", "BIGINT", "measure", e + "session_id", "agg.cart_sessions"),
                col("checkout_sessions", "BIGINT", "measure", e + "session_id", "agg.checkout_sessions"),
                col("orders", "BIGINT", "measure", e + "session_id", "agg.orders"),
                col("cart_abandon_rate", RATE, "measure", "", "CAST(1 - agg.checkout_sessions / "
                    "nullif(agg.cart_sessions, 0) AS DECIMAL(8,4))"),
                col("checkout_abandon_rate", RATE, "measure", "", "CAST(1 - agg.orders / "
                    "nullif(agg.checkout_sessions, 0) AS DECIMAL(8,4))")]}


def build(style: str = "galaxy") -> dict:
    if style == "star":
        tables = [dim_customer(), dim_campaign(), dim_date(), fact_orders()]
    elif style in ("galaxy", "snowflake"):
        snow = style == "snowflake"
        tables = [dim_customer(snow), dim_product(snow), dim_campaign(), dim_date(),
                  fact_orders(), fact_order_lines(), fact_funnel_daily()]
        if snow:
            tables = [dim_city(), dim_category()] + tables
    else:
        raise ValueError("style must be star, snowflake or galaxy: %r" % style)
    return {"style": style, "catalog": CATALOG, "schema": SCHEMA, "generated_by": "baseline", "tables": tables}
