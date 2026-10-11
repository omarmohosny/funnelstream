"""Generate dashboards/powerbi/FunnelStream.pbip (version 2: Databricks Gold tables plus the Postgres inventory and marketing
marts) from samples/*.csv, the Postgres column lists below and measures.dax.

The Postgres columns are copied from warehouse/live_ddl.sql (see PG_TABLES); test_make_pbip.py checks them against that file."""
import csv, json, re, uuid, shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "dashboards" / "powerbi"
HOST = "dbc-a56fe50a-1b9d.cloud.databricks.com"
PATH = "/sql/1.0/warehouses/17ec51a6e0e7906f"
CATALOG = "funnelstream"

# (power bi name, gold table, filter step or None)
TABLES = [
    ("Date", "dim_date", None),
    ("Campaign", "dim_campaign", None),
    ("Product", "dim_product", 'Table.SelectRows(Table.Buffer(#"Selected"), each [is_current] = true)'),
    ("Campaign Daily", "agg_campaign_daily", 'Table.SelectRows(#"Selected", each [attribution_model] = "last_touch")'),
    ("Funnel Daily", "agg_funnel_daily", None),
    ("Orders Daily", "agg_orders_daily", None),
    ("Orders", "fact_orders", None),
    ("City Product Daily", "agg_city_product_daily", None),
    ("Order Lines", "fact_order_lines", None),
    ("Customer 360", "customer_360", None),
    ("Customer", "dim_customer", 'Table.SelectRows(Table.Buffer(#"Selected"), each [is_current] = true)'),
]
PG_HOST = "localhost:5432"
PG_DB = "funnelstream"
PG_SCHEMA = "public"
# (power bi name, postgres table, [(column, power bi type)]) -- read-only role grafana_ro, Import mode
PG_TABLES = [
    ("Store", "dim_store", [("store_id", "string"), ("store_name", "string"), ("city", "string")]),
    ("Stock Now", "live_stock_now", [("store_id", "string"), ("product_id", "string"), ("on_hand", "int64"),
                                     ("reserved_qty", "int64"), ("available", "int64"), ("reorder_point", "int64"),
                                     ("updated_at", "dateTime")]),
    ("Inventory Daily", "inventory_daily", [("report_date", "dateTime"), ("store_id", "string"), ("product_id", "string"),
                                            ("closing_on_hand", "int64"), ("units_sold", "int64"), ("units_received", "int64"),
                                            ("units_shrunk", "int64"), ("units_adjusted", "int64"), ("oversold_units", "int64"),
                                            ("ended_out_of_stock", "boolean")]),
    ("OOS Demand Daily", "oos_demand_daily", [("report_date", "dateTime"), ("store_id", "string"), ("product_id", "string"),
                                              ("campaign_id", "string"), ("utm_campaign", "string"), ("served_views", "int64"),
                                              ("served_add_to_cart", "int64"), ("served_clicks", "int64"), ("oos_views", "int64"),
                                              ("oos_add_to_cart", "int64"), ("oos_clicks", "int64")]),
    ("Express Daily", "express_daily", [("report_date", "dateTime"), ("store_id", "string"), ("orders", "int64"),
                                        ("delivered", "int64"), ("on_time", "int64"), ("breached", "int64"),
                                        ("avg_delivery_minutes", "decimal"), ("max_delivery_minutes", "decimal")]),
    ("Marketing D1 Campaign", "marketing_d1_campaign", [("report_date", "dateTime"), ("campaign_id", "string"),
                                                        ("utm_campaign", "string"), ("spend", "decimal"), ("ad_clicks", "int64"),
                                                        ("orders_placed", "int64"), ("revenue_placed", "decimal"),
                                                        ("revenue_delivered", "decimal"), ("cpa", "decimal"),
                                                        ("roas_placed", "decimal"), ("roas_delivered", "decimal"),
                                                        ("served_clicks", "int64"), ("oos_clicks", "int64"),
                                                        ("oos_click_share", "decimal"), ("exposed_spend", "decimal"),
                                                        ("provisional", "boolean")]),
    ("Marketing D1 Product", "marketing_d1_product", [("report_date", "dateTime"), ("store_id", "string"),
                                                      ("product_id", "string"), ("oos_views", "int64"),
                                                      ("oos_add_to_carts", "int64"), ("revenue_at_risk", "decimal")]),
]
PRODUCT_KEEP = ["product_id", "name", "category", "price", "is_current"]
CUSTOMER_KEEP = ["customer_id", "country", "city", "district", "signup_date", "primary_device", "marketing_consent", "is_current"]
INT_NAMES = {"impressions", "clicks", "platform_conversions", "orders_placed", "orders_delivered", "orders_cancelled",
             "orders_returned", "orders_with_failed_attempt", "new_customer_orders", "sessions_viewed", "add_to_cart", "units", "quantity"}
DOUBLE_NAMES = {"spend", "revenue_placed", "revenue_delivered", "aov", "cpa", "cost_per_delivered_order", "roas_placed",
                "roas_delivered", "ctr", "conversion_rate", "cancel_rate", "failed_attempt_rate", "return_rate", "open_rate",
                "cart_abandon_rate", "checkout_abandon_rate", "price", "order_total", "delivery_lead_hours", "revenue", "line_total"}
DATE_NAMES = {"date", "report_date", "event_date", "valid_from", "valid_to", "start_date", "end_date", "signup_date"}
RATE_HIDDEN = {"ctr", "conversion_rate", "cpa", "cost_per_delivered_order", "roas_placed", "roas_delivered", "cancel_rate",
               "failed_attempt_rate", "return_rate", "open_rate", "aov", "cart_abandon_rate", "checkout_abandon_rate"}
SHOW_IDS = {"order_id"}


def q(name):
    return "'%s'" % name if re.search(r"[^A-Za-z0-9_]", name) else name


def infer(col, values):
    if col in INT_NAMES:
        return "int64"
    if col in DOUBLE_NAMES:
        return "double"
    if col in DATE_NAMES:
        return "dateTime"
    if col.endswith("_at"):
        return "dateTime"
    vals = [v for v in values if v != ""]
    if not vals:
        return "string"
    low = {v.lower() for v in vals}
    if low <= {"true", "false"}:
        return "boolean"
    try:
        [int(v) for v in vals]
        return "int64"
    except ValueError:
        pass
    try:
        [float(v) for v in vals]
        return "double"
    except ValueError:
        return "string"


def read_cols(table):
    with open(ROOT / "samples" / (table + ".csv"), newline="", encoding="utf-8") as fh:
        rd = csv.DictReader(fh)
        rows = list(rd)
        cols = rd.fieldnames
    return [(c, infer(c, [r[c] for r in rows])) for c in cols]


def hidden(col):
    if col in SHOW_IDS:
        return False
    return col.endswith("_key") and col not in ("date_key",) or col.endswith("_id") or col in RATE_HIDDEN or col == "attribution_model"


def table_tmdl(name, gold, step):
    cols = read_cols(gold)
    if name == "Product":
        cols = [c for c in cols if c[0] in PRODUCT_KEEP]
    if name == "Customer":
        cols = [c for c in cols if c[0] in CUSTOMER_KEEP]
    if name == "Campaign Daily":
        cols = [c for c in cols if c[0] != "attribution_model"]
    names = ", ".join('"%s"' % c for c, _ in cols)
    steps = [
        "Source = Databricks.Catalogs(\"%s\", \"%s\", [Catalog = \"%s\", Database = null, EnableAutomaticProxyDiscovery = null])" % (HOST, PATH, CATALOG),
        "Db = Source{[Name = \"%s\", Kind = \"Database\"]}[Data]" % CATALOG,
        "Gold = Db{[Name = \"gold\", Kind = \"Schema\"]}[Data]",
        "Raw = Gold{[Name = \"%s\", Kind = \"Table\"]}[Data]" % gold,
    ]
    last = "Raw"
    if step:
        steps.append("Filtered = " + step.replace('#"Selected"', "Raw"))
        last = "Filtered"
    steps.append("Selected = Table.SelectColumns(%s, {%s})" % (last, names))
    m = "let\n\t\t\t\t" + ",\n\t\t\t\t".join(steps) + "\n\t\t\tin\n\t\t\t\tSelected"
    out = ["table " + q(name)]
    if name == "Date":
        out.append("\tdataCategory: Time")
    out.append("")
    for c, t in cols:
        out.append("\tcolumn " + q(c))
        out.append("\t\tdataType: " + t)
        if t == "dateTime":
            out.append("\t\tformatString: " + ("yyyy-MM-dd HH:mm" if c.endswith("_at") else "yyyy-MM-dd"))
        if t == "int64":
            out.append("\t\tformatString: 0")
        if name == "Date" and c == "date":
            out.append("\t\tisKey")
        if hidden(c):
            out.append("\t\tisHidden")
        out.append("\t\tsummarizeBy: none")
        out.append("\t\tsourceColumn: " + c)
        out.append("")
    out.append("\tpartition %s = m" % q(name))
    out.append("\t\tmode: import")
    out.append("\t\tsource =")
    out.append("\t\t\t" + m)
    out.append("")
    return "\n".join(out)


def pg_table_tmdl(name, pg_table, cols):
    names = ", ".join('"%s"' % c for c, _ in cols)
    steps = [
        'Source = PostgreSQL.Database("%s", "%s")' % (PG_HOST, PG_DB),
        'Raw = Source{[Schema = "%s", Item = "%s"]}[Data]' % (PG_SCHEMA, pg_table),
        "Selected = Table.SelectColumns(Raw, {%s})" % names,
    ]
    m = "let\n\t\t\t\t" + ",\n\t\t\t\t".join(steps) + "\n\t\t\tin\n\t\t\t\tSelected"
    out = ["table " + q(name), ""]
    for c, t in cols:
        out.append("\tcolumn " + q(c))
        out.append("\t\tdataType: " + t)
        if t == "dateTime":
            out.append("\t\tformatString: " + ("yyyy-MM-dd HH:mm" if c.endswith("_at") else "yyyy-MM-dd"))
        if t == "int64":
            out.append("\t\tformatString: 0")
        if hidden(c):
            out.append("\t\tisHidden")
        out.append("\t\tsummarizeBy: none")
        out.append("\t\tsourceColumn: " + c)
        out.append("")
    out.append("\tpartition %s = m" % q(name))
    out.append("\t\tmode: import")
    out.append("\t\tsource =")
    out.append("\t\t\t" + m)
    out.append("")
    return "\n".join(out)


PCT = {"CTR", "Conversion Rate", "View Rate", "Cart Rate", "Checkout Rate", "Order Rate", "Session Conversion",
       "Cart Abandon Rate", "Checkout Abandon Rate", "Delivered Share of Revenue", "Cancel Rate", "Return Rate",
       "Failed Attempt Rate", "New Customer Share", "Direct Share", "Revenue Placed vs Last Week",
       "Out of Stock Rate", "Shrinkage Rate", "OOS Click Share", "Exposed Share of Spend", "Express On-time Rate",
       "Product Cart Rate", "First Attempt Delivery Rate", "Repeat Buyer Rate"}
RATIO2 = {"ROAS Placed", "ROAS Delivered", "ROAS Gap", "Platform vs Attributed"}
ONE_DECIMAL = {"Days of Cover", "Slowest Express Delivery (min)", "Average Express Delivery (min)", "Average Delivery Hours"}
TEXT = {"Maturity Label", "Is Provisional", "Promise Met"}


def parse_measures():
    lines = (ROOT / "dashboards" / "powerbi" / "measures.dax").read_text(encoding="utf-8").splitlines()
    folder, out, cur = None, [], None
    for ln in lines + [""]:
        if cur is not None:
            if ln.strip() == "":
                out.append(cur)
                cur = None
            else:
                cur["body"].append(ln)
            continue
        m = re.match(r"//\s*Display folder:\s*(.*)", ln)
        if m:
            folder = m.group(1).strip().replace(" \\ ", "\\")
            continue
        if ln and not ln.startswith("//") and not ln[0].isspace() and " =" in ln:
            name, rest = ln.split(" =", 1)
            cur = {"name": name.strip(), "folder": folder, "body": [rest] if rest.strip() else []}
    return [m for m in out if m["folder"]]


def measure_tmdl(m):
    n = m["name"]
    body = []
    for ln in m["body"]:
        stripped = ln.lstrip(" ")
        lead = (len(ln) - len(stripped)) // 4
        body.append("\t\t\t" + "\t" * lead + stripped)
    out = ["\tmeasure %s =" % q(n)] + body
    if n in PCT:
        out.append("\t\tformatString: \"0.0%\"")
    elif n in RATIO2:
        out.append("\t\tformatString: \"0.00\"")
    elif n in ONE_DECIMAL:
        out.append("\t\tformatString: \"#,0.0\"")
    elif n not in TEXT:
        out.append("\t\tformatString: \"#,0\"")
    out.append("\t\tdisplayFolder: " + m["folder"])
    out.append("")
    return "\n".join(out)


RELS = [
    ("Campaign Daily", "report_date", "Date", "date"),
    ("Funnel Daily", "event_date", "Date", "date"),
    ("Orders Daily", "date_key", "Date", "date_key"),
    ("Orders", "date_key", "Date", "date_key"),
    ("Campaign Daily", "campaign_id", "Campaign", "campaign_id"),
    ("Orders", "campaign_id", "Campaign", "campaign_id"),
    # version 2: the Postgres marts (MODEL.md section 2)
    ("Inventory Daily", "report_date", "Date", "date"),
    ("OOS Demand Daily", "report_date", "Date", "date"),
    ("Marketing D1 Campaign", "report_date", "Date", "date"),
    ("Marketing D1 Product", "report_date", "Date", "date"),
    ("Express Daily", "report_date", "Date", "date"),
    ("Marketing D1 Campaign", "campaign_id", "Campaign", "campaign_id"),
    ("OOS Demand Daily", "campaign_id", "Campaign", "campaign_id"),
    ("Stock Now", "store_id", "Store", "store_id"),
    ("Inventory Daily", "store_id", "Store", "store_id"),
    ("OOS Demand Daily", "store_id", "Store", "store_id"),
    ("Express Daily", "store_id", "Store", "store_id"),
    ("Marketing D1 Product", "store_id", "Store", "store_id"),
    ("Stock Now", "product_id", "Product", "product_id"),
    ("Inventory Daily", "product_id", "Product", "product_id"),
    ("OOS Demand Daily", "product_id", "Product", "product_id"),
    ("Marketing D1 Product", "product_id", "Product", "product_id"),
    # demand, order lines and customers
    ("City Product Daily", "date_key", "Date", "date_key"),
    ("City Product Daily", "product_id", "Product", "product_id"),
    ("Order Lines", "date_key", "Date", "date_key"),
    ("Order Lines", "product_id", "Product", "product_id"),
    ("Orders", "customer_id", "Customer", "customer_id"),
]


# ---- design (version 3): theme, page backgrounds, header bands and styled visuals on a 1280 x 720 canvas -------------
import hashlib

PAGE_W, PAGE_H, MARGIN, GAP = 1280, 720, 16, 8
HEADER_H = 64
SLICER_Y, SLICER_H = 72, 44
CARD_Y, CARD_H = 124, 84
BODY_Y = 216
BODY_H = PAGE_H - BODY_Y - MARGIN
BODY_BOTTOM = PAGE_H - MARGIN

# Colours. The categorical order and the white surface were run through the dataviz skill's validate_palette.js:
# every hard check passes; aqua, yellow and magenta are under 3:1 on white, so bar and column charts show data labels.
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
FUNNEL_RAMP = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281"]  # blue steps 250 to 650, validated as an ordinal ramp
NAVY, NAVY_SOFT = "#0d366b", "#cde2fb"                               # blue step 700 (header, callouts) and step 100
PLANE, SURFACE = "#f1f4f9", "#ffffff"
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
GOOD, WARNING, SERIOUS, CRITICAL = "#0ca30c", "#fab219", "#ec835a", "#d03b3b"
FONT, FONT_BOLD = "Segoe UI", "Segoe UI Semibold"

ROLES = {"card": ("Values",), "slicer": ("Values",), "tableEx": ("Values",), "funnel": ("Category", "Y"),
         "lineChart": ("Category", "Y", "Series"), "clusteredBarChart": ("Category", "Y", "Series"),
         "clusteredColumnChart": ("Category", "Y", "Series"), "treemap": ("Group", "Values"),
         "donutChart": ("Category", "Y"), "textbox": ()}


def M(name):
    return ("m", name)


def C(table, column):
    return ("c", table, column)


def field_json(f):
    if f[0] == "m":
        entity, prop, kind = "_Measures", f[1], "Measure"
    else:
        entity, prop, kind = f[1], f[2], "Column"
    return {"field": {kind: {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}},
            "queryRef": "%s.%s" % (entity, prop), "nativeQueryRef": prop}


# Power BI formatting values are expressions: a literal wrapped in {"expr": {"Literal": {"Value": ...}}}.
def lit(value):
    return {"expr": {"Literal": {"Value": value}}}


def text(value):
    return lit("'%s'" % value.replace("'", "''"))


def flag(value):
    return lit("true" if value else "false")


def number(value, suffix="D"):
    return lit("%s%s" % (value, suffix))


def solid(hex_color):
    return {"solid": {"color": text(hex_color)}}


def vis(kind, title, box, roles, sort=None, dropdown=False, header=None):
    """box = (x, y, w, h); roles = {role: [fields]}; sort = a field to sort descending by."""
    return {"kind": kind, "title": title, "box": box, "roles": roles, "sort": sort, "dropdown": dropdown, "header": header}


def row(n, y, h, x0=MARGIN, width=PAGE_W - 2 * MARGIN):
    """n equal boxes in a row."""
    w = (width - GAP * (n - 1)) / n
    return [(round(x0 + i * (w + GAP), 1), y, round(w, 1), h) for i in range(n)]


def header(title, subtitle):
    return vis("textbox", None, (0, 0, PAGE_W, HEADER_H), {}, header=(title, subtitle))


def slicer_row(fields):
    return [vis("slicer", None, box, {"Values": [f]}, dropdown=f != C("Date", "date"))
            for box, f in zip(row(len(fields), SLICER_Y, SLICER_H), fields)]


def card_row(measures, y=CARD_Y, h=CARD_H, x0=MARGIN, width=PAGE_W - 2 * MARGIN):
    return [vis("card", name, box, {"Values": [M(name)]}) for box, name in zip(row(len(measures), y, h, x0, width), measures)]


def page_layouts():
    """page id -> (tab name, visuals). The first visual of every page is the header band."""
    date = C("Date", "date")
    top, mid = BODY_Y, BODY_Y + 248          # two rows of the body: 240 and 240 high
    pages = {}
    pages["executive"] = ("Executive", [header("FunnelStream  |  Executive", "Revenue, orders and how they end, from the Gold layer in Databricks")]
        + slicer_row([date, C("Orders Daily", "channel"), C("Orders Daily", "platform")])
        + card_row(["Revenue Delivered", "Revenue Placed", "AOV", "Delivered Share of Revenue", "Orders Placed",
                    "Cancel Rate", "Revenue Placed vs Last Week"])
        + [vis("lineChart", "Revenue by day", (16, top, 760, 240),
               {"Category": [date], "Y": [M("Revenue Placed"), M("Revenue Delivered")]}),
           vis("clusteredBarChart", "Orders by channel", (784, top, 480, 240),
               {"Category": [C("Orders Daily", "channel")], "Y": [M("Orders Placed")]}),
           vis("tableEx", "Outcome rates by payment method", (16, mid, 760, 240),
               {"Values": [C("Orders Daily", "payment_method"), M("Orders Placed"), M("Cancel Rate"), M("Return Rate"),
                           M("Failed Attempt Rate")]}),
           vis("donutChart", "Orders by platform", (784, mid, 480, 240),
               {"Category": [C("Orders Daily", "platform")], "Y": [M("Orders Placed")]})])
    pages["campaign"] = ("Campaign", [header("FunnelStream  |  Campaigns", "Spend, return on ad spend and conversions per campaign; ad numbers are the ad API's UTC day")]
        + slicer_row([date, C("Campaign", "name"), C("Campaign", "platform")])
        + card_row(["Spend", "CPA", "Cost per Delivered Order", "CTR", "Conversion Rate", "ROAS Gap", "Maturity Label"])
        + [vis("clusteredBarChart", "ROAS placed and delivered by campaign", (16, top, 620, BODY_H),
               {"Category": [C("Campaign", "name")], "Y": [M("ROAS Placed"), M("ROAS Delivered")]}),
           vis("clusteredColumnChart", "Platform conversions against attributed orders", (644, top, 620, 240),
               {"Category": [C("Campaign", "name")], "Y": [M("Platform Conversions"), M("Orders on Spend Days")]}),
           vis("tableEx", "Campaign scorecard", (644, mid, 620, 240),
               {"Values": [C("Campaign", "name"), M("Spend"), M("Attributed Orders"), M("CPA"), M("ROAS Placed"), M("CTR")]},
               sort=M("Spend"))])
    pages["funnel"] = ("Funnel", [header("FunnelStream  |  Funnel", "From session to order: where shoppers drop out, by channel, platform and country")]
        + slicer_row([date, C("Funnel Daily", "channel"), C("Funnel Daily", "platform")])
        + card_row(["Cart Abandon Rate", "Checkout Abandon Rate", "Session Conversion"])
        + [vis("funnel", "Funnel stages", (16, top, 480, BODY_H),
               {"Y": [M("Sessions"), M("Product View Sessions"), M("Cart Sessions"), M("Checkout Sessions"),
                      M("Funnel Orders")]}),
           vis("clusteredBarChart", "Sessions and orders by channel", (504, top, 760, 240),
               {"Category": [C("Funnel Daily", "channel")], "Y": [M("Sessions"), M("Funnel Orders")]}),
           vis("clusteredBarChart", "Sessions and orders by platform", (504, mid, 370, 240),
               {"Category": [C("Funnel Daily", "platform")], "Y": [M("Sessions"), M("Funnel Orders")]}),
           vis("clusteredBarChart", "Sessions and orders by country", (882, mid, 382, 240),
               {"Category": [C("Funnel Daily", "country")], "Y": [M("Sessions"), M("Funnel Orders")]})])
    pages["inventory"] = ("Inventory", [header("FunnelStream  |  Inventory and express delivery", "Simulated stock in 8 dark stores; every express order carries a 20-minute promise")]
        + slicer_row([date, C("Store", "store_name"), C("Product", "category")])
        + card_row(["Out of Stock Lines", "Low Stock Lines", "Out of Stock Rate", "Stock Units Available",
                    "Stock Freshness (seconds)"])
        + [vis("lineChart", "Closing stock by day and store (simulated)", (16, top, 620, 240),
               {"Category": [date], "Y": [M("Closing Stock")], "Series": [C("Store", "store_name")]}),
           vis("clusteredColumnChart", "Units sold and received by day", (644, top, 620, 240),
               {"Category": [date], "Y": [M("Units Sold"), M("Units Received")]}),
           vis("tableEx", "Products that ended out of stock", (16, mid, 500, 240),
               {"Values": [C("Product", "name"), M("Ended Out of Stock"), M("Days of Cover"), M("Oversold Units")]},
               sort=M("Ended Out of Stock"))]
        + card_row(["Express Orders", "Express On-time Rate", "Express Breaches", "Slowest Express Delivery (min)"],
                   y=mid, h=84, x0=524, width=740)
        + [vis("clusteredBarChart", "Shrinkage rate by store", (524, mid + 92, 740, 148),
               {"Category": [C("Store", "store_name")], "Y": [M("Shrinkage Rate")]})])
    pages["marketing_inventory"] = ("Marketing and inventory", [header("FunnelStream  |  Marketing and inventory", "Ad spend that sent shoppers to empty shelves; an estimate, spend is the ad UTC day and demand the Cairo day")]
        + slicer_row([date, C("Campaign", "name")])
        + card_row(["Exposed Spend", "OOS Click Share", "Exposed Share of Spend", "Revenue at Risk", "OOS Add to Cart"])
        + [vis("clusteredBarChart", "Exposed spend next to spend by campaign (estimate)", (16, top, 620, 240),
               {"Category": [C("Campaign", "name")], "Y": [M("Exposed Spend"), M("Campaign Spend D1")]}),
           vis("lineChart", "Out-of-stock add to cart by day", (644, top, 620, 240),
               {"Category": [date], "Y": [M("OOS Add to Cart")]}),
           vis("tableEx", "Products by revenue at risk", (16, mid, 760, 240),
               {"Values": [C("Product", "name"), C("Store", "store_name"), M("Revenue at Risk"), M("Days of Cover")]},
               sort=M("Revenue at Risk")),
           vis("clusteredBarChart", "Revenue at risk by store", (784, mid, 480, 240),
               {"Category": [C("Store", "store_name")], "Y": [M("Revenue at Risk")]})])
    pages["products"] = ("Products and cities", [header("FunnelStream  |  Products and cities", "What shoppers view, add to the cart and buy, by product category and city")]
        + slicer_row([date, C("Product", "category"), C("City Product Daily", "city")])
        + card_row(["Product Revenue", "Product Units", "Product Sessions", "Product Add to Carts", "Product Cart Rate"])
        + [vis("treemap", "Revenue by product category", (16, top, 400, 240),
               {"Group": [C("Product", "category")], "Values": [M("Product Revenue")]}),
           vis("clusteredBarChart", "Revenue by city", (424, top, 420, 240),
               {"Category": [C("City Product Daily", "city")], "Y": [M("Product Revenue")]}),
           vis("clusteredColumnChart", "Units ordered by category (order lines)", (852, top, 412, 240),
               {"Category": [C("Product", "category")], "Y": [M("Units Ordered")]}),
           vis("lineChart", "Revenue by day", (16, mid, 400, 240),
               {"Category": [date], "Y": [M("Product Revenue")]}),
           vis("tableEx", "Top products", (424, mid, 840, 240),
               {"Values": [C("Product", "name"), C("Product", "category"), M("Product Units"), M("Product Revenue"),
                           M("Product Sessions"), M("Product Cart Rate")]},
               sort=M("Product Revenue"))])
    pages["customers_delivery"] = ("Customers and delivery", [header("FunnelStream  |  Customers and delivery", "Who orders, from where, and how fast and how reliably the order arrives")]
        + slicer_row([date, C("Orders", "city"), C("Orders", "carrier")])
        + card_row(["Active Customers", "Order Count", "Order Revenue", "Average Delivery Hours", "First Attempt Delivery Rate"])
        + [vis("clusteredBarChart", "Orders by city", (16, top, 400, BODY_H),
               {"Category": [C("Orders", "city")], "Y": [M("Order Count")]}),
           vis("donutChart", "Orders by status", (424, top, 420, 240),
               {"Category": [C("Orders", "current_status")], "Y": [M("Order Count")]}),
           vis("clusteredColumnChart", "Customers by device", (852, top, 412, 240),
               {"Category": [C("Customer", "primary_device")], "Y": [M("Active Customers")]}),
           vis("clusteredBarChart", "Average delivery hours by carrier", (424, mid, 420, 240),
               {"Category": [C("Orders", "carrier")], "Y": [M("Average Delivery Hours")]}),
           vis("clusteredColumnChart", "Orders by payment method", (852, mid, 412, 240),
               {"Category": [C("Orders", "payment_method")], "Y": [M("Order Count")]})])
    pages["customer_360"] = ("Customer 360", [header("FunnelStream  |  Customer 360", "One row per customer: how often they buy, what they buy and who has gone quiet; no date filter, this is the current state")]
        + slicer_row([C("Customer 360", "city"), C("Customer 360", "top_category"), C("Customer 360", "preferred_payment_method")])
        + card_row(["Customers on File", "Buyers", "Repeat Buyer Rate", "Lifetime Spend", "Average Spend per Buyer",
                    "Lapsed Buyers (30+ days)"])
        + [vis("clusteredBarChart", "Buyers by favourite category", (16, top, 400, 240),
               {"Category": [C("Customer 360", "top_category")], "Y": [M("Buyers")]}),
           vis("donutChart", "Preferred payment method", (424, top, 420, 240),
               {"Category": [C("Customer 360", "preferred_payment_method")], "Y": [M("Buyers")]}),
           vis("clusteredColumnChart", "One-time and repeat buyers", (852, top, 412, 240),
               {"Category": [C("Customer 360", "is_repeat_buyer")], "Y": [M("Buyers")]}),
           vis("clusteredBarChart", "Lifetime spend by city", (16, mid, 400, 240),
               {"Category": [C("Customer 360", "city")], "Y": [M("Lifetime Spend")]}),
           vis("tableEx", "Top customers by lifetime spend", (424, mid, 840, 240),
               {"Values": [C("Customer 360", "customer_id"), C("Customer 360", "city"), C("Customer 360", "top_category"),
                           C("Customer 360", "orders_count"), C("Customer 360", "total_spend"),
                           C("Customer 360", "days_since_last_order")]},
               sort=C("Customer 360", "total_spend"))])
    return pages


def theme():
    """The report theme: validated palette, status colours, fonts and quiet axes and gridlines."""
    def ink(hex_color):
        return {"solid": {"color": hex_color}}
    return {
        "name": "FunnelStream", "dataColors": CATEGORICAL,
        "background": SURFACE, "foreground": INK, "tableAccent": CATEGORICAL[0],
        "good": GOOD, "neutral": WARNING, "bad": CRITICAL,
        "maximum": CATEGORICAL[0], "center": "#f0efec", "minimum": CATEGORICAL[7],
        "textClasses": {
            "callout": {"fontSize": 24, "fontFace": FONT_BOLD, "color": NAVY},
            "title": {"fontSize": 12, "fontFace": FONT_BOLD, "color": INK},
            "header": {"fontSize": 11, "fontFace": FONT_BOLD, "color": INK},
            "label": {"fontSize": 10, "fontFace": FONT, "color": INK2}},
        "visualStyles": {"*": {"*": {
            "categoryAxis": [{"labelColor": ink(MUTED), "fontSize": 9, "fontFamily": FONT, "gridlineShow": False}],
            "valueAxis": [{"labelColor": ink(MUTED), "fontSize": 9, "fontFamily": FONT, "gridlineShow": True,
                           "gridlineColor": ink(GRID)}],
            "legend": [{"labelColor": ink(INK2), "fontSize": 10, "fontFamily": FONT, "position": "TopCenter"}]}}}}


def container_objects(spec):
    """Formatting of the visual's container: white rounded card with a hairline border, or the navy header band."""
    if spec["kind"] == "textbox":
        return {"background": [{"properties": {"show": flag(True), "color": solid(NAVY), "transparency": number(0)}}],
                "border": [{"properties": {"show": flag(False)}}]}
    obj = {"background": [{"properties": {"show": flag(True), "color": solid(SURFACE), "transparency": number(0)}}],
           "border": [{"properties": {"show": flag(True), "color": solid(GRID), "radius": number(8)}}]}
    if spec["title"] and spec["kind"] not in ("slicer", "card"):
        obj["title"] = [{"properties": {"show": flag(True), "text": text(spec["title"]), "fontColor": solid(INK),
                                        "fontSize": number(12), "bold": flag(True), "fontFamily": text(FONT_BOLD)}}]
    return obj


def visual_objects(spec):
    """Formatting that belongs to the chart itself (callout, data labels, table header, funnel colours)."""
    kind = spec["kind"]
    if kind == "card":
        return {"labels": [{"properties": {"color": solid(NAVY), "fontSize": number(24), "fontFamily": text(FONT_BOLD)}}],
                "categoryLabels": [{"properties": {"show": flag(True), "color": solid(INK2), "fontSize": number(10),
                                                   "fontFamily": text(FONT)}}]}
    if kind in ("clusteredBarChart", "clusteredColumnChart"):
        return {"labels": [{"properties": {"show": flag(True), "fontSize": number(9), "color": solid(INK2)}}]}
    if kind == "lineChart":
        return {"lineStyles": [{"properties": {"strokeWidth": number(3, "L")}}]}
    if kind == "tableEx":
        return {"columnHeaders": [{"properties": {"fontColor": solid("#ffffff"), "backColor": solid(NAVY), "fontSize": number(10),
                                                  "bold": flag(True)}}],
                "values": [{"properties": {"fontColor": solid(INK), "fontSize": number(10), "backColorPrimary": solid(SURFACE),
                                           "backColorSecondary": solid(PLANE)}}],
                "grid": [{"properties": {"gridHorizontal": flag(True), "gridHorizontalColor": solid(GRID),
                                         "gridVertical": flag(False)}}]}
    if kind == "funnel":
        return {"dataPoint": [{"properties": {"fill": solid(color)}, "selector": {"metadata": "_Measures." + f[1]}}
                              for f, color in zip(spec["roles"]["Y"], FUNNEL_RAMP)]}
    if kind == "slicer" and spec["dropdown"]:
        return {"data": [{"properties": {"mode": text("Dropdown")}}]}
    if kind == "textbox":
        title, subtitle = spec["header"]
        return {"general": [{"properties": {"paragraphs": [
            {"textRuns": [{"value": title, "textStyle": {"fontFamily": FONT_BOLD, "fontSize": "20pt", "fontWeight": "bold",
                                                         "color": "#ffffff"}}], "horizontalTextAlignment": "left"},
            {"textRuns": [{"value": subtitle, "textStyle": {"fontFamily": FONT, "fontSize": "10pt", "color": NAVY_SOFT}}],
             "horizontalTextAlignment": "left"}]}}]}
    return None


def visual_json(page, index, spec):
    x, y, w, h = spec["box"]
    assert set(spec["roles"]) <= set(ROLES[spec["kind"]]), spec
    visual = {"visualType": spec["kind"]}
    if ROLES[spec["kind"]]:
        state = {role: {"projections": [field_json(f) for f in spec["roles"][role]]}
                 for role in ROLES[spec["kind"]] if spec["roles"].get(role)}
        query = {"queryState": state}
        if spec["sort"]:
            query["sortDefinition"] = {"sort": [{"field": field_json(spec["sort"])["field"], "direction": "Descending"}],
                                       "isDefaultSort": True}
        visual["query"] = query
    objects = visual_objects(spec)
    if objects:
        visual["objects"] = objects
    visual["visualContainerObjects"] = container_objects(spec)
    visual["drillFilterOtherVisuals"] = True
    label = spec["title"] or (spec["header"][0] if spec["header"] else json.dumps(spec["roles"]))
    name = hashlib.sha1(("%s/%d/%s" % (page, index, label)).encode()).hexdigest()[:20]
    return {"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.0.0/schema.json",
            "name": name, "position": {"x": x, "y": y, "z": index, "height": h, "width": w, "tabOrder": index},
            "visual": visual}


def page_objects():
    """The page canvas and the area around it share one light plane."""
    return {"background": [{"properties": {"color": solid(PLANE), "transparency": number(0)}}],
            "outspace": [{"properties": {"color": solid(PLANE), "transparency": number(0)}}]}


def main():
    sm = OUT / "FunnelStream.SemanticModel"
    rp = OUT / "FunnelStream.Report"
    for d in (sm, rp):
        if d.exists():
            shutil.rmtree(d)
    (sm / "definition" / "tables").mkdir(parents=True)
    w = lambda p, t: p.write_text(t, encoding="utf-8", newline="\n")
    w(sm / "definition.pbism", json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
        "version": "4.2", "settings": {}}, indent=2))
    w(sm / "definition" / "database.tmdl", "database\n\tcompatibilityLevel: 1600\n")
    names = [t[0] for t in TABLES] + [t[0] for t in PG_TABLES] + ["_Measures"]
    model = ["model Model", "\tculture: en-US", "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
             "\tdiscourageImplicitMeasures", "\tsourceQueryCulture: en-US", "\tannotation __PBI_TimeIntelligenceEnabled = 0", ""]
    model += ["ref table " + q(n) for n in names] + [""]
    w(sm / "definition" / "model.tmdl", "\n".join(model))
    for name, gold, step in TABLES:
        w(sm / "definition" / "tables" / (name + ".tmdl"), table_tmdl(name, gold, step))
    for name, pg_table, cols in PG_TABLES:
        w(sm / "definition" / "tables" / (name + ".tmdl"), pg_table_tmdl(name, pg_table, cols))
    ms = parse_measures()
    mt = ["table _Measures", ""]
    mt += [measure_tmdl(m) for m in ms]
    mt += ["\tcolumn Dummy", "\t\tdataType: string", "\t\tisHidden", "\t\tsummarizeBy: none", "\t\tsourceColumn: Dummy", "",
           "\tpartition _Measures = m", "\t\tmode: import", "\t\tsource =",
           "\t\t\tlet", "\t\t\t\tSource = #table(type table [Dummy = text], {})", "\t\t\tin", "\t\t\t\tSource", ""]
    w(sm / "definition" / "tables" / "_Measures.tmdl", "\n".join(mt))
    rel = []
    for f_t, f_c, t_t, t_c in RELS:
        rel += ["relationship " + str(uuid.uuid5(uuid.NAMESPACE_URL, "funnelstream/%s.%s/%s.%s" % (f_t, f_c, t_t, t_c))), "\tfromColumn: %s.%s" % (q(f_t), f_c), "\ttoColumn: %s.%s" % (q(t_t), t_c), ""]
    w(sm / "definition" / "relationships.tmdl", "\n".join(rel))

    (rp / "definition" / "pages").mkdir(parents=True)
    w(rp / "definition.pbir", json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0", "datasetReference": {"byPath": {"path": "../FunnelStream.SemanticModel"}}}, indent=2))
    base = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/"
    w(rp / "definition" / "version.json", json.dumps({"$schema": base + "versionMetadata/1.0.0/schema.json", "version": "2.0.0"}, indent=2))
    at_import = {"page": "2.0.0", "report": "3.0.0", "visual": "2.0.0"}
    w(rp / "definition" / "report.json", json.dumps({"$schema": base + "report/3.0.0/schema.json", "themeCollection": {
        "baseTheme": {"name": "CY24SU10", "reportVersionAtImport": at_import, "type": "SharedResources"},
        "customTheme": {"name": "FunnelStreamTheme.json", "reportVersionAtImport": at_import, "type": "RegisteredResources"}},
        "resourcePackages": [{"name": "RegisteredResources", "type": "RegisteredResources", "items": [
            {"name": "FunnelStreamTheme.json", "path": "FunnelStreamTheme.json", "type": "CustomTheme"}]}]}, indent=2))
    (rp / "StaticResources" / "RegisteredResources").mkdir(parents=True)
    w(rp / "StaticResources" / "RegisteredResources" / "FunnelStreamTheme.json", json.dumps(theme(), indent=2))
    layouts = page_layouts()
    pages = [(pid, tab) for pid, (tab, _) in layouts.items()]
    w(rp / "definition" / "pages" / "pages.json", json.dumps({
        "$schema": base + "pagesMetadata/1.0.0/schema.json", "pageOrder": [p[0] for p in pages], "activePageName": "campaign"}, indent=2))
    for pid, title in pages:
        (rp / "definition" / "pages" / pid).mkdir()
        w(rp / "definition" / "pages" / pid / "page.json", json.dumps({
            "$schema": base + "page/2.0.0/schema.json", "name": pid, "displayName": title,
            "displayOption": "FitToPage", "height": 720, "width": 1280, "objects": page_objects()}, indent=2))
    n_visuals = 0
    for pid, (_, specs) in layouts.items():
        for i, spec in enumerate(specs):
            v = visual_json(pid, i, spec)
            vd = rp / "definition" / "pages" / pid / "visuals" / v["name"]
            vd.mkdir(parents=True)
            w(vd / "visual.json", json.dumps(v, indent=2))
            n_visuals += 1
    w(OUT / "FunnelStream.pbip", json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
        "version": "1.0", "artifacts": [{"report": {"path": "FunnelStream.Report"}}], "settings": {"enableAutoRecovery": True}}, indent=2))
    print("measures:", len(ms), "| tables:", len(names), "| visuals:", n_visuals)


if __name__ == "__main__":
    main()
