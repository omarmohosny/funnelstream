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
       "Cart Abandon Rate", "Checkout Abandon Rate", "Delivered Share", "Cancel Rate", "Return Rate",
       "Failed Attempt Rate", "New Customer Share", "Direct Share", "Revenue vs Prior 7 Days",
       "Out of Stock Rate", "Shrinkage Rate", "OOS Click Share", "Exposed Share of Spend", "Express On-time Rate",
       "Product Cart Rate", "First-Attempt Rate", "Repeat Buyer Rate"}
RATIO2 = {"ROAS Placed", "ROAS Delivered", "ROAS Gap", "Platform vs Attributed"}
ONE_DECIMAL = {"Days of Cover (max 90)", "Slowest Express (min)", "Average Express Delivery (min)", "Average Delivery Hours"}
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


# ---- design (version 4): the "Accessible Tidal" theme and a left rail + title banner layout on a 1280 x 720 canvas -----
import hashlib

PAGE_W, PAGE_H, GAP = 1280, 720, 8
RAIL_W = 200                                   # navy rail on the left: title, page navigator, slicers
MARGIN = 16
CX = RAIL_W + MARGIN                           # content starts here
CW = PAGE_W - CX - MARGIN                      # content width
BANNER_Y, BANNER_H = MARGIN, 64
CARD_Y, CARD_H = BANNER_Y + BANNER_H + GAP, 84
BODY_Y = CARD_Y + CARD_H + GAP
BODY_BOTTOM = PAGE_H - MARGIN
BODY_H = BODY_BOTTOM - BODY_Y
ROW_H = (BODY_H - GAP) // 2                    # two body rows with an 8 px gap
NAV_Y, NAV_H = 92, 236                         # page navigator in the rail
RAIL_SLICER_X, RAIL_SLICER_W = 12, RAIL_W - 24

# Colours: the palette of the "Accessible Tidal" Power BI theme (navy, blues, greens) with its own data colour order.
TIDAL_COLORS = ["#0F3D6E", "#1F84FF", "#19423F", "#59A472", "#0F3A69", "#0E94FF", "#0B2623", "#59A472", "#3599B8", "#DFBFBF",
                "#4AC5BB", "#5F6B6D", "#FB8281", "#F4D25A", "#7F898A", "#A4DDEE", "#FDAB89", "#B687AC", "#28738A", "#A78F8F",
                "#168980", "#293537", "#BB4A4A", "#B59525", "#475052", "#6A9FB0", "#BD7150", "#7B4F71", "#1B4D5C", "#706060",
                "#0F5C55", "#1C2325"]
CATEGORICAL = TIDAL_COLORS
FUNNEL_RAMP = ["#A9CFFB", "#6FB0FA", "#1F84FF", "#186DD5", "#0F3D6E"]   # one hue, light to dark, for the five funnel stages
NAVY, NAVY_SOFT, RAIL_ITEM = "#0F3D6E", "#D8E1EC", "#1B5493"
TIDAL_GREEN = "#59A472"
PLANE, SURFACE, OUTSPACE = "#EEF2F7", "#ffffff", "#D8E1EC"
INK, INK2, MUTED, GRID = "#121212", "#4A4F57", "#6C6966", "#DCE2EA"
GOOD, WARNING, SERIOUS, CRITICAL = "#46835A", "#F4D25A", "#FDAB89", "#BB4A4A"
FONT, FONT_BOLD = "Segoe UI", "Segoe UI Semibold"

ROLES = {"pageNavigator": (), "card": ("Values",), "slicer": ("Values",), "tableEx": ("Values",), "funnel": ("Category", "Y"),
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
    out = {"field": {kind: {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}},
           "queryRef": "%s.%s" % (entity, prop), "nativeQueryRef": prop}
    if kind == "Column":
        out["displayName"] = DISPLAY[(entity, prop)]   # KeyError names a column that still needs a readable name
    return out


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


def vis(kind, title, box, roles, sort=None, dropdown=False, header=None, variant=None):
    """box = (x, y, w, h); roles = {role: [fields]}; sort = a field to sort descending by."""
    return {"kind": kind, "title": title, "box": box, "roles": roles, "sort": sort, "dropdown": dropdown, "header": header,
            "variant": variant}


def row(n, y, h, x0=CX, width=CW):
    """n equal boxes in a row."""
    w = (width - GAP * (n - 1)) / n
    return [(round(x0 + i * (w + GAP), 1), y, round(w, 1), h) for i in range(n)]


def cols(weights, y, h):
    """Boxes side by side across the content area, widths in proportion to the weights."""
    usable = CW - GAP * (len(weights) - 1)
    out, x = [], CX
    for weight in weights:
        w = round(usable * weight / sum(weights), 1)
        out.append((round(x, 1), y, w, h))
        x += w + GAP
    return out


def header(title, subtitle):
    """The rail (navy, with the product name), the page navigator inside it, and the title banner of the page."""
    return [vis("textbox", None, (0, 0, RAIL_W, PAGE_H), {}, header=("FunnelStream", "Real-time e-commerce analytics"), variant="rail"),
            vis("pageNavigator", None, (RAIL_SLICER_X, NAV_Y, RAIL_SLICER_W, NAV_H), {}),
            vis("textbox", None, (CX, BANNER_Y, CW, BANNER_H), {}, header=(title, subtitle), variant="banner")]


def slicer_row(fields):
    """The slicers stack in the rail, under the page navigator: the date range first, then dropdowns."""
    out, y = [], NAV_Y + NAV_H + 16
    for f in fields:
        h = 80 if f == C("Date", "date") else 64
        out.append(vis("slicer", None, (RAIL_SLICER_X, y, RAIL_SLICER_W, h), {"Values": [f]}, dropdown=f != C("Date", "date")))
        y += h + GAP
    return out


def card_row(measures, y=CARD_Y, h=CARD_H, x0=CX, width=CW):
    return [vis("card", name, box, {"Values": [M(name)]}) for box, name in zip(row(len(measures), y, h, x0, width), measures)]


# Readable names shown in slicers, table headers and axes instead of the raw column names (the projection's displayName).
DISPLAY = {
    ("Date", "date"): "Date",
    ("Orders Daily", "channel"): "Channel", ("Orders Daily", "platform"): "Platform",
    ("Orders Daily", "payment_method"): "Payment method",
    ("Campaign", "name"): "Campaign", ("Campaign", "platform"): "Platform",
    ("Funnel Daily", "channel"): "Channel", ("Funnel Daily", "platform"): "Platform",
    ("Store", "store_name"): "Store",
    ("Product", "category"): "Category", ("Product", "name"): "Product",
    ("City Product Daily", "city"): "City",
    ("Orders", "carrier"): "Carrier",
    ("Customer 360", "city"): "City", ("Customer 360", "top_category"): "Favourite category",
    ("Customer 360", "preferred_payment_method"): "Preferred payment",
    ("Customer 360", "is_repeat_buyer"): "Repeat buyer", ("Customer 360", "customer_id"): "Customer",
    ("Customer 360", "orders_count"): "Orders", ("Customer 360", "total_spend"): "Lifetime spend",
    ("Customer 360", "days_since_last_order"): "Days since last order",
}


def page_layouts():
    """page id -> (tab name, visuals), in tab order. Each page starts with the rail, the page navigator and the title banner
    (header), then the slicers in the rail, the cards and the charts. Seven pages, one question each; no pie, donut or
    treemap; every chart title says what is shown."""
    date = C("Date", "date")
    top, mid = BODY_Y, BODY_Y + ROW_H + GAP          # two rows of the body, ROW_H high each
    pages = {}
    channel, platform, payment = C("Orders Daily", "channel"), C("Orders Daily", "platform"), C("Orders Daily", "payment_method")
    t1, t2 = cols([3, 2], top, ROW_H), cols([3, 2], mid, ROW_H)
    pages["executive"] = ("Executive", header("FunnelStream  |  Executive", "Is revenue on track, and where do orders leak? The simulated data has a one-day volume peak on 9 October")
        + slicer_row([date, channel, platform])
        + card_row(["Revenue Placed", "Revenue Delivered", "Orders Placed", "AOV", "Delivered Share", "Cancel Rate"])
        + [vis("lineChart", "Revenue by day (one-day volume peak on 9 Oct)", t1[0],
               {"Category": [date], "Y": [M("Revenue Placed"), M("Revenue Delivered")]}),
           vis("clusteredBarChart", "Orders by channel", t1[1],
               {"Category": [channel], "Y": [M("Orders Placed")]}, sort=M("Orders Placed")),
           vis("clusteredBarChart", "Where orders leak, by payment method", t2[0],
               {"Category": [payment], "Y": [M("Cancel Rate"), M("Return Rate"), M("Failed Attempt Rate")]}),
           vis("clusteredBarChart", "Orders by payment method", t2[1],
               {"Category": [payment], "Y": [M("Orders Placed")]}, sort=M("Orders Placed"))])
    camp, camp_platform = C("Campaign", "name"), C("Campaign", "platform")
    left, right_top, right_low = cols([1, 1], top, BODY_H), cols([1, 1], top, ROW_H), cols([1, 1], mid, ROW_H)
    pages["campaign"] = ("Campaign", header("FunnelStream  |  Campaigns", "Where should the next budget go? Ad numbers are the ad API's UTC day; the latest days are still settling")
        + slicer_row([date, camp, camp_platform])
        + card_row(["Spend", "CPA", "CTR", "Conversion Rate", "ROAS Delivered"])
        + [vis("clusteredBarChart", "Return on ad spend by campaign: delivered and placed", left[0],
               {"Category": [camp], "Y": [M("ROAS Delivered"), M("ROAS Placed")]}, sort=M("ROAS Delivered")),
           vis("clusteredBarChart", "Cost per delivered order by campaign", right_top[1],
               {"Category": [camp], "Y": [M("Cost per Delivered Order")]}, sort=M("Cost per Delivered Order")),
           vis("tableEx", "Campaign scorecard", right_low[1],
               {"Values": [camp, M("Spend"), M("Orders on Spend Days"), M("CPA"), M("ROAS Delivered"), M("CTR")]},
               sort=M("Spend"))])
    store, category, product = C("Store", "store_name"), C("Product", "category"), C("Product", "name")
    pages["marketing_inventory"] = ("Marketing and stock", header("FunnelStream  |  Marketing and stock", "Which campaign sent shoppers to empty shelves? An estimate: spend is the ad UTC day and demand the Cairo day")
        + slicer_row([date, camp])
        + card_row(["Exposed Spend", "OOS Click Share", "Exposed Share of Spend", "Revenue at Risk", "OOS Add to Cart"])
        + [vis("clusteredBarChart", "Exposed spend and campaign spend (D-1)", t1[0],
               {"Category": [camp], "Y": [M("Exposed Spend"), M("Campaign Spend D1")]}),
           vis("clusteredColumnChart", "Out-of-stock add to cart by category", t1[1],
               {"Category": [category], "Y": [M("OOS Add to Cart")]}, sort=M("OOS Add to Cart")),
           vis("tableEx", "Products by revenue at risk (all campaigns)", t2[0],
               {"Values": [product, store, M("Revenue at Risk")]}, sort=M("Revenue at Risk")),
           vis("clusteredBarChart", "Revenue at risk by category", t2[1],
               {"Category": [category], "Y": [M("Revenue at Risk")]}, sort=M("Revenue at Risk"))])
    th, tm = cols([1, 1, 1], top, ROW_H), cols([1, 1, 1], mid, ROW_H)
    pages["inventory"] = ("Operations", header("FunnelStream  |  Operations", "What to restock today, and is the 20-minute express promise holding? The stock cards are the current snapshot of 8 dark stores")
        + slicer_row([date, store, category])
        + card_row(["Out of Stock Lines", "Low Stock Lines", "Out of Stock Rate", "Express Orders", "Express On-time Rate",
                    "Express Breaches"])
        + [vis("clusteredBarChart", "Oversold units by store", th[0],
               {"Category": [store], "Y": [M("Oversold Units")]}, sort=M("Oversold Units")),
           vis("clusteredColumnChart", "Units sold and received by day", th[1],
               {"Category": [date], "Y": [M("Units Sold"), M("Units Received")]}),
           vis("clusteredColumnChart", "Shrinkage rate by store", th[2],
               {"Category": [store], "Y": [M("Shrinkage Rate")]}),
           vis("tableEx", "Products with oversold units", tm[0],
               {"Values": [product, M("Oversold Units"), M("Days of Cover (max 90)")]}, sort=M("Oversold Units")),
           vis("clusteredColumnChart", "Express breaches by store", tm[1],
               {"Category": [store], "Y": [M("Express Breaches")]}),
           vis("clusteredBarChart", "Delivery hours by carrier (all orders)", tm[2],
               {"Category": [C("Orders", "carrier")], "Y": [M("Average Delivery Hours")]})])
    fchannel = C("Funnel Daily", "channel")
    fl, fr_top, fr_low = cols([2, 3], top, BODY_H), cols([2, 3], top, ROW_H), cols([2, 3], mid, ROW_H)
    pages["funnel"] = ("Funnel", header("FunnelStream  |  Funnel", "Which step and which channel should we fix first? From session to order, by channel")
        + slicer_row([date, fchannel, C("Funnel Daily", "platform")])
        + card_row(["Cart Abandon Rate", "Checkout Abandon Rate", "Session Conversion"])
        + [vis("funnel", "Funnel stages", fl[0],
               {"Y": [M("Sessions"), M("Product View Sessions"), M("Cart Sessions"), M("Checkout Sessions"),
                      M("Funnel Orders")]}),
           vis("clusteredBarChart", "Cart and checkout abandon rate by channel", fr_top[1],
               {"Category": [fchannel], "Y": [M("Cart Abandon Rate"), M("Checkout Abandon Rate")]}),
           vis("clusteredBarChart", "Sessions by channel", fr_low[1],
               {"Category": [fchannel], "Y": [M("Sessions")]}, sort=M("Sessions"))])
    pcategory, city = C("Product", "category"), C("City Product Daily", "city")
    pages["products"] = ("Products and cities", header("FunnelStream  |  Products and cities", "What should we stock and promote, and where? Revenue, add-to-cart and cart rate by category and city")
        + slicer_row([date, pcategory, city])
        + card_row(["Product Revenue", "Product Units", "Product Page Views", "Product Add to Carts", "Product Cart Rate"])
        + [vis("clusteredBarChart", "Revenue by product category", th[0],
               {"Category": [pcategory], "Y": [M("Product Revenue")]}, sort=M("Product Revenue")),
           vis("clusteredBarChart", "Revenue by city", th[1],
               {"Category": [city], "Y": [M("Product Revenue")]}, sort=M("Product Revenue")),
           vis("clusteredColumnChart", "Add to cart by category", th[2],
               {"Category": [pcategory], "Y": [M("Product Add to Carts")]}),
           vis("tableEx", "Top products", (CX, mid, CW, ROW_H),
               {"Values": [C("Product", "name"), pcategory, M("Product Units"), M("Product Revenue"),
                           M("Product Page Views"), M("Product Cart Rate")]},
               sort=M("Product Revenue"))])
    c360 = lambda col: C("Customer 360", col)
    cm = cols([1, 2], mid, ROW_H)
    pages["customers"] = ("Customers", header("FunnelStream  |  Customers", "Who buys again, and who should we win back? One row per customer, the current state, no date filter")
        + slicer_row([c360("city"), c360("top_category"), c360("preferred_payment_method")])
        + card_row(["Customers on File", "Buyers", "Repeat Buyer Rate", "Lifetime Spend", "Average Spend per Buyer",
                    "Quiet Buyers (7+ days)"])
        + [vis("clusteredBarChart", "Buyers by favourite category", th[0],
               {"Category": [c360("top_category")], "Y": [M("Buyers")]}, sort=M("Buyers")),
           vis("clusteredBarChart", "Buyers by preferred payment", th[1],
               {"Category": [c360("preferred_payment_method")], "Y": [M("Buyers")]}, sort=M("Buyers")),
           vis("clusteredColumnChart", "Average spend per buyer by category", th[2],
               {"Category": [c360("top_category")], "Y": [M("Average Spend per Buyer")]}),
           vis("clusteredBarChart", "Lifetime spend by city", cm[0],
               {"Category": [c360("city")], "Y": [M("Lifetime Spend")]}, sort=M("Lifetime Spend")),
           vis("tableEx", "Top customers by lifetime spend", cm[1],
               {"Values": [c360("customer_id"), c360("city"), c360("top_category"), c360("orders_count"),
                           c360("total_spend"), c360("days_since_last_order")]},
               sort=c360("total_spend"))])
    return pages


def theme():
    """The report theme: the Accessible Tidal palette and status colours, our fonts, and quiet axes and gridlines."""
    def ink(hex_color):
        return {"solid": {"color": hex_color}}
    return {
        "name": "Accessible Tidal", "dataColors": CATEGORICAL,
        "foreground": INK, "background": SURFACE, "foregroundNeutralSecondary": "#6C6966", "backgroundLight": "#E5E0DA",
        "foregroundNeutralTertiary": "#ABA7A2", "backgroundNeutral": "#C6C2BE", "tableAccent": "#4E9466",
        "maximum": "#0B2F54", "center": TIDAL_GREEN, "minimum": "#0E94FF",
        "bad": NAVY, "neutral": "#186DD5", "good": "#46835A",
        "textClasses": {
            "callout": {"fontSize": 24, "fontFace": FONT_BOLD, "color": NAVY},
            "title": {"fontSize": 12, "fontFace": FONT_BOLD, "color": INK},
            "header": {"fontSize": 11, "fontFace": FONT_BOLD, "color": INK},
            "label": {"fontSize": 10, "fontFace": FONT, "color": INK2}},
        "visualStyles": {"*": {"*": {
            "categoryAxis": [{"labelColor": ink(MUTED), "fontSize": 9, "fontFamily": FONT, "gridlineShow": False,
                              "showAxisTitle": False}],
            "valueAxis": [{"labelColor": ink(MUTED), "fontSize": 9, "fontFamily": FONT, "gridlineShow": True, "showAxisTitle": False,
                           "gridlineColor": ink(GRID)}],
            "legend": [{"labelColor": ink(INK2), "fontSize": 10, "fontFamily": FONT, "position": "TopCenter"}]}}}}


def container_objects(spec):
    """Formatting of the visual's container: white rounded card with a hairline border, or the navy header band."""
    if spec["kind"] == "textbox" and spec["variant"] == "rail":
        return {"background": [{"properties": {"show": flag(True), "color": solid(NAVY), "transparency": number(0)}}],
                "border": [{"properties": {"show": flag(False)}}]}
    if spec["kind"] == "textbox":      # the title banner: a white card with a navy accent in the text
        return {"background": [{"properties": {"show": flag(True), "color": solid(SURFACE), "transparency": number(0)}}],
                "border": [{"properties": {"show": flag(True), "color": solid(GRID), "radius": number(8)}}]}
    if spec["kind"] == "pageNavigator":
        return {"background": [{"properties": {"show": flag(False)}}], "border": [{"properties": {"show": flag(False)}}]}
    obj = {"background": [{"properties": {"show": flag(True), "color": solid(SURFACE), "transparency": number(0)}}],
           "border": [{"properties": {"show": flag(True), "color": solid(GRID), "radius": number(8)}}]}
    if spec["title"] and spec["kind"] not in ("slicer", "card"):
        obj["title"] = [{"properties": {"show": flag(True), "text": text(spec["title"]), "fontColor": solid(INK),
                                        "fontSize": number(12), "bold": flag(True), "fontFamily": text(FONT_BOLD)}}]
    return obj


def table_column_widths(spec):
    """Explicit column widths that add up to the table's inner width, so the table fills its box: a text column weighs
    1.6 and a measure column 1. The selector names the column by its queryRef."""
    fields = spec["roles"]["Values"]
    weights = [1 if f[0] == "m" else 1.6 for f in fields]
    inner = spec["box"][2] - 28                     # card padding and the scroll bar
    out = []
    for f, weight in zip(fields, weights):
        ref = field_json(f)["queryRef"]
        out.append({"properties": {"value": number(round(inner * weight / sum(weights)))}, "selector": {"metadata": ref}})
    return out


def visual_objects(spec):
    """Formatting that belongs to the chart itself (callout, data labels, table header, funnel colours)."""
    kind = spec["kind"]
    if kind == "card":
        return {"labels": [{"properties": {"color": solid(NAVY), "fontSize": number(24), "fontFamily": text(FONT_BOLD),
                                           "labelDisplayUnits": number(1)}}],
                "categoryLabels": [{"properties": {"show": flag(True), "color": solid(INK2), "fontSize": number(10),
                                                   "fontFamily": text(FONT)}}]}
    if kind in ("clusteredBarChart", "clusteredColumnChart"):
        return {"labels": [{"properties": {"show": flag(True), "fontSize": number(9), "color": solid(INK2)}}]}
    if kind == "lineChart":
        return {"lineStyles": [{"properties": {"strokeWidth": number(3, "L")}}]}
    if kind == "tableEx":
        return {"columnHeaders": [{"properties": {"fontColor": solid("#ffffff"), "backColor": solid(NAVY), "fontSize": number(11),
                                                  "bold": flag(True)}}],
                "values": [{"properties": {"fontColor": solid(INK), "fontSize": number(11), "backColorPrimary": solid(SURFACE),
                                           "backColorSecondary": solid(PLANE)}}],
                "grid": [{"properties": {"gridHorizontal": flag(True), "gridHorizontalColor": solid(GRID),
                                         "gridVertical": flag(False), "rowPadding": number(3)}}],
                "columnWidth": table_column_widths(spec)}
    if kind == "funnel":
        return {"labels": [{"properties": {"labelDisplayUnits": number(1)}}],
                "dataPoint": [{"properties": {"fill": solid(color)}, "selector": {"metadata": "_Measures." + f[1]}}
                              for f, color in zip(spec["roles"]["Y"], FUNNEL_RAMP)]}
    if kind == "slicer" and spec["dropdown"]:
        return {"data": [{"properties": {"mode": text("Dropdown")}}]}
    if kind == "textbox" and spec["variant"] == "rail":
        title, subtitle = spec["header"]
        return {"general": [{"properties": {"paragraphs": [
            {"textRuns": [{"value": title, "textStyle": {"fontFamily": FONT_BOLD, "fontSize": "18pt", "fontWeight": "bold",
                                                         "color": "#ffffff"}}], "horizontalTextAlignment": "left"},
            {"textRuns": [{"value": subtitle, "textStyle": {"fontFamily": FONT, "fontSize": "9pt", "color": NAVY_SOFT}}],
             "horizontalTextAlignment": "left"}]}}]}
    if kind == "textbox":
        title, subtitle = spec["header"]
        return {"general": [{"properties": {"paragraphs": [
            {"textRuns": [{"value": title, "textStyle": {"fontFamily": FONT_BOLD, "fontSize": "18pt", "fontWeight": "bold",
                                                         "color": NAVY}}], "horizontalTextAlignment": "left"},
            {"textRuns": [{"value": subtitle, "textStyle": {"fontFamily": FONT, "fontSize": "9pt", "color": INK2}}],
             "horizontalTextAlignment": "left"}]}}]}
    if kind == "pageNavigator":
        def sel(name):
            return {"id": name}
        return {"layout": [{"properties": {"orientation": lit("1D"), "cellPadding": number(6, "L")}}],
                "shape": [{"properties": {"tileShape": text("rectangleRounded"), "rectangleRoundedCurve": number(8, "L")},
                           "selector": sel("default")}],
                "text": [{"properties": {"fontColor": solid("#ffffff"), "fontSize": number(10), "fontFamily": text(FONT)},
                          "selector": sel("default")}],
                "fill": [{"properties": {"fillColor": solid(RAIL_ITEM), "transparency": number(0)}, "selector": sel("default")},
                         {"properties": {"fillColor": solid(TIDAL_GREEN), "transparency": number(0)}, "selector": sel("selected")}],
                "outline": [{"properties": {"show": flag(False)}}],
                "shadow": [{"properties": {"show": flag(False)}}]}
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
    label = spec["title"] or ((spec["variant"] or "") + (spec["header"][0] if spec["header"] else json.dumps(spec["roles"])))
    name = hashlib.sha1(("%s/%d/%s" % (page, index, label)).encode()).hexdigest()[:20]
    return {"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.0.0/schema.json",
            "name": name, "position": {"x": x, "y": y, "z": index, "height": h, "width": w, "tabOrder": index},
            "visual": visual}


def page_objects():
    """A light plane for the page; the area around the canvas is the theme's blue-grey."""
    return {"background": [{"properties": {"color": solid(PLANE), "transparency": number(0)}}],
            "outspace": [{"properties": {"color": solid(OUTSPACE), "transparency": number(0)}}]}


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
