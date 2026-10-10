"""Generate dashboards/powerbi/FunnelStream.pbip (Gold part of the model) from samples/*.csv and measures.dax."""
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
    ("Product", "dim_product", 'Table.SelectRows(#"Selected", each [is_current] = true)'),
    ("Campaign Daily", "agg_campaign_daily", 'Table.SelectRows(#"Selected", each [attribution_model] = "last_touch")'),
    ("Funnel Daily", "agg_funnel_daily", None),
    ("Orders Daily", "agg_orders_daily", None),
    ("Orders", "fact_orders", None),
]
PRODUCT_KEEP = ["product_id", "name", "category", "price", "is_current"]
INT_NAMES = {"impressions", "clicks", "platform_conversions", "orders_placed", "orders_delivered", "orders_cancelled",
             "orders_returned", "orders_with_failed_attempt", "new_customer_orders"}
DOUBLE_NAMES = {"spend", "revenue_placed", "revenue_delivered", "aov", "cpa", "cost_per_delivered_order", "roas_placed",
                "roas_delivered", "ctr", "conversion_rate", "cancel_rate", "failed_attempt_rate", "return_rate", "open_rate",
                "cart_abandon_rate", "checkout_abandon_rate", "price", "order_total", "delivery_lead_hours"}
DATE_NAMES = {"date", "report_date", "event_date", "valid_from", "valid_to", "start_date", "end_date"}
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


PCT = {"CTR", "Conversion Rate", "View Rate", "Cart Rate", "Checkout Rate", "Order Rate", "Session Conversion",
       "Cart Abandon Rate", "Checkout Abandon Rate", "Delivered Share of Revenue", "Cancel Rate", "Return Rate",
       "Failed Attempt Rate", "New Customer Share", "Direct Share", "Revenue Placed vs Last Week"}
RATIO2 = {"ROAS Placed", "ROAS Delivered", "ROAS Gap", "Platform vs Attributed"}
TEXT = {"Maturity Label", "Is Provisional"}


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
    keep = ("Campaign", "Funnel", "Executive")
    return [m for m in out if m["folder"] and m["folder"].split("\\")[0] in keep]


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
]


def main():
    sm = OUT / "FunnelStream.SemanticModel"
    rp = OUT / "FunnelStream.Report"
    for d in (sm, rp):
        if d.exists():
            shutil.rmtree(d)
    (sm / "definition" / "tables").mkdir(parents=True)
    w = lambda p, t: p.write_text(t, encoding="utf-8", newline="\n")
    w(sm / "definition.pbism", json.dumps({"version": "4.2", "settings": {}}, indent=2))
    w(sm / "definition" / "database.tmdl", "database\n\tcompatibilityLevel: 1600\n")
    names = [t[0] for t in TABLES] + ["_Measures"]
    model = ["model Model", "\tculture: en-US", "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
             "\tdiscourageImplicitMeasures", "\tsourceQueryCulture: en-US", ""]
    model += ["ref table " + q(n) for n in names] + [""]
    w(sm / "definition" / "model.tmdl", "\n".join(model))
    for name, gold, step in TABLES:
        w(sm / "definition" / "tables" / (name + ".tmdl"), table_tmdl(name, gold, step))
    ms = parse_measures()
    mt = ["table _Measures", ""]
    mt += [measure_tmdl(m) for m in ms]
    mt += ["\tcolumn Dummy", "\t\tdataType: string", "\t\tisHidden", "\t\tsummarizeBy: none", "\t\tsourceColumn: Dummy", "",
           "\tpartition _Measures = m", "\t\tmode: import", "\t\tsource =",
           "\t\t\tlet", "\t\t\t\tSource = #table(type table [Dummy = text], {})", "\t\t\tin", "\t\t\t\tSource", ""]
    w(sm / "definition" / "tables" / "_Measures.tmdl", "\n".join(mt))
    rel = []
    for f_t, f_c, t_t, t_c in RELS:
        rel += ["relationship " + str(uuid.uuid4()), "\tfromColumn: %s.%s" % (q(f_t), f_c), "\ttoColumn: %s.%s" % (q(t_t), t_c), ""]
    w(sm / "definition" / "relationships.tmdl", "\n".join(rel))

    (rp / "definition" / "pages").mkdir(parents=True)
    w(rp / "definition.pbir", json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0", "datasetReference": {"byPath": {"path": "../FunnelStream.SemanticModel"}}}, indent=2))
    base = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/"
    w(rp / "definition" / "version.json", json.dumps({"$schema": base + "versionMetadata/1.0.0/schema.json", "version": "2.0.0"}, indent=2))
    w(rp / "definition" / "report.json", json.dumps({"$schema": base + "report/3.0.0/schema.json", "themeCollection": {
        "baseTheme": {"name": "CY24SU10", "reportVersionAtImport": "5.61", "type": "SharedResources"}}}, indent=2))
    pages = [("executive", "Executive"), ("campaign", "Campaign"), ("funnel", "Funnel")]
    w(rp / "definition" / "pages" / "pages.json", json.dumps({
        "$schema": base + "pagesMetadata/1.0.0/schema.json", "pageOrder": [p[0] for p in pages], "activePageName": "campaign"}, indent=2))
    for pid, title in pages:
        (rp / "definition" / "pages" / pid).mkdir()
        w(rp / "definition" / "pages" / pid / "page.json", json.dumps({
            "$schema": base + "page/2.0.0/schema.json", "name": pid, "displayName": title,
            "displayOption": "FitToPage", "height": 720, "width": 1280}, indent=2))
    w(OUT / "FunnelStream.pbip", json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
        "version": "1.0", "artifacts": [{"report": {"path": "FunnelStream.Report"}}], "settings": {"enableAutoRecovery": True}}, indent=2))
    print("measures:", len(ms), "| tables:", len(names))


main()
