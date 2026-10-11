"""The Power BI project generator (scripts/make_pbip.py) cannot be opened in Power BI Desktop by a test, so these
checks catch what a person would otherwise find by opening it: a Postgres column that does not exist, a measure that
points at a missing column, a relationship to a missing table."""
import re
from pathlib import Path

import pytest

from scripts import make_pbip

ROOT = Path(__file__).resolve().parent.parent
PG_TYPE = {"text": "string", "integer": "int64", "bigint": "int64", "numeric": "decimal", "date": "dateTime",
           "timestamptz": "dateTime", "boolean": "boolean"}


@pytest.fixture(scope="module")
def project(tmp_path_factory):
    out = tmp_path_factory.mktemp("pbip")
    saved = make_pbip.OUT
    make_pbip.OUT = out
    try:
        make_pbip.main()
    finally:
        make_pbip.OUT = saved
    return out


def ddl_columns(table):
    text = (ROOT / "warehouse" / "live_ddl.sql").read_text(encoding="utf-8")
    body = re.search(r"CREATE TABLE IF NOT EXISTS %s \((.*?)\n\);" % table, text, re.S).group(1)
    cols = []
    for line in body.splitlines():
        m = re.match(r"\s+([a-z_0-9]+)\s+(text|integer|bigint|numeric(?:\(\d+,\s*\d+\))?|date|timestamptz|boolean)\b", line)
        if m:
            cols.append((m.group(1), PG_TYPE[m.group(2).split("(")[0]]))
    return cols


@pytest.mark.parametrize("name,table,cols", make_pbip.PG_TABLES)
def test_postgres_columns_equal_the_ddl(name, table, cols):
    assert cols == ddl_columns(table)


def model_columns(project):
    tables = {}
    for f in (project / "FunnelStream.SemanticModel" / "definition" / "tables").glob("*.tmdl"):
        text = f.read_text(encoding="utf-8")
        name = re.match(r"table (.+)", text).group(1).strip("'")
        tables[name] = {c.strip("'") for c in re.findall(r"^\tcolumn (.+)$", text, re.M)}
        tables[name] |= {m.strip("'") for m in re.findall(r"^\tmeasure (.+?) =", text, re.M)}
    return tables


def test_every_table_is_in_the_model_and_has_a_source(project):
    d = project / "FunnelStream.SemanticModel" / "definition"
    listed = re.findall(r"^ref table (.+)$", (d / "model.tmdl").read_text(encoding="utf-8"), re.M)
    files = {f.stem for f in (d / "tables").glob("*.tmdl")}
    assert {t.strip("'") for t in listed} == files and len(files) == 18
    for f in (d / "tables").glob("*.tmdl"):
        assert "\tpartition " in f.read_text(encoding="utf-8"), f.name


def test_every_measure_points_at_existing_columns_and_measures(project):
    tables = model_columns(project)
    text = (project / "FunnelStream.SemanticModel" / "definition" / "tables" / "_Measures.tmdl").read_text(encoding="utf-8")
    refs = re.findall(r"'([A-Za-z0-9 ]+)'\[([A-Za-z0-9_ ()\-]+)\]|\b([A-Za-z0-9_]+)\[([A-Za-z0-9_ ()\-]+)\]", text)
    missing = []
    for qt, qc, ut, uc in refs:
        t, c = (qt, qc) if qt else (ut, uc)
        if t in ("VAR", "RETURN"):
            continue
        # DAX matches table and column names case-insensitively ('Date'[Date] is the column "date")
        by_lower = {name.lower(): {col.lower() for col in cols} for name, cols in tables.items()}
        if t.lower() not in by_lower or c.lower() not in by_lower[t.lower()]:
            missing.append("%s[%s]" % (t, c))
    assert not missing, sorted(set(missing))


def test_every_measure_of_measures_dax_is_in_the_model(project):
    text = (project / "FunnelStream.SemanticModel" / "definition" / "tables" / "_Measures.tmdl").read_text(encoding="utf-8")
    generated = len(re.findall(r"^\tmeasure ", text, re.M))
    assert generated == len(make_pbip.parse_measures()) >= 80
    folders = set(re.findall(r"^\t\tdisplayFolder: (.+)$", text, re.M))
    for needed in ("Inventory", "Marketing and inventory", "Express delivery (the 20-minute promise)", "Products and cities", "Customers and delivery"):
        assert any(f.startswith(needed) for f in folders), needed


def test_relationships_connect_existing_columns_and_stay_many_to_one(project):
    tables = model_columns(project)
    text = (project / "FunnelStream.SemanticModel" / "definition" / "relationships.tmdl").read_text(encoding="utf-8")
    pairs = re.findall(r"fromColumn: (.+?)\.(\w+)\n\ttoColumn: (.+?)\.(\w+)", text)
    assert len(pairs) == len(make_pbip.RELS) == 27
    seen = set()
    for ft, fc, tt, tc in pairs:
        ft, tt = ft.strip("'"), tt.strip("'")
        assert fc in tables[ft] and tc in tables[tt], (ft, fc, tt, tc)
        assert tt in ("Date", "Campaign", "Product", "Store", "Customer"), "the one side must be a dimension: " + tt
        assert (ft, fc) not in seen, "duplicate relationship from " + ft + "." + fc
        seen.add((ft, fc))


def test_the_report_has_the_five_pages_of_the_guide(project):
    pages = (project / "FunnelStream.Report" / "definition" / "pages" / "pages.json").read_text(encoding="utf-8")
    for page in ("executive", "campaign", "funnel", "inventory", "marketing_inventory", "products", "customers_delivery"):
        assert '"%s"' % page in pages
        assert (project / "FunnelStream.Report" / "definition" / "pages" / page / "page.json").exists()


def test_report_json_has_the_theme_version_object_power_bi_requires(project):
    """Power BI Desktop refused the first project: reportVersionAtImport was the string "5.61", but the
    report/3.0.0 schema wants an object with page, report and visual versions (scripts/validate_pbip.py checks every file)."""
    import json
    report = json.loads((project / "FunnelStream.Report" / "definition" / "report.json").read_text(encoding="utf-8"))
    version = report["themeCollection"]["baseTheme"]["reportVersionAtImport"]
    assert isinstance(version, dict) and set(version) == {"page", "report", "visual"}
    assert all(re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", v) for v in version.values())


def test_definition_pbism_names_its_schema(project):
    """The semanticModel/definitionProperties schema marks $schema as required; Power BI Desktop checks it."""
    import json
    doc = json.loads((project / "FunnelStream.SemanticModel" / "definition.pbism").read_text(encoding="utf-8"))
    assert doc["$schema"].endswith("/item/semanticModel/definitionProperties/1.0.0/schema.json")
    assert isinstance(doc["version"], str)


def test_product_filter_is_not_folded_into_a_boolean_comparison_databricks_rejects(project):
    """Power BI Desktop folded [is_current] = true into SQL as `is_current = 1` and Databricks refused it
    (DATATYPE_MISMATCH BOOLEAN against INT), which also cancelled the load of every other table. Product is 200 rows,
    so the filter runs in Power BI after the table is buffered."""
    text = (project / "FunnelStream.SemanticModel" / "definition" / "tables" / "Product.tmdl").read_text(encoding="utf-8")
    assert "Table.Buffer(Raw)" in text and "[is_current] = true" in text


def test_no_measure_names_a_variable_with_a_dax_reserved_word(project):
    """`VAR Current` was a syntax error in Power BI Desktop ("The syntax for 'Current' is incorrect")."""
    text = (project / "FunnelStream.SemanticModel" / "definition" / "tables" / "_Measures.tmdl").read_text(encoding="utf-8")
    names = {n.lower() for n in re.findall(r"\bVAR\s+([A-Za-z_][A-Za-z0-9_]*)", text)}
    assert names, "the measures use VAR"
    assert not names & {"current"}


def test_automatic_date_tables_are_switched_off(project):
    """The model has its own marked Date table; Desktop otherwise adds a LocalDateTable per date column."""
    text = (project / "FunnelStream.SemanticModel" / "definition" / "model.tmdl").read_text(encoding="utf-8")
    assert "annotation __PBI_TimeIntelligenceEnabled = 0" in text


def test_regenerating_gives_the_same_relationship_ids(project, tmp_path):
    """Random ids made every regeneration a diff of all 22 relationships."""
    saved = make_pbip.OUT
    make_pbip.OUT = tmp_path
    try:
        make_pbip.main()
    finally:
        make_pbip.OUT = saved
    name = Path("FunnelStream.SemanticModel") / "definition" / "relationships.tmdl"
    assert (project / name).read_text(encoding="utf-8") == (tmp_path / name).read_text(encoding="utf-8")


def visuals(project):
    import json
    out = {}
    for f in sorted((project / "FunnelStream.Report" / "definition" / "pages").glob("*/visuals/*/visual.json")):
        out.setdefault(f.parts[-4], []).append(json.loads(f.read_text(encoding="utf-8")))
    return out


def fields_of(visual):
    for state in visual["visual"].get("query", {}).get("queryState", {}).values():
        for proj in state["projections"]:
            (kind, body), = proj["field"].items()
            yield kind, body["Expression"]["SourceRef"]["Entity"], body["Property"], proj


def test_every_page_has_visuals_and_the_headline_numbers_of_the_guide(project):
    found = visuals(project)
    assert set(found) == {"executive", "campaign", "funnel", "inventory", "marketing_inventory", "products", "customers_delivery"}
    assert all(len(v) >= 8 for v in found.values()), {k: len(v) for k, v in found.items()}
    wanted = {"executive": "Revenue Delivered", "campaign": "ROAS Placed", "funnel": "Cart Abandon Rate",
              "inventory": "Out of Stock Lines", "marketing_inventory": "Exposed Spend", "products": "Product Revenue",
              "customers_delivery": "Average Delivery Hours"}
    for page, measure in wanted.items():
        used = {prop for v in found[page] for kind, _, prop, _ in fields_of(v) if kind == "Measure"}
        assert measure in used, (page, measure)


def test_every_visual_field_exists_in_the_model(project):
    tables = {name.lower(): {c.lower() for c in cols} for name, cols in model_columns(project).items()}
    missing = []
    for page, items in visuals(project).items():
        for v in items:
            for kind, entity, prop, proj in fields_of(v):
                if kind == "Measure":
                    assert entity == "_Measures", (page, prop)
                if prop.lower() not in tables.get(entity.lower(), set()):
                    missing.append("%s: %s[%s]" % (page, entity, prop))
                assert proj["queryRef"] == "%s.%s" % (entity, prop)
    assert not missing, missing


def test_visuals_fit_the_page_and_have_unique_names(project):
    names = set()
    for page, items in visuals(project).items():
        for v in items:
            pos = v["position"]
            assert pos["x"] >= 0 and pos["y"] >= 0 and pos["x"] + pos["width"] <= 1280 and pos["y"] + pos["height"] <= 720, (page, v["name"])
            assert v["name"] not in names
            names.add(v["name"])
        # no two visuals overlap
        boxes = [(v["position"]["x"], v["position"]["y"], v["position"]["x"] + v["position"]["width"], v["position"]["y"] + v["position"]["height"]) for v in items]
        for i, a in enumerate(boxes):
            for b in boxes[i + 1:]:
                assert a[2] <= b[0] or b[2] <= a[0] or a[3] <= b[1] or b[3] <= a[1], (page, a, b)


def test_every_visual_has_a_title_except_slicers(project):
    for page, items in visuals(project).items():
        for v in items:
            if v["visual"]["visualType"] not in ("slicer", "card", "textbox"):
                title = v["visual"]["visualContainerObjects"]["title"][0]["properties"]["text"]["expr"]["Literal"]["Value"]
                assert title.startswith("'") and len(title) > 3, (page, v["name"])


def test_the_model_uses_the_demand_order_line_and_customer_tables(project):
    """City x product demand, order lines and customers were in Gold but not in the model."""
    tables = model_columns(project)
    for name, needed in (("City Product Daily", {"city", "product_id", "units", "revenue", "sessions_viewed", "add_to_cart"}),
                         ("Order Lines", {"product_id", "city", "quantity", "line_total"}),
                         ("Customer", {"customer_id", "city", "primary_device", "signup_date"})):
        assert needed <= {c.lower() for c in tables[name]}, name
    for measure in ("Product Revenue", "Product Cart Rate", "Lines Revenue", "Active Customers", "Average Delivery Hours",
                    "First Attempt Delivery Rate"):
        assert measure in tables["_Measures"], measure


PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]


def test_every_page_has_a_header_band_with_its_title(project):
    for page, items in visuals(project).items():
        headers = [v for v in items if v["visual"]["visualType"] == "textbox"]
        assert len(headers) == 1, page
        pos = headers[0]["position"]
        assert (pos["x"], pos["y"], pos["width"]) == (0, 0, 1280), page
        text = json_dumps(headers[0])
        assert "FunnelStream" in text, page


def json_dumps(obj):
    import json
    return json.dumps(obj)


def test_the_theme_carries_the_validated_palette_and_is_registered(project):
    import json
    theme_file = project / "FunnelStream.Report" / "StaticResources" / "RegisteredResources" / "FunnelStreamTheme.json"
    theme = json.loads(theme_file.read_text(encoding="utf-8"))
    assert theme["dataColors"] == PALETTE, "scripts/validate_palette.js of the dataviz skill was run on exactly this order"
    assert theme["good"] == "#0ca30c" and theme["bad"] == "#d03b3b"
    report = json.loads((project / "FunnelStream.Report" / "definition" / "report.json").read_text(encoding="utf-8"))
    assert report["themeCollection"]["customTheme"]["name"] == "FunnelStreamTheme.json"
    package, = report["resourcePackages"]
    assert package["type"] == "RegisteredResources"
    assert {"name": "FunnelStreamTheme.json", "path": "FunnelStreamTheme.json", "type": "CustomTheme"} in package["items"]


def test_every_page_has_a_background_and_cards_are_styled(project):
    import json
    for page_json in (project / "FunnelStream.Report" / "definition" / "pages").glob("*/page.json"):
        page = json.loads(page_json.read_text(encoding="utf-8"))
        assert page["objects"]["background"][0]["properties"]["color"], page["name"]
        assert page["objects"]["outspace"][0]["properties"]["color"], page["name"]
    for page, items in visuals(project).items():
        for v in items:
            if v["visual"]["visualType"] in ("card", "tableEx", "lineChart", "clusteredBarChart", "clusteredColumnChart", "funnel", "treemap", "donutChart"):
                obj = v["visual"]["visualContainerObjects"]
                assert "background" in obj and "border" in obj, (page, v["name"])
            if v["visual"]["visualType"] == "card":
                assert "labels" in v["visual"]["objects"], (page, v["name"])


def test_bar_and_column_charts_show_data_labels_because_three_palette_colours_are_light(project):
    """The palette check warns that aqua, yellow and magenta are under 3:1 on white; visible labels are the relief."""
    for page, items in visuals(project).items():
        for v in items:
            if v["visual"]["visualType"] in ("clusteredBarChart", "clusteredColumnChart"):
                assert "labels" in v["visual"]["objects"], (page, v["name"])
