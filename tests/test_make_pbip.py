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
    assert {t.strip("'") for t in listed} == files and len(files) == 19
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
    for needed in ("Inventory", "Marketing and inventory", "Express delivery (the 20-minute promise)", "Products and cities", "Customers and delivery", "Customer 360"):
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


# Stage 2: seven pages, the marketing page third because it is the story page
PAGES = ("executive", "campaign", "marketing_inventory", "inventory", "funnel", "products", "customers")


def test_the_report_has_the_five_pages_of_the_guide(project):
    pages = (project / "FunnelStream.Report" / "definition" / "pages" / "pages.json").read_text(encoding="utf-8")
    for page in PAGES:
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
    assert not names & {"current", "earlier", "earliest"}  # DAX functions: `VAR Earlier` left a card empty in Desktop


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


def is_rail(visual):
    pos = visual["position"]
    return visual["visual"]["visualType"] == "textbox" and (pos["x"], pos["y"], pos["width"], pos["height"]) == (0, 0, 200, 720)


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
    assert set(found) == set(PAGES)
    assert all(len(v) >= 8 for v in found.values()), {k: len(v) for k, v in found.items()}
    wanted = {"executive": "Revenue Delivered", "campaign": "ROAS Delivered", "funnel": "Cart Abandon Rate",
              "inventory": "Express On-time Rate", "marketing_inventory": "Exposed Spend", "products": "Product Revenue",
              "customers": "Repeat Buyer Rate"}
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
        # no two visuals overlap, except that the rail is the navy background behind the navigator and the slicers
        boxes = [(v["position"]["x"], v["position"]["y"], v["position"]["x"] + v["position"]["width"], v["position"]["y"] + v["position"]["height"])
                 for v in items if not is_rail(v)]
        for i, a in enumerate(boxes):
            for b in boxes[i + 1:]:
                assert a[2] <= b[0] or b[2] <= a[0] or a[3] <= b[1] or b[3] <= a[1], (page, a, b)


def test_every_visual_has_a_title_except_slicers(project):
    for page, items in visuals(project).items():
        for v in items:
            if v["visual"]["visualType"] not in ("slicer", "card", "textbox", "pageNavigator"):
                title = v["visual"]["visualContainerObjects"]["title"][0]["properties"]["text"]["expr"]["Literal"]["Value"]
                assert title.startswith("'") and len(title) > 3, (page, v["name"])


def test_the_model_uses_the_demand_order_line_and_customer_tables(project):
    """City x product demand, order lines and customers were in Gold but not in the model."""
    tables = model_columns(project)
    for name, needed in (("City Product Daily", {"city", "product_id", "units", "revenue", "sessions_viewed", "add_to_cart"}),
                         ("Order Lines", {"product_id", "city", "quantity", "line_total"}),
                         ("Customer", {"customer_id", "city", "primary_device", "signup_date"}),
                         ("Customer 360", {"customer_id", "orders_count", "total_spend", "top_category", "is_repeat_buyer",
                                           "days_since_last_order", "preferred_payment_method"})):
        assert needed <= {c.lower() for c in tables[name]}, name
    for measure in ("Product Revenue", "Product Cart Rate", "Lines Revenue", "Active Customers", "Average Delivery Hours",
                    "First-Attempt Rate"):
        assert measure in tables["_Measures"], measure


PALETTE_HEAD = ["#0F3D6E", "#1F84FF", "#19423F", "#59A472"]   # the first data colours of the Accessible Tidal theme


def test_every_page_has_a_rail_a_page_navigator_and_a_title_banner(project):
    for page, items in visuals(project).items():
        textboxes = [v for v in items if v["visual"]["visualType"] == "textbox"]
        assert len(textboxes) == 2, page
        rails = [v for v in textboxes if is_rail(v)]
        banners = [v for v in textboxes if not is_rail(v)]
        assert len(rails) == 1 and len(banners) == 1, page
        pos = banners[0]["position"]
        assert (pos["x"], pos["y"], pos["width"], pos["height"]) == (216, 16, 1048, 64), page
        assert "FunnelStream" in json_dumps(banners[0]), page
        navigators = [v for v in items if v["visual"]["visualType"] == "pageNavigator"]
        assert len(navigators) == 1, page
        # the slicers sit inside the rail
        for v in items:
            if v["visual"]["visualType"] == "slicer":
                assert v["position"]["x"] + v["position"]["width"] <= 200, page


def json_dumps(obj):
    import json
    return json.dumps(obj)


def test_the_theme_is_accessible_tidal_and_is_registered(project):
    import json
    theme_file = project / "FunnelStream.Report" / "StaticResources" / "RegisteredResources" / "FunnelStreamTheme.json"
    theme = json.loads(theme_file.read_text(encoding="utf-8"))
    assert theme["name"] == "Accessible Tidal"
    assert theme["dataColors"][:4] == PALETTE_HEAD and len(theme["dataColors"]) == 32
    assert theme["good"] == "#46835A" and theme["bad"] == "#0F3D6E"
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


def test_bar_and_column_charts_show_data_labels_because_some_palette_colours_are_light(project):
    """Several colours of the palette (aqua, yellow, pale blue) are under 3:1 on white; visible labels are the relief."""
    for page, items in visuals(project).items():
        for v in items:
            if v["visual"]["visualType"] in ("clusteredBarChart", "clusteredColumnChart"):
                assert "labels" in v["visual"]["objects"], (page, v["name"])


def _flat(text):
    return re.sub(r"\s+", "", re.sub(r"//[^\n]*", "", text))


@pytest.mark.parametrize("name", ["Cart Abandon Rate", "Checkout Abandon Rate"])
def test_abandon_rates_are_blank_for_an_empty_slice_and_clamped(name):
    """I7: `1 - DIVIDE(...)` showed 100 percent for an empty slice and could go below zero. Gold returns NULL for
    an empty slice and clamps to 0..1; measures.dax and the model saved by Desktop must do the same."""
    dax = {m["name"]: "\n".join(m["body"]) for m in make_pbip.parse_measures()}[name]
    saved = (ROOT / "dashboards" / "powerbi" / "FunnelStream.SemanticModel" / "definition" / "tables"
             / "_Measures.tmdl").read_text(encoding="utf-8")
    block = re.search(r"^\tmeasure '%s' =(.*?)(?=^\t\w|\Z)" % name, saved, re.M | re.S).group(1)
    block = block.split("formatString")[0]
    for body in (dax, block):
        flat = _flat(body)
        assert "1-DIVIDE(" not in flat
        assert "ISBLANK" in flat and "MAX(0,MIN(1,1-" in flat
    assert _flat(dax) == _flat(block)


def test_slicers_are_tall_enough_and_every_page_fits(project):
    for page, items in visuals(project).items():
        slicers = [v for v in items if v["visual"]["visualType"] == "slicer"]
        assert len(slicers) <= 3, page
        for s in slicers:
            assert s["position"]["height"] >= 64, (page, s["name"])
        bottom = max(v["position"]["y"] + v["position"]["height"] for v in items if not is_rail(v))
        assert bottom <= 704, (page, bottom)


def test_card_labels_fit_and_numbers_are_not_rounded(project):
    for page, items in visuals(project).items():
        for v in items:
            if v["visual"]["visualType"] != "card":
                continue
            [(kind, _, prop, _)] = fields_of(v)
            assert len(prop) <= 24, (page, prop)
            unit = v["visual"]["objects"]["labels"][0]["properties"]["labelDisplayUnits"]["expr"]["Literal"]["Value"]
            assert unit == "1D", (page, prop)  # display units None: 2,410,979 and not 2M


def test_the_pages_are_in_the_stage_2_order(project):
    import json
    meta = json.loads((project / "FunnelStream.Report" / "definition" / "pages" / "pages.json").read_text(encoding="utf-8"))
    assert tuple(meta["pageOrder"]) == PAGES


def test_no_pie_donut_or_treemap_is_left(project):
    for page, items in visuals(project).items():
        kinds = {v["visual"]["visualType"] for v in items}
        assert not kinds & {"donutChart", "pieChart", "treemap"}, (page, kinds)


def test_at_most_six_cards_and_three_slicers_per_page(project):
    for page, items in visuals(project).items():
        kinds = [v["visual"]["visualType"] for v in items]
        assert kinds.count("card") <= 6 and kinds.count("slicer") <= 3, (page, kinds.count("card"), kinds.count("slicer"))


def test_fields_show_readable_names_and_never_snake_case(project):
    for page, items in visuals(project).items():
        for v in items:
            for kind, entity, prop, proj in fields_of(v):
                if kind == "Column":
                    name = proj.get("displayName")
                    assert name and "_" not in name and name[0].isupper(), (page, entity, prop, name)


def test_axis_titles_are_off_in_the_theme_because_chart_titles_say_what_is_shown(project):
    import json
    theme = json.loads((project / "FunnelStream.Report" / "StaticResources" / "RegisteredResources" / "FunnelStreamTheme.json")
                       .read_text(encoding="utf-8"))
    star = theme["visualStyles"]["*"]["*"]
    assert star["categoryAxis"][0]["showAxisTitle"] is False and star["valueAxis"][0]["showAxisTitle"] is False


def test_executive_has_no_prior_week_card_and_the_marketing_page_has_no_days_of_cover(project):
    found = visuals(project)
    used = lambda page: {prop for v in found[page] for kind, _, prop, _ in fields_of(v) if kind == "Measure"}
    assert "Revenue vs Prior 7 Days" not in used("executive")
    assert "Days of Cover (max 90)" not in used("marketing_inventory")
    assert "Average Delivery Hours" not in used("customers")  # Orders has no store or product relationship; it lives on Operations


def test_chart_titles_fit_their_box_and_the_carrier_chart_says_all_orders(project):
    import json
    titles = {}
    for page, items in visuals(project).items():
        for v in items:
            if v["visual"]["visualType"] in ("slicer", "card", "textbox", "pageNavigator"):
                continue
            title = v["visual"]["visualContainerObjects"]["title"][0]["properties"]["text"]["expr"]["Literal"]["Value"].strip("'")
            width = v["position"]["width"]
            titles[title] = page
            # about 7.5 px per character at 12 pt Segoe UI Semibold, minus the card padding
            assert len(title) * 7.5 <= width - 24, (page, title, len(title), width)
    carrier = [t for t in titles if "carrier" in t.lower()]
    assert carrier and all("standard delivery" not in t and "all orders" in t for t in carrier), carrier
    assert any("(D-1" in t for t in titles), "the exposed-spend chart must say its spend is the marketing-mart D-1 spend"


def test_the_weakest_charts_of_the_analyst_review_are_replaced(project):
    found = visuals(project)
    used = lambda page: {prop for v in found[page] for kind, _, prop, _ in fields_of(v) if kind == "Measure"}
    assert "Cost per Delivered Order" in used("campaign") and "Orders Placed" in used("executive")
    # Desktop showed Ended Out of Stock empty everywhere (no product ended a day at zero stock): Operations uses oversold units
    assert "Oversold Units" in used("inventory") and "Ended Out of Stock" not in used("inventory")
    assert "Net Stock Change" not in used("inventory")
    assert "Product Add to Carts" in used("products")
    assert "Lifetime Spend" in used("customers")
    assert "Product Sessions" not in {p for page in found for p in used(page)}  # renamed: it counts session x product, not sessions


def test_charts_do_not_show_a_blank_category_or_two_funnel_stages_as_2k(project):
    import json
    found = visuals(project)
    titles = {v["name"]: v["visual"]["visualContainerObjects"].get("title") for page in found for v in found[page]}
    cust = [v for v in found["customers"] if v["visual"]["visualType"] == "clusteredColumnChart"]
    # Lifetime Spend by favourite category listed (Blank) with 0 for the 3,705 customers who never ordered; a per-buyer measure is blank for them
    spend_by_category = [v for v in cust if {p for k, _, p, _ in fields_of(v)} == {"top_category", "Average Spend per Buyer"}]
    assert spend_by_category, "customers page needs the average spend per buyer by favourite category chart"
    funnel = [v for v in found["funnel"] if v["visual"]["visualType"] == "funnel"][0]
    unit = funnel["visual"]["objects"]["labels"][0]["properties"]["labelDisplayUnits"]["expr"]["Literal"]["Value"]
    assert unit == "1D"  # display units None: 1,7xx orders and 2,xxx checkout sessions instead of "2K" and "2K"
    ops_titles = [t for page in ("inventory",) for v in found[page] if v["visual"]["visualType"] == "tableEx"
                  for t in [v["visual"]["visualContainerObjects"]["title"][0]["properties"]["text"]["expr"]["Literal"]["Value"]]]
    assert any("oversold" in t.lower() for t in ops_titles), ops_titles


def test_table_columns_have_explicit_widths_that_fill_the_box(project):
    """A table with automatic widths hugged the left of its box; the generator sets one width per column."""
    found = [v for items in visuals(project).values() for v in items if v["visual"]["visualType"] == "tableEx"]
    assert found
    for v in found:
        widths = v["visual"]["objects"]["columnWidth"]
        refs = [proj["queryRef"] for _, _, _, proj in fields_of(v)]
        assert [w["selector"]["metadata"] for w in widths] == refs
        total = sum(float(w["properties"]["value"]["expr"]["Literal"]["Value"].rstrip("D")) for w in widths)
        assert abs(total - (v["position"]["width"] - 28)) <= len(refs), (v["name"], total)
