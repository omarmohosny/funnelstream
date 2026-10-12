# Power BI redesign, Stage 1 (must-fix) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the visible defects of the Power BI report (cut-off slicers, (Blank) cards, "2M" rounding, truncated labels, meaningless measures) without changing the page set, so a presentable report exists before Stage 2.

**Architecture:** All changes are in the generator `scripts/make_pbip.py` and the measure file `dashboards/powerbi/measures.dax`; the report and semantic model folders are regenerated from them. The tests regenerate into a temporary folder (`make_pbip.OUT`), so they never overwrite the committed project.

**Tech Stack:** Python 3.11, pytest, DAX, TMDL, PBIR JSON.

**Spec:** `docs/superpowers/specs/2026-10-12-powerbi-redesign-design.md`

## Global Constraints

- Canvas 1280 x 720, margin 16 px, gap 8 px, header 64 px, slicers at least 64 px tall, at most 3 slicers per page.
- A card label is at most 24 characters (the measure name is the label).
- A card must never show (Blank) for a count: counts return 0 when nothing matches.
- Card numbers are shown with display units "None" (value `1D`), never "2M".
- Do not run `python scripts/make_pbip.py` over `dashboards/powerbi/` before Task 4; tests use a temporary folder.
- Do not commit or push without the owner's word. Git author per command: `git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit ...`, message ending with the `Co-Authored-By` line.
- Host tests must stay at 0 failed (`.venv\Scripts\python -m pytest tests -q -p no:cacheprovider`, last run 1134 passed, 218 skipped).

## Review Focus

- A slicer that is tall enough but pushed off the canvas: the bottom of every visual stays at or below 704 (`test_slicers_are_tall_enough_and_every_page_fits`).
- A renamed measure still referenced under its old name in the generator, the guide or a test: the generator raises or a test fails (`test_every_visual_field_exists_in_the_model`, existing).
- A measure that returns BLANK for an empty slice: the three stock measures and the quiet-buyers measure are pinned by text tests (`tests/test_powerbi_measures_stage1.py`).
- The prior-7-days measure dividing by zero: it uses `DIVIDE` and is BLANK, not an error, when the earlier window is empty.
- A card label longer than 24 characters after a future rename (`test_card_labels_fit_and_numbers_are_not_rounded`).

Deferred to Stage 2 (a change to the spec's Stage 1 list, reported to the owner): the Top-N filter on tables and the blank-carrier filter need PBIR filter JSON that cannot be checked without Power BI Desktop. The carrier problem is handled in Task 3 inside the measure instead, and the Top-N tables are replaced by ranked bars in Stage 2.

---

### Task 1: Slicer height and a layout that fits

**Files:**
- Modify: `scripts/make_pbip.py:284-288` (constants) and `scripts/make_pbip.py:368-480` (every `240` row height)
- Test: `tests/test_make_pbip.py` (append)

**Interfaces:**
- Produces: constants `SLICER_H = 64`, `CARD_Y = 144`, `BODY_Y = 236`, `ROW_H = 230` used by `page_layouts()`.

- [ ] **Step 1: Write the failing test** (append to `tests/test_make_pbip.py`)

```python
def test_slicers_are_tall_enough_and_every_page_fits(project):
    for page, items in visuals(project).items():
        slicers = [v for v in items if v["visual"]["visualType"] == "slicer"]
        assert len(slicers) <= 3, page
        for s in slicers:
            assert s["position"]["height"] >= 64, (page, s["name"])
        bottom = max(v["position"]["y"] + v["position"]["height"] for v in items)
        assert bottom <= 704, (page, bottom)
```

- [ ] **Step 2: Run it to see it fail**

Run: `.venv\Scripts\python -m pytest tests/test_make_pbip.py::test_slicers_are_tall_enough_and_every_page_fits -q -p no:cacheprovider`
Expected: FAIL (`height >= 64`, slicers are 44 high; and `marketing_inventory` or the executive page has more than 3 slicers only if they do, the first assert shows which).

- [ ] **Step 3: Change the constants** in `scripts/make_pbip.py`

```python
SLICER_Y, SLICER_H = 72, 64
CARD_Y, CARD_H = 144, 84
BODY_Y = 236
BODY_H = PAGE_H - BODY_Y - MARGIN
ROW_H = (BODY_H - GAP) // 2          # two body rows of 230 px with an 8 px gap
BODY_BOTTOM = PAGE_H - MARGIN
```

- [ ] **Step 4: Use the grid in `page_layouts()`.** Replace the first line of the row maths and every hard-coded row height:

```python
    top, mid = BODY_Y, BODY_Y + ROW_H + GAP   # two rows of the body, ROW_H high each
```

then run (every `240` row height in the file is inside `page_layouts()`, so no line range is used):

```bash
sed -i 's/, 240)/, ROW_H)/g; s/mid + 92, 740, 148/mid + 92, 740, ROW_H - 92/' scripts/make_pbip.py
```

Check: `grep -n "240" scripts/make_pbip.py` prints nothing.

- [ ] **Step 5: Run the test and the existing generator tests**

Run: `.venv\Scripts\python -m pytest tests/test_make_pbip.py -q -p no:cacheprovider`
Expected: all pass, including `test_visuals_fit_the_page_and_have_unique_names` (no overlap).

- [ ] **Step 6: Commit** (only when the owner says so)

```bash
git add scripts/make_pbip.py tests/test_make_pbip.py
git commit -m "Make the report slicers 64 px tall and size the body rows from the grid"
```

---

### Task 2: Card numbers and labels

**Files:**
- Modify: `scripts/make_pbip.py` (`visual_objects`, card branch; the `PCT`, `RATIO2`, `ONE_DECIMAL`, `TEXT` sets; card lists in `page_layouts`)
- Modify: `dashboards/powerbi/measures.dax`, `dashboards/powerbi/MODEL.md` (measure renames)
- Test: `tests/test_make_pbip.py` (append)

**Interfaces:**
- Consumes: Task 1 layout.
- Produces: measure names `Delivered Share`, `Revenue vs Prior 7 Days`, `Slowest Express (min)`, `First-Attempt Rate`, `Quiet Buyers (7+ days)` (the last two defined in Task 3), used by the card lists.

- [ ] **Step 1: Write the failing test** (append to `tests/test_make_pbip.py`)

```python
def test_card_labels_fit_and_numbers_are_not_rounded(project):
    for page, items in visuals(project).items():
        for v in items:
            if v["visual"]["visualType"] != "card":
                continue
            [(kind, _, prop, _)] = fields_of(v)
            assert len(prop) <= 24, (page, prop)
            unit = v["visual"]["objects"]["labels"][0]["properties"]["labelDisplayUnits"]["expr"]["Literal"]["Value"]
            assert unit == "1D", (page, prop)  # display units None: 2,410,979 and not 2M
```

- [ ] **Step 2: Run it to see it fail**

Run: `.venv\Scripts\python -m pytest tests/test_make_pbip.py::test_card_labels_fit_and_numbers_are_not_rounded -q -p no:cacheprovider`
Expected: FAIL (`KeyError: 'labelDisplayUnits'`).

- [ ] **Step 3: Switch off display-unit rounding.** In `visual_objects`, replace the card branch:

```python
    if kind == "card":
        return {"labels": [{"properties": {"color": solid(NAVY), "fontSize": number(24), "fontFamily": text(FONT_BOLD),
                                           "labelDisplayUnits": number(1)}}],
                "categoryLabels": [{"properties": {"show": flag(True), "color": solid(INK2), "fontSize": number(10),
                                                   "fontFamily": text(FONT)}}]}
```

- [ ] **Step 4: Rename the five long measure names everywhere** (found by listing every card label over 24 characters; `Stock Freshness (seconds)` is removed in Task 3).

```bash
for f in dashboards/powerbi/measures.dax dashboards/powerbi/MODEL.md scripts/make_pbip.py; do
  sed -i 's/Delivered Share of Revenue/Delivered Share/g; s/Revenue Placed vs Last Week/Revenue vs Prior 7 Days/g; s/Slowest Express Delivery (min)/Slowest Express (min)/g; s/First Attempt Delivery Rate/First-Attempt Rate/g' "$f"
done
grep -rn "Delivered Share of Revenue\|vs Last Week\|Slowest Express Delivery\|First Attempt Delivery Rate" dashboards scripts tests docs | grep -v "_archive\|FunnelStream.Report\|FunnelStream.SemanticModel"
```

Expected: the grep prints nothing. (`FunnelStream.Report` and `FunnelStream.SemanticModel` are regenerated in Task 4.)

- [ ] **Step 5: Run the tests**

Run: `.venv\Scripts\python -m pytest tests/test_make_pbip.py -q -p no:cacheprovider`
Expected: all pass. A failure naming `Cost per Delivered Order` or another label over 24 characters means that label must be renamed the same way in Step 4.

- [ ] **Step 6: Commit** (only when the owner says so)

```bash
git add scripts/make_pbip.py dashboards/powerbi/measures.dax dashboards/powerbi/MODEL.md tests/test_make_pbip.py
git commit -m "Show card numbers without display-unit rounding and shorten long card labels"
```

---

### Task 3: Measures that are never blank and never meaningless

**Files:**
- Modify: `dashboards/powerbi/measures.dax` (measures named below)
- Modify: `scripts/make_pbip.py` (card lists: remove `Stock Freshness (seconds)`, replace `Lapsed Buyers (30+ days)`; format sets)
- Modify: `dashboards/powerbi/MODEL.md` (lines naming the removed and renamed measures)
- Test: `tests/test_powerbi_measures_stage1.py` (create)

**Interfaces:**
- Produces: measures `Out of Stock Lines`, `Low Stock Lines`, `Out of Stock Rate` (return 0 when empty), `Quiet Buyers (7+ days)`, `Revenue vs Prior 7 Days`, `Average Delivery Hours` (BLANK without delivered orders), `Days of Cover` (capped at 90), `Maturity Label` (short text). `Stock Freshness (seconds)` and `Lapsed Buyers (30+ days)` no longer exist.

- [ ] **Step 1: Write the failing test** (create `tests/test_powerbi_measures_stage1.py`)

```python
"""Stage 1 of the Power BI redesign: measures that show (Blank) or a meaningless number in the report."""
import re
from pathlib import Path

DAX = (Path(__file__).resolve().parent.parent / "dashboards" / "powerbi" / "measures.dax").read_text(encoding="utf-8")


def body(name):
    m = re.search(r"^%s =\n?(.*?)(?=\n\n)" % re.escape(name), DAX, re.S | re.M)
    assert m, name
    return m.group(1)


def test_stock_counts_are_zero_not_blank_when_nothing_matches():
    for name in ("Out of Stock Lines", "Low Stock Lines"):
        assert "COALESCE" in body(name) and re.search(r",\s*0\s*\)", body(name)), name
    assert "COALESCE" in body("Out of Stock Rate")


def test_the_freshness_and_lapsed_measures_are_gone_and_quiet_buyers_replaces_lapsed():
    assert "Stock Freshness" not in DAX and "Lapsed Buyers" not in DAX
    assert ">= 7" in body("Quiet Buyers (7+ days)") and "COALESCE" in body("Quiet Buyers (7+ days)")


def test_prior_week_change_compares_two_explicit_seven_day_windows():
    text = body("Revenue vs Prior 7 Days")
    assert text.count("DATESBETWEEN") == 2 and "DIVIDE" in text and "DATEADD" not in text


def test_delivery_hours_are_blank_without_delivered_orders_and_days_of_cover_are_capped():
    assert "is_delivered" in body("Average Delivery Hours") and "BLANK" in body("Average Delivery Hours")
    assert "MIN (" in body("Days of Cover") and "90" in body("Days of Cover")


def test_days_of_cover_prints_the_cap_as_90_plus():
    from scripts import make_pbip
    tmdl = make_pbip.measure_tmdl(next(m for m in make_pbip.parse_measures() if m["name"] == "Days of Cover"))
    # no quote characters inside the format, so the TMDL string needs no escaping: \9\+ prints the literal text 90+
    assert "formatString: \"[>=90]\\9\\0\\+;#,0.0\"" in tmdl
```

- [ ] **Step 2: Run it to see it fail**

Run: `.venv\Scripts\python -m pytest tests/test_powerbi_measures_stage1.py -q -p no:cacheprovider`
Expected: 5 failed.

- [ ] **Step 3: Edit `dashboards/powerbi/measures.dax`.** Replace each measure with exactly this text (keep the display-folder comments above them):

```dax
Out of Stock Lines =
COALESCE ( CALCULATE ( COUNTROWS ( 'Stock Now' ), KEEPFILTERS ( 'Stock Now'[available] <= 0 ) ), 0 )

Low Stock Lines =
// Above zero but below the reorder point. A column-to-column comparison needs FILTER, not a CALCULATE boolean.
COALESCE (
    COUNTROWS (
        FILTER (
            'Stock Now',
            'Stock Now'[available] > 0 && 'Stock Now'[available] < 'Stock Now'[reorder_point]
        )
    ),
    0
)

Out of Stock Rate = COALESCE ( DIVIDE ( [Out of Stock Lines], [Stock Lines] ), 0 )

Days of Cover =
// Closing stock divided by the average units sold per day with data, capped at 90 (shown as 90+). A product that sold nothing
// never runs out in the data, so it is also 90.
VAR Days = CALCULATE ( DISTINCTCOUNT ( 'Inventory Daily'[report_date] ) )
VAR AvgDailySold = DIVIDE ( [Units Sold], Days )
RETURN
    IF (
        AvgDailySold > 0,
        MIN ( DIVIDE ( [Closing Stock], AvgDailySold ), 90 ),
        IF ( [Closing Stock] > 0, 90 )
    )

Maturity Label =
IF ( [Is Provisional], "Provisional", "Settled" )

Revenue vs Prior 7 Days =
// The latest 7 days with orders against the 7 days before them, whatever the date slicer says; blank when the earlier window is empty.
VAR LastDay = MAXX ( FILTER ( ALL ( 'Date' ), [Orders Placed] > 0 ), 'Date'[Date] )
VAR Latest =
    CALCULATE ( [Revenue Placed], REMOVEFILTERS ( 'Date' ), DATESBETWEEN ( 'Date'[Date], LastDay - 6, LastDay ) )
VAR Earlier =
    CALCULATE ( [Revenue Placed], REMOVEFILTERS ( 'Date' ), DATESBETWEEN ( 'Date'[Date], LastDay - 13, LastDay - 7 ) )
RETURN
    DIVIDE ( Latest - Earlier, Earlier )

Average Delivery Hours =
// Hours from order to delivery, over delivered orders only; blank (so no bar) when a group has no delivered order.
VAR Delivered = CALCULATE ( COUNTROWS ( Orders ), Orders[is_delivered] = TRUE () )
RETURN
    IF ( Delivered > 0, CALCULATE ( AVERAGE ( Orders[delivery_lead_hours] ), Orders[is_delivered] = TRUE () ), BLANK () )

Quiet Buyers (7+ days) =
// Buyers whose last order is 7 or more days old (the data spans about 15 days, so a 30-day rule would always be empty).
COALESCE ( CALCULATE ( DISTINCTCOUNT ( 'Customer 360'[customer_id] ), 'Customer 360'[days_since_last_order] >= 7 ), 0 )
```

Delete the whole `Stock Freshness (seconds) =` measure (and its two comment lines) and the whole `Lapsed Buyers (30+ days) =` measure.

- [ ] **Step 4: Update the generator.** In `scripts/make_pbip.py`:

```bash
sed -i 's/"Lapsed Buyers (30+ days)"/"Quiet Buyers (7+ days)"/' scripts/make_pbip.py
```

then in the inventory page, change the card list to

```python
        + card_row(["Out of Stock Lines", "Low Stock Lines", "Out of Stock Rate", "Stock Units Available"])
```

In the format sets, take `"Days of Cover"` out of `ONE_DECIMAL` and add a branch in `measure_tmdl` before the `ONE_DECIMAL` branch, so the cap prints as "90+" (backslash-escaped literal characters, which avoids nested quotes in TMDL):

```python
CAPPED = {"Days of Cover"}
```

```python
    elif n in CAPPED:
        out.append("\t\tformatString: \"[>=90]\\9\\0\\+;#,0.0\"")
```

`Maturity Label` stays in `TEXT`.

- [ ] **Step 5: Update the guide.** In `dashboards/powerbi/MODEL.md` remove the words `Stock Freshness`, `Lapsed Buyers (30+ days)` and replace them with `Quiet Buyers (7+ days)`; `grep -n "Freshness\|Lapsed" dashboards/powerbi/MODEL.md` must print nothing.

- [ ] **Step 6: Run the new and the old tests**

Run: `.venv\Scripts\python -m pytest tests/test_powerbi_measures_stage1.py tests/test_make_pbip.py -q -p no:cacheprovider`
Expected: all pass. `test_every_measure_points_at_existing_columns_and_measures` checks that `Orders[is_delivered]` and `'Customer 360'[days_since_last_order]` exist; if it names a missing column, use the column name it prints.

- [ ] **Step 7: Commit** (only when the owner says so)

```bash
git add dashboards/powerbi/measures.dax dashboards/powerbi/MODEL.md scripts/make_pbip.py tests/test_powerbi_measures_stage1.py
git commit -m "Make the stock and buyer measures never blank and replace the meaningless ones"
```

---

### Task 4: Regenerate, validate and check in Power BI Desktop

**Files:**
- Modify (generated): `dashboards/powerbi/FunnelStream.Report/`, `dashboards/powerbi/FunnelStream.SemanticModel/`

- [ ] **Step 1: Run the whole host suite**

Run: `.venv\Scripts\python -m pytest tests -q -p no:cacheprovider`
Expected: `0 failed`.

- [ ] **Step 2: Copy the current project, then regenerate** (the commit `7918108` is the fallback, and a plain folder copy keeps any Desktop edits made since)

Run: `cp -r dashboards/powerbi data/_archive/powerbi_before_stage1` then `.venv\Scripts\python scripts/make_pbip.py`

Expected: the report and model folders are rewritten; `git status` lists them as modified.

- [ ] **Step 3: Validate** (optional before the defense; start it in the background while the owner checks Desktop; it needs the internet and takes about 20 minutes; the known 404 on `visualContainer/2.13.0` is a download failure and not a violation)

Run: `.venv\Scripts\python scripts/validate_pbip.py`
Expected: `0 INVALID`.

- [ ] **Step 4: Owner check in Power BI Desktop.** The owner closes Desktop, reopens `dashboards\powerbi\FunnelStream.pbip`, refreshes, and sends screenshots of all pages. Pass criteria: no (Blank) on any card; no label ending in an ellipsis; every slicer shows its dropdown; revenue cards read like `2,410,979`; the Campaign page shows no overlap when no visual is selected; the page "Customers and delivery" has no (Blank) carrier bar.

- [ ] **Step 5: If Desktop reports a load error,** do not edit the report files by hand: fix the generator, rerun Steps 1 to 3, and reopen. `git checkout -- dashboards/powerbi/FunnelStream.Report dashboards/powerbi/FunnelStream.SemanticModel` restores the committed project.

- [ ] **Step 6: Commit** (only when the owner says so, after Step 4 passes)

```bash
git add dashboards/powerbi docs/superpowers
git commit -m "Regenerate the Power BI project for Stage 1 of the redesign"
```

## Self-review notes

- Spec coverage: slicer visibility (Task 1), blank cards, number formats, card labels, Stock Freshness, Days of Cover, week-over-week (Tasks 2 and 3), blank carrier (Task 3, in the measure), the Maturity label (Task 3). The spec's Stage 1 items "Top-N tables" and "the Campaign overlap" are changed: Top-N moves to Stage 2 (see the deferred note above); the overlap cannot occur in the generated layout (`test_visuals_fit_the_page_and_have_unique_names` forbids overlaps, and the screenshot was most likely the selected visual's toolbar), so Task 4 Step 4 verifies it in Desktop.
- The reconciliation of the Campaign numbers (909 against 126, CPA 93 against the table) and the order-definition footnotes are Stage 2, with the page rewrite.
- Names used across tasks: `ROW_H`, `Revenue vs Prior 7 Days`, `Quiet Buyers (7+ days)`, `Delivered Share`, `Slowest Express (min)` are identical in every task.

## Review notes (analyst review of this plan, 12 October 2026)

- Fixed in the plan after the review: the `Low Stock Lines` test now matches a multi-line COALESCE; `First Attempt Delivery Rate` (27 characters) is renamed; the card category label stays 10 pt; the row-height `sed` has no line range; `Days of Cover` prints "90+" through a format string; the project is copied to a folder before regeneration.
- Known and left for Stage 2 or for the owner, because Stage 1 does not touch them: slicer headers and axis titles that show raw field names; pie and donut labels; tables that scroll; the 909 against 126 campaign mismatch; the `ROAS Gap` card; seven cards on Executive and Campaign (labels of 24 characters or fewer fit in the 172 px cards at 10 pt).
- `Revenue vs Prior 7 Days` ignores the date slicer on purpose (title and subtitle say so in Stage 2) and, because 9 October (about 1.4M) falls in its latest window, it will show a large positive percentage on the current data. That is correct arithmetic on spiky data; the owner decides in Stage 2 whether to annotate the spike or start the default date filter on 10 October.
- A zero on the three stock cards means `Stock Now` has no empty line at refresh. The marts still show revenue at risk from earlier days; the owner should expect that contradiction to be asked about, and Stage 2 labels the cards "now" and the risk table "by day".
- `labelDisplayUnits` value `1D` for display units None is from memory (about 85 percent sure); the Desktop check in Task 4 is the proof, and if the card still shows "2M" the fix is a one-line change of that value in the generator.
