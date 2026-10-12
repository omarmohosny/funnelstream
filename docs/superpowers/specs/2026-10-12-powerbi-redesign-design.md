# Power BI report redesign: design

Date: 12 October 2026. Defense: 13 October 2026. Status: written for the owner's review; nothing in the report has been changed yet.

## 1. Intent and success criteria

The report in `dashboards/powerbi/` is shown to a defense panel one page at a time and is meant for two users: Operations (stock, express delivery) and Marketing (campaign return, ad spend sent to empty shelves). On 12 October the owner opened it in Power BI Desktop and was not satisfied: cards show (Blank), labels and slicers are cut off, large numbers are rounded to "2M", and several visuals do not help a decision.

Success means that every page can be presented without an apology and answers one question in about ten seconds:

1. No card shows (Blank), 0.0% from a broken measure, or a text cut off with an ellipsis.
2. Every slicer is fully visible and usable, with at most three per page.
3. Every number a panel can compare across pages is either equal or labelled with its definition.
4. Each page has one headline that states its takeaway, driven by a measure where the number is live.
5. The report passes the repository checks (`tests/test_make_pbip.py`, `scripts/validate_pbip.py`) and the owner confirms it in Power BI Desktop.

Not in scope: new data sources, new Gold tables, the Grafana dashboards, and changing the simulator.

## 2. Evidence this design rests on

- The 8 screenshots of the pages as rendered in Power BI Desktop (owner, 12 October).
- A business intelligence analyst review of those screenshots and of `scripts/make_pbip.py` (a reviewer only; it did not open Power BI Desktop and did not verify any cause).
- The generator `scripts/make_pbip.py` (page geometry: 1280 x 720 canvas, 16 px margin, 8 px gap, header 64, slicers 44, cards 84).
- The Power BI report design canon (skill `pbi-report-design`): 3-30-300 rule, at most 3 slicers and 4 to 6 cards per page, equal spacing, theme-driven colour, Segoe UI, a target and a trend on a KPI.

Causes that are known from the code: the slicers are 44 px tall (`SLICER_Y, SLICER_H = 72, 44`), which fits only the header of a dropdown slicer; `Stock Freshness (seconds)` is `DATEDIFF(MAX(updated_at), NOW(), SECOND)`, the age of the data at refresh, and is meaningless on a demo snapshot; `Out of Stock Lines` is a `CALCULATE(COUNTROWS(...))`, which is BLANK when no row matches.

Causes that are guesses until Stage 1 tests them: why `Revenue Placed vs Last Week` is 0.0% (`DATEADD` over a sparse date range), why `Lapsed Buyers (30+ days)` is blank (the data spans about 15 days, so nobody is 30 days quiet), and the Campaign page overlap (the layout looks correct on paper, so the rendered height must be checked).

## 3. Approach

Rebuild the report on one design system, in two stages so that a presentable report exists after the first.

- Stage 1, the must-fix list. Same pages, same layout, with defects fixed. Ends in a report the owner can present.
- Stage 2, the redesign. New theme and grid, a new page order and headlines, and the chart replacements below. Ends in the final report.

Both stages edit `scripts/make_pbip.py`, `dashboards/powerbi/measures.dax` and the theme, then regenerate. The generator replaces the whole report folder, so before Stage 1 the current state is already committed (commit `7918108` on `funnelstream-build`), and Desktop-added lineage tags are expected to be rewritten when the owner reopens and saves the project.

## 4. Design identity

- Tone: corporate and restrained. One accent colour per chart, no decoration.
- Signature: the navy header band on every page, with a one-line takeaway as its subtitle; the same card silhouette on every page; a dashed target line wherever a target exists.
- Colour: navy `#0d366b` for the header and key numbers, blue `#2a78d6` as the primary series, orange `#eb6834` for the comparison series, grey `#898781` for axes. Green `#0ca30c`, amber `#fab219` and red `#d03b3b` only for status, always paired with a word or an arrow.
- Typography: Segoe UI and Segoe UI Semibold. Header 20 pt, card value 28 pt, card label 11 pt, chart title 12 pt semibold, axis and table 10 pt or larger.
- Units: the unit is part of the title ("Revenue placed (EGP)"). Counts use `#,0`, percentages `0.0%`, money `#,0` with display units off, so a total reads "2,410,979" or "EGP 2.41M" and never "2M".
- Grid: 1280 x 720, 16 px margin, 8 px gap, every position computed from the margin and gap. Cards are 5 or 6 per row at most, with labels of at most 24 characters.
- Slicers: at most 3 per page, 64 px tall, dropdown mode (the date slicer in Between mode), checked fully visible in Desktop.

## 5. Pages (seven, one question each)

| # | Page | Question and headline (live numbers come from measures) | Main visuals |
|---|---|---|---|
| 1 | Executive | Is revenue on track and where do orders leak? "Revenue EGP x, y percent of placed orders never become delivered revenue." | 5 cards (Revenue placed, Revenue delivered, Orders placed, AOV, Delivered share) with a change against the previous 7 days; revenue by day; leak by outcome (cancelled, returned, failed COD) as ranked bars; orders by channel; orders by payment method as ranked bars. |
| 2 | Campaigns | Which campaign deserves more budget? | Delivered ROAS by campaign with placed ROAS as a comparison marker; spend and CPA table with data bars; conversions against attributed orders, labelled with both definitions. |
| 3 | Marketing and stock | How much ad spend sent shoppers to empty shelves? | Exposed spend (EGP and share of spend), exposed spend by campaign next to the campaign's own spend, revenue at risk by product (Top 8), by store. |
| 4 | Operations | Which stores and products need action today, and is the 20-minute promise holding? | Out-of-stock, low-stock and in-stock rate cards; stock by store; restock watchlist (Top 10); express on-time rate against a 95 percent target, breaches by store; delivery hours by carrier, labelled as standard delivery. |
| 5 | Funnel | Where do shoppers drop out? | Funnel with the conversion between stages; abandon rate by channel and platform as one small-multiples chart. |
| 6 | Products and cities | What to stock and promote? | Revenue by category and by city as ranked bars; Top 8 products with data bars; cart rate by category. |
| 7 | Customers | Who buys again and who should we win back? | Buyers and repeat rate; favourite category and preferred payment as ranked bars; quiet buyers (7 or more days); Top 10 customers. |

The page "Customers and delivery" is removed: its delivery half moves to Operations, its customer half to Customers. "Customer 360" becomes Customers.

## 6. Measure and model changes

| Measure | Change |
|---|---|
| `Out of Stock Lines`, `Low Stock Lines`, `Out of Stock Rate` | Return 0 when nothing matches (add 0 or `COALESCE`). A card must never be BLANK. |
| `Lapsed Buyers (30+ days)` | Replace with `Quiet Buyers (7+ days)`, because the data covers about 15 days; keep the 30 day measure out of the report. |
| `Revenue Placed vs Last Week` | Replace with the change of the latest 7 days against the previous 7 days, computed from explicit date ranges, and test it on the real data. |
| `Stock Freshness (seconds)` | Remove from the report; put "Data as of <timestamp>" in the page footer. |
| `Days of Cover` | Show as "more than 90 days" above a cap, or replace with "days since last restock". |
| `Maturity Label` | Short text "Provisional" or "Settled", shown as a small badge and not as a card. |
| `ROAS Gap` | Remove the card; the gap is shown by the placed and delivered markers. |
| Orders definitions | Footnote or rename: orders placed (Orders Daily), funnel orders (sessions with a purchase), attributed orders (last touch). Reconcile the Campaign page: the attributed-orders total (909) against the "Orders on Spend Days" chart (126), and the CPA card against the table rows. |
| Blank carrier | Filter the blank carrier out of the carrier visual. |

## 7. Data decisions for the owner

1. The 9 October spike (about 55 percent of all revenue on one day). Default: keep the full date range and annotate the spike on the Executive trend ("one-day volume peak in the simulated data"). Alternative: start the default date filter on 10 October. This is the owner's call because it changes what the panel sees.
2. Inventory is sparse (about 539K units on hand against 93 to 837 sold per day) and all revenue at risk is in one store. Default: do not change the simulator; hide Days of Cover above the cap and label the Nasr City concentration as the hero scenario.
3. The report is a snapshot of the Gold tables at refresh time, not a live view. The "Data as of" footer says so.

## 8. Verification

- Host: `tests/test_make_pbip.py` is updated to the new pages, slicer heights, measure formats and measure list; `scripts/validate_pbip.py` is run (the known 404 schema downloads for Desktop-written visuals are expected).
- A layout check in the generator test: no two visuals overlap, equal gaps and margins, every card label is 24 characters or fewer, every slicer is at least 64 px tall.
- Owner check in Power BI Desktop after each stage, with screenshots: no (Blank), no truncated text, slicers fully visible, numbers consistent between pages. An agent cannot see the canvas, so this step is not replaceable.
- Every measure change is also checked against the numbers behind the visual (Databricks Gold or the Postgres marts) before a headline uses it.

## 9. Risks

- The generator overwrites the report folder. Mitigation: work is committed first; the regenerated report is opened and saved once in Desktop.
- Blank cards or the week-over-week result may have causes in the model that the guesses in section 2 miss. Mitigation: Stage 1 begins with a diagnosis of each, in Desktop or against the data, before any fix.
- Time: the defense is on 13 October. Stage 1 alone must leave a presentable report; Stage 2 is cut by page, in the order of section 5, if time runs out.

## 10. Stages

Stage 1: slicer height and visibility, the blank cards, number formats and card labels, `Stock Freshness`, `Days of Cover`, the week-over-week measure, the Campaign overlap, Top-N tables, the blank carrier, the number labels from section 6. No page is removed or reordered.

Stage 2: the theme and grid of section 4, the page set and order of section 5, the chart replacements (ranked bars instead of pies and donuts, a clearer closing-stock view, delivered-versus-placed ROAS, small-multiples abandon rate, bullet or target-line for express on-time), headlines, footers.
