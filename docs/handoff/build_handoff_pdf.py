"""Build docs/FunnelStream-Team-Handoff.pdf (Q&A handoff for the team).

    pip install reportlab          # not part of requirements.txt; only needed to rebuild the PDF
    python docs/handoff/build_handoff_pdf.py

Edit the QA / TASKS data below, rebuild, commit the PDF. State is "as of" the commit named in AS_OF.
"""
import os
import sys

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (BaseDocTemplate, Frame, KeepTogether, PageBreak, PageTemplate, Paragraph, Preformatted,
                                Spacer, Table, TableStyle)

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "FunnelStream-Team-Handoff.pdf")
AS_OF = "8 October 2026, branch funnelstream-build, commit f27bd14"
NAVY = colors.HexColor("#0B1F3A")
GREY = colors.HexColor("#F2F4F7")
LINE = colors.HexColor("#C9CFD8")

ss = getSampleStyleSheet()
BODY = ParagraphStyle("body", parent=ss["Normal"], fontName="Helvetica", fontSize=9.5, leading=13.2, alignment=TA_LEFT)
SMALL = ParagraphStyle("small", parent=BODY, fontSize=8.3, leading=11)
CELL = ParagraphStyle("cell", parent=BODY, fontSize=8.4, leading=10.8)
CELLB = ParagraphStyle("cellb", parent=CELL, fontName="Helvetica-Bold", textColor=colors.white)
H1 = ParagraphStyle("h1", parent=BODY, fontName="Helvetica-Bold", fontSize=16, leading=20, textColor=NAVY, spaceBefore=4, spaceAfter=6)
H2 = ParagraphStyle("h2", parent=BODY, fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=NAVY, spaceBefore=8, spaceAfter=3)
Q = ParagraphStyle("q", parent=BODY, fontName="Helvetica-Bold", textColor=NAVY, spaceBefore=6, spaceAfter=1.5)
A = ParagraphStyle("a", parent=BODY, leftIndent=8, spaceAfter=2)
BUL = ParagraphStyle("bul", parent=BODY, leftIndent=18, bulletIndent=8, spaceAfter=1)
CODE = ParagraphStyle("code", fontName="Courier", fontSize=7.4, leading=9.4, leftIndent=4, textColor=colors.HexColor("#111111"))
TITLE = ParagraphStyle("title", parent=BODY, fontName="Helvetica-Bold", fontSize=28, leading=33, textColor=NAVY)
SUB = ParagraphStyle("sub", parent=BODY, fontSize=12.5, leading=17, textColor=colors.HexColor("#334155"))


def p(text, style=A):
    return Paragraph(text, style)


def bullets(items):
    return [Paragraph(t, BUL, bulletText="•") for t in items]


MAX_CODE_COLS = 104


def code(text):
    for ln in text.strip(chr(10)).splitlines():
        if len(ln) > MAX_CODE_COLS:
            raise ValueError("code line too long (%d): %s" % (len(ln), ln[:60]))
    box = Table([[Preformatted(text.strip("\n"), CODE)]], colWidths=[170 * mm])
    box.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), GREY), ("BOX", (0, 0), (-1, -1), 0.4, LINE),
                             ("LEFTPADDING", (0, 0), (-1, -1), 5), ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                             ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4)]))
    return [Spacer(1, 2), box, Spacer(1, 3)]


def table(rows, widths, header=True):
    data = [[Paragraph(str(c), CELLB if (header and i == 0) else CELL) for c in r] for i, r in enumerate(rows)]
    t = Table(data, colWidths=[w * mm for w in widths], repeatRows=1 if header else 0)
    st = [("GRID", (0, 0), (-1, -1), 0.3, LINE), ("VALIGN", (0, 0), (-1, -1), "TOP"),
          ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
          ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]
    if header:
        st.append(("BACKGROUND", (0, 0), (-1, 0), NAVY))
    for i in range(2, len(data), 2):
        st.append(("BACKGROUND", (0, i), (-1, i), GREY))
    t.setStyle(TableStyle(st))
    return [t, Spacer(1, 4)]


def qa(question, *answer_flowables):
    """question text, then paragraphs (str) or flowable lists."""
    out = [Paragraph(question, Q)]
    for a in answer_flowables:
        if isinstance(a, str):
            out.append(p(a))
        elif isinstance(a, list):
            out.extend(a)
        else:
            out.append(a)
    return KeepTogether(out) if all(isinstance(a, str) for a in answer_flowables) else out


# ---------------------------------------------------------------- content ---
story = []


def S(text):
    story.append(Paragraph(text, H1))


def S2(text):
    story.append(Paragraph(text, H2))


def add(*items):
    for it in items:
        if isinstance(it, list):
            story.extend(it)
        else:
            story.append(it)


# cover -------------------------------------------------------------------
story += [Spacer(1, 55 * mm), Paragraph("FunnelStream", TITLE), Spacer(1, 4),
          Paragraph("Team handoff: questions and answers", ParagraphStyle("t2", parent=TITLE, fontSize=19, leading=24)),
          Spacer(1, 8),
          Paragraph("A production-style streaming and batch data platform for an Egyptian online store: "
                    "Kafka, Spark, Delta Lake, Databricks, Postgres, Grafana, Airflow and Power BI.", SUB),
          Spacer(1, 14),
          Paragraph("State of the project: <b>%s</b>." % AS_OF, BODY),
          Paragraph("Defense: <b>13 October 2026</b>. Team: 5 members.", BODY),
          Spacer(1, 10),
          Paragraph("How to use this document: Part 1 explains what exists and why. Part 2 shows how to run it. "
                    "Part 3 lists the remaining tasks with owners and acceptance checks. Part 4 covers decisions, "
                    "limits and troubleshooting. The repository is the source of truth: the spec "
                    "(<font name='Courier'>docs/superpowers/specs/</font>), the plan "
                    "(<font name='Courier'>docs/superpowers/plans/</font>), the run guide "
                    "(<font name='Courier'>docs/RUNNING.md</font>) and the metric definitions "
                    "(<font name='Courier'>docs/metrics.md</font>) go deeper than this summary.", BODY),
          PageBreak()]

# contents ----------------------------------------------------------------
S("Contents")
add(*bullets([
    "<b>Part 1. What it is and how it works</b>: the project, architecture, data generation, Bronze, Silver, Gold, attribution, live monitoring, the ad report.",
    "<b>Part 2. Running it</b>: setup, the live demo, loading Gold, tests, secrets, memory.",
    "<b>Part 3. Remaining tasks</b>: every open task with owner, files, steps and acceptance check; what to cut if time runs out.",
    "<b>Part 4. Decisions, limits and troubleshooting</b>.",
    "<b>Appendix</b>: repository map, status table, vocabulary.",
]))
story.append(Spacer(1, 6))
S2("Status at a glance")
add(table([
    ["Area", "State", "Evidence"],
    ["Generator, noise, mock ad API", "Done", "19 noise kinds with exact manifest counts; 39 official districts under 6 cities; tracking URL with UTM"],
    ["Kafka, Bronze, Silver", "Done", "3 topics; idempotent; Silver counts reconcile to the manifest to the row"],
    ["Databricks mirror and Gold build", "Done", "Real workspace: 0 mismatches vs the Python reference on all Gold tables built so far"],
    ["Attribution (T13)", "Done", "fact_attribution 702 rows for 351 orders, 0 mismatches on the real workspace"],
    ["Live monitoring (G1, G2)", "Done", "Postgres live tables, Grafana dashboard (14 panels), 3 alert rules fired in a live check"],
    ["Ad report DAG", "Done", "Airflow dags test: first run pulled, second run cached, identical numbers"],
    ["Tests", "Green", "Host 645 passed, 121 skipped. Full Docker suite 765 passed, 1 skipped (25 min)"],
    ["Open work", "8 tasks", "T12, T14, T15, T16, G3, T17, T18, DOC (+ stretch T19). See Part 3"],
], [48, 20, 102]))
story.append(PageBreak())

# PART 1 ------------------------------------------------------------------
S("Part 1. What it is and how it works")
S2("The project")
add(qa("What is FunnelStream and what problem does it solve?",
       "A simulated online shop in Egypt. Customers browse in an app or on the web, add to cart, check out (mostly cash on delivery, some prepaid), "
       "and orders are delivered, sometimes failing or being returned. The platform ingests that traffic as a stream, cleans it, builds business "
       "tables in Databricks and shows live pipeline health in Grafana. It is built the way a production platform would be, not as a classroom ETL: "
       "dirty data on purpose, late and duplicate events, idempotent writes, alerts, and a documented rule for every number."),
    qa("Which business questions does it answer?",
       *bullets(["Where do users drop in the funnel (by day, channel and platform: app or web)?",
                 "Which campaigns bring orders, at what cost per order and per delivered order (last-touch attribution)?",
                 "How many orders are delivered, cancelled, returned, or had at least one failed delivery attempt? (Failed cash-on-delivery is the main story.)",
                 "Which products do people view and buy per city, and what should we recommend next?",
                 "Is the pipeline healthy right now (events per minute, late events, quarantined rows, freshness)?"])),
    qa("Why is the data synthetic, and is it realistic?",
       "No access to a real shop. A Python generator with a fixed seed produces journeys and orders with realistic drop-off, payment mix, delivery failures and "
       "Egyptian cities and districts. It also injects defects on purpose and writes <font name='Courier'>noise_manifest.json</font> with the exact count of each, "
       "so the pipeline can be proven correct: quarantine plus duplicates must match the manifest. The limit is stated in the spec: synthetic data cannot be proven 100% realistic."))

S2("Architecture")
add(code("""
 simulator --> Kafka (3 topics) --> Bronze (raw Delta) --> Silver (clean Delta)
 run_simulator   app.events         bronze_app_events      silver_events, silver_orders,
                 orders             bronze_orders          silver_order_status,
                 delivery.events    bronze_delivery_events silver_order_items, silver_quarantine

 Silver --+--> publish_silver --> Databricks Silver mirror --> Gold build (SQL MERGE)
          |                                                    dims, facts, summaries
          |                                                           |
          |                                                           v
          |                                                    Power BI (business view)
          +--> Postgres live tables --> Grafana (live view and alerts)

 mock ad API --> ads_report (Airflow DAG, daily) --> silver_campaign_snapshots (Databricks)
"""),
    qa("Which technology does what, and why?",
       *table([
           ["Component", "Role", "Why this choice"],
           ["Kafka (1 broker)", "Event bus: app.events, orders, delivery.events", "Standard streaming backbone; keys keep one session or order in order"],
           ["Spark (in Docker)", "Bronze ingest, Silver clean, local reference builders", "Structured Streaming + Delta MERGE; Docker because Spark needs winutils on Windows"],
           ["Delta Lake (local)", "Bronze and Silver storage", "ACID, MERGE for idempotent writes, no cloud needed locally"],
           ["Databricks", "Silver mirror and all Gold tables", "SQL MERGE builds, shared by the team, Power BI connector"],
           ["Postgres", "Live and health tables for Grafana; DDL fallback for Gold", "Light, Grafana reads it directly"],
           ["Grafana", "Live dashboard (5 s refresh) and 3 alert rules", "Light on memory; Power BI is for business analysis, Grafana for live monitoring"],
           ["Airflow", "Daily ad report DAG", "Scheduling; off by default (2 GB), started with a compose profile"],
           ["Power BI", "Business dashboards (open task T18)", "Connects to the Databricks Gold tables"],
       ], [32, 62, 76])),
    qa("Why is Gold written by Databricks only?",
       "One writer per table (spec section 17, rule 1). Late and corrected data is fixed by recomputing the affected dates from Silver and MERGE-ing the result, "
       "never by adding increments. The local Spark job writes only Bronze, Silver and the Postgres live tables."))

S2("Data generation")
add(qa("What generates the data and what does one run produce?",
       "<font name='Courier'>scripts/run_simulator.py</font> (modules in <font name='Courier'>simulator/</font>). One run publishes three kinds of messages to Kafka: "
       "app events (browsing and purchase), order lifecycle events and delivery events. An order exists only for a session that reached <i>purchase</i>. "
       "Example run (seed 7, 20,000 events): 19,316 app events, 667 order events, 383 delivery events, 20,366 messages in total."),
    qa("What are the order and delivery rules?",
       *bullets(["Status rank (highest wins): order_created 1, payment_authorised 2, order_packed 3, order_shipped 4, out_for_delivery and delivery_failed 5, delivered 6, order_completed and order_returned 7 (mutually exclusive; a tie goes to the latest status time). order_cancelled has rank 2 and counts only before shipment.",
                 "<b>Cash on delivery skips payment_authorised</b>; payment is collected at delivery. Prepaid methods (card, fawry, vodafone_cash, instapay) emit it.",
                 "A delivery can fail and be retried. If the last attempt fails and none succeeded, the order ends as <b>order_returned</b>.",
                 "Current status is the highest rank by <b>event time</b>, never by arrival order. A cancel counts only if it happened before the shipment; a cancel after shipping is quarantined (CANCEL_AFTER_SHIPMENT)."])),
    qa("What defects are injected (noise)?",
       "19 kinds, each counted exactly in the manifest: duplicates, malformed JSON, missing fields, unknown event types, bad versions, negative prices, future timestamps, "
       "late and very late events, non-UTC timestamps, null and unknown product ids, dirty text (city, device), numbers sent as strings, out-of-order status, "
       "cancel after shipment, bot sessions, <b>dirty districts</b> and <b>dirty UTM values</b>. Defects the validator can see go to quarantine; the others are cleaned in Silver."),
    qa("What is an address and what is the tracking URL?",
       *bullets(["Address = country > city > <b>district</b>. Cities: Cairo, Giza, Alexandria, Mansoura, Tanta, Assiut, Other. 39 official districts (qisms and ahya) under the six named cities, plus the single value Other, for example Nasr City, Maadi and Heliopolis under Cairo; Dokki, Agouza and Imbaba under Giza. Source of truth: <font name='Courier'>contracts/geo.py</font>.",
                 "A missing, unknown or mismatched district is stored as <b>unknown</b> and the row is kept (never quarantined). <b>Other</b> is a real value for smaller cities. Mohandessin and Faisal are neighborhoods, not districts, so they are not used.",
                 "Campaign traffic is tracked by UTM parameters inside <font name='Courier'>page_url</font> on the landing and product-view events, for example <font name='Courier'>https://shop.example.eg/p/prod_0042?utm_source=meta&amp;utm_medium=paid_social&amp;utm_campaign=ramadan_offer_1</font>. Silver parses utm_source, utm_medium and utm_campaign from it."])),
    qa("What is the difference between --seed and --universe-seed?",
       "<b>--universe-seed</b> fixes the customers and the product catalog (always 7 in this project). <b>--seed</b> fixes the events: session, order and event ids. "
       "The same seed regenerates the same ids and Silver silently drops them as duplicates, so every new run needs a <b>new --seed</b> with the same --universe-seed. "
       "<font name='Courier'>watch_demo.ps1</font> keeps the next free seed in <font name='Courier'>data/state/next_seed.txt</font>."))

S2("Kafka, Bronze and Silver")
add(qa("What are the Kafka topics?",
       "<font name='Courier'>app.events</font> (6 partitions, key session_id), <font name='Courier'>orders</font> (3, key order_id) and "
       "<font name='Courier'>delivery.events</font> (3, key order_id). There are no dead-letter topics: bad messages go to the Silver quarantine table (one term, one place)."),
    qa("What does Bronze do?",
       "<font name='Courier'>streaming/jobs/bronze_ingest.py</font>: one streaming query per topic writes every raw message, untouched, to a Delta table with Kafka partition, offset, timestamp "
       "and ingest time. Nothing is dropped or parsed. Processing is at-least-once; a restart resumes from the checkpoint."),
    qa("What does Silver do?",
       *bullets(["Validates (schema, types, version, product exists), normalizes city, device, district, timestamps (UTC) and numbers, and parses UTM from page_url.",
                 "Deduplicates by event id with an idempotent Delta MERGE, partitioned by Cairo event date.",
                 "Flags late events (<font name='Courier'>is_late</font>, more than 10 minutes behind) and bot sessions (more than 100 events in 10 seconds).",
                 "Writes silver_events, silver_orders, silver_order_status, silver_order_items and silver_quarantine. Products and customers are seeded first (<font name='Courier'>scripts/seed_silver_reference.py</font>).",
                 "Never raises on bad input: a bad row becomes a quarantine row with an error code and detail, never the raw payload."])),
    qa("What are the quarantine error codes?",
       "BAD_JSON, MISSING_FIELD, UNKNOWN_EVENT_TYPE, BAD_VERSION, NEGATIVE_PRICE, FUTURE_TIMESTAMP, BAD_VALUE, UNKNOWN_PRODUCT, CANCEL_AFTER_SHIPMENT, NEGATIVE_SPEND, CLICKS_GT_IMPRESSIONS."),
    qa("How do we know Silver is correct?",
       "It reconciles to the manifest. Example (seed 7): 20,366 messages - 314 quarantined - 199 duplicates = 19,853 = 18,822 silver_events + 1,031 silver_order_status rows. "
       "The 314 equals the sum of the manifest counts for the defects the validator can see."),
    qa("How are late events handled?",
       "Late rows stay in Silver and in Gold (funnel, attribution, orders). They are excluded only from the live metrics. In history mode (old events replayed) every row is late by the rule, "
       "so the late-rate alert is meaningful for <font name='Courier'>--live</font> runs only."))

S2("Databricks Silver mirror and Gold")
add(qa("How does data get from local Silver to Databricks?",
       "<font name='Courier'>scripts/publish_silver.py</font> uploads an explicit allow-list of columns (no names, phones or raw payloads) as Parquet to a Databricks volume and MERGEs "
       "insert-only into the Silver mirror tables. It is idempotent. <font name='Courier'>--reset --yes</font> truncates the mirror tables first (needed after a fresh local run, because the mirror only adds rows)."),
    qa("Which Gold tables exist?",
       *table([
           ["Table", "Meaning", "Built by"],
           ["dim_customer, dim_product", "Versioned (SCD2) dimensions; key -1 = unknown member", "Gold build (done)"],
           ["dim_campaign, dim_date", "3 campaigns; calendar 2025-2027 with weekend, Ramadan, Eid, holidays", "Gold build (done)"],
           ["fact_orders, fact_order_lines", "One row per order / order line; current status, flags, district, platform, campaign_id", "Gold build (done)"],
           ["fact_attribution", "Two rows per order: last_touch and first_touch", "Gold build (done, T13)"],
           ["agg_orders_daily", "Orders and revenue by day, channel, payment method, platform", "Gold build (done)"],
           ["agg_funnel_daily", "Sessions and drop-off by day, country, channel, platform", "Gold build (done)"],
           ["agg_campaign_daily", "Spend, orders, CPA, cost per delivered order, rates per campaign and date", "<b>T12 (open)</b>"],
           ["agg_city_product_daily", "Views, carts, sales per city, product, day", "<b>T14 (open)</b>"],
           ["rec_product, customer_360", "Recommendations; customer current state", "T14 / T19 (cut first)"],
       ], [44, 86, 40])),
    qa("How does the Gold build run?",
       *code("""
docker compose run --rm spark python -m warehouse.gold_build --days 7      # last 7 days (default)
docker compose run --rm spark python -m warehouse.gold_build --full        # rebuild everything
docker compose run --rm spark python -m warehouse.gold_build --days 7 --dry-run    # prints the SQL only
"""),
       "Steps run in a fixed order: dimensions, dim_date, fact_orders, fact_order_lines, fact_attribution, agg_orders_daily, agg_funnel_daily, then the slots for the open tasks. "
       "Each step is a MERGE script in <font name='Courier'>warehouse/gold/NN_name.sql</font>. A failing step stops the run and names itself. The session time zone is pinned to UTC; dates use Africa/Cairo."),
    qa("What is the unknown member (key -1)?",
       "A fact whose customer or product has no dimension row yet (a brand-new customer or product, or an anonymous customer) stores key -1 instead of being quarantined. "
       "Facts keep the natural ids, so the next Gold build re-joins and repairs the key once the dimension row exists."),
    qa("What is the limit of the 7-day window?",
       "A late status row for an order older than the window is missed until a wider <font name='Courier'>--days</font> or a <font name='Courier'>--full</font> run. Run <font name='Courier'>--full</font> now and then (the funnel is not affected)."),
    qa("How is attribution decided?",
       *bullets(["<b>Click</b> = the first page_view or product_view in a session that carries utm_campaign; bot sessions and unresolved campaigns do not count.",
                 "<b>last_touch</b>: the order's own session click (click_id on the order) first; else the customer's latest click in the 7 days before the order (identified customers only); else <b>direct</b> (campaign NULL).",
                 "<b>first_touch</b>: own session click; else the customer's earliest click in the same 7-day window; else direct.",
                 "agg_campaign_daily uses last_touch and says so in an attribution_model column. direct_share is computed from agg_orders_daily (channel direct), not per campaign.",
                 "Linking an anonymous pre-login session to a customer after login is out of scope (documented limitation)."])),
    qa("What are the business definitions?",
       *table([
           ["Metric", "Definition"],
           ["Placed", "Every attributed order, whatever its final status"],
           ["failed_attempt_rate", "Orders with at least one delivery_failed / orders placed"],
           ["return_rate", "Orders whose final status is order_returned / orders placed"],
           ["open_rate", "Orders not delivered, cancelled or returned after 14 days / orders placed"],
           ["CPA / cost per delivered order", "spend / orders placed; spend / orders delivered (both NULL when the denominator is 0)"],
           ["Revenue dating", "Delivered revenue is counted on the ORDER date (cohort view), so recent days are incomplete"],
           ["CTR", "Clicks reported by the ad API / impressions (not the same clicks as attribution)"],
       ], [50, 120])))

S2("Live monitoring and the ad report")
add(qa("What does Grafana show and where does the data come from?",
       "The Silver job, after each batch, recomputes full per-minute totals and upserts them (<font name='Courier'>INSERT ... ON CONFLICT DO UPDATE</font>, never +1) into four Postgres tables: "
       "<font name='Courier'>live_metrics_minute</font> (events, active sessions, product views, add to cart, orders per minute and country), "
       "<font name='Courier'>pipeline_health_minute</font> (events, late events, quarantined rows, Silver batches), <font name='Courier'>pipeline_batch_log</font> and "
       "<font name='Courier'>pipeline_heartbeat</font> (silver_job, gold_build, publish: when each last reported). Grafana (http://localhost:3000, 5 s refresh, anonymous view-only) reads them with a read-only role. "
       "Panels: events per minute, active sessions, orders, late-event rate, quarantine share and rows per hour, seconds since the last Silver report, Gold update and publish."),
    qa("Which alerts exist?",
       *table([
           ["Rule", "Fires when", "Note"],
           ["Late-event rate", "Above 5% over 5 minutes (at least 20 events)", "Meaningful for --live runs only"],
           ["Quarantine share", "Above 2% over 5 minutes (at least 20 rows)", "quarantined / (events + quarantined)"],
           ["No events", "No events for 2 minutes (no data counts as firing)", "Fires when the generator is idle"],
       ], [36, 80, 54]),
       "Alerts go to a webhook. <font name='Courier'>scripts/alert_sink.py</font> receives them, prints one line each in the terminal and logs them to <font name='Courier'>logs/alerts.log</font>."),
    qa("What if Postgres is down?",
       "The Silver job still commits Silver first; the live-store write is best effort with timeouts, and the pending minutes are repaired by the next successful batch. Silver is always the source of truth."),
    qa("What is the daily ad report?",
       "Airflow DAG <font name='Courier'>ads_yesterday_report</font> (03:00 UTC, logic in <font name='Courier'>batch/ads_report.py</font>) pulls the mock ad API for yesterday once, stores it in "
       "<font name='Courier'>silver_campaign_snapshots</font> and writes <font name='Courier'>data/reports/ads_report_YYYY-MM-DD.json</font>. A rerun on the same day returns the stored report without calling the API. "
       "Bad rows (negative spend, clicks above impressions) go to quarantine. It needs <font name='Courier'>GOLD_BACKEND=databricks</font>."))
story.append(PageBreak())

# PART 2 ------------------------------------------------------------------
S("Part 2. Running it")
add(qa("What do I need installed?",
       "Docker Desktop (running, 8 GB or more), Python 3.11, Git, and a Databricks workspace with a SQL warehouse and your own personal access token. Java is NOT needed on the host."),
    qa("How do I set up on a new machine?",
       *code("""
git clone https://github.com/omarmohosny/funnelstream.git
cd funnelstream
git checkout funnelstream-build        # or main once the pull request is merged
python -m venv .venv
.venv\\Scripts\\python -m pip install -r requirements.txt
Copy-Item .env.example .env            # then fill in the values listed below
docker compose build spark
.venv\\Scripts\\python -m pytest -q      # expect: 645 passed, 121 skipped
"""),
       "Values to set in <font name='Courier'>.env</font>: <font name='Courier'>DATABRICKS_HOST</font>, <font name='Courier'>DATABRICKS_TOKEN</font>, "
       "<font name='Courier'>DATABRICKS_WAREHOUSE_ID</font>, <font name='Courier'>DATABRICKS_CATALOG</font>, <font name='Courier'>POSTGRES_PASSWORD</font>, "
       "<font name='Courier'>GRAFANA_DB_PASSWORD</font>, <font name='Courier'>GRAFANA_ADMIN_PASSWORD</font>. The file is git-ignored: never commit it and never paste tokens in chat."),
    qa("How do I run the live demo?",
       *code("""
# opens the Kafka listener, the Bronze+Silver pipeline and the generator; starts Postgres and Grafana
.\\scripts\\watch_demo.ps1
# stops the pipeline container; then close the windows
.\\scripts\\stop_demo.ps1
"""),
       "Wait about a minute for Spark to start; the generator starts after that. Open http://localhost:3000 to watch the lines rise. Options: "
       "<font name='Courier'>-Events 20000 -Rate 50 -ShowEvery 20 -Sample 3</font>. To see the alerts print, run <font name='Courier'>.venv\\Scripts\\python scripts\\alert_sink.py</font> in another window; "
       "stop the generator and the 'no events' alert fires after 2 to 3 minutes."),
    qa("How do I run the pipeline by hand, step by step?",
       *code("""
docker compose up -d kafka postgres
.venv\\Scripts\\python kafka/create_topics.py
# once per fresh lake
docker compose run --rm spark python scripts/seed_silver_reference.py --lake data/lake
# generator: new --seed every run, same --universe-seed
.venv\\Scripts\\python scripts/run_simulator.py --live --rate 50 --events 3000 `
    --seed 22 --universe-seed 7 --show --show-every 10
# Bronze + Silver in one container
docker compose run --rm spark python -m streaming.jobs.pipeline `
    --lake data/lake --checkpoints data/checkpoints --show 3
"""),
       "The history mode (no <font name='Courier'>--live</font>) loads old days quickly and is what the Gold tables need for a first dataset. Use a new <font name='Courier'>--seed</font> each time."),
    qa("How do I get data into Databricks Gold?",
       *code("""
docker compose run --rm spark python scripts/check_databricks.py --upload-test
# once: creates the tables and seeds the -1 rows
docker compose run --rm spark python scripts/apply_ddl.py --backend databricks
# add --reset --yes after a fresh local run
docker compose run --rm spark python scripts/publish_silver.py
docker compose run --rm spark python -m warehouse.gold_build --days 7
"""),
       "After a schema change use <font name='Courier'>apply_ddl.py --recreate TABLE,TABLE --yes</font> (drops and recreates those tables; it prints the plan and exits 2 without --yes), then republish and rebuild."),
    qa("How do I look at the results?",
       "Databricks SQL editor, for example <font name='Courier'>SELECT * FROM funnelstream.gold.fact_orders LIMIT 20</font>, then fact_attribution, agg_orders_daily, agg_funnel_daily. "
       "Postgres: <font name='Courier'>docker compose exec postgres psql -U fs -d funnelstream -c \"select * from live_metrics_minute order by minute_start desc limit 10;\"</font>. "
       "Local counts: <font name='Courier'>docker compose run --rm spark python data/count.py data/lake/silver/silver_events</font> (create data/count.py from docs/RUNNING.md)."),
    qa("How do I run the tests?",
       *code("""
# host suite, 20-30 s
.venv\\Scripts\\python -m pytest -q
# full suite, about 25 min
docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests -q
# one Spark test file
docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests/test_gold_build_spark.py -q
"""),
       "Run only one Spark container at a time (each can use 4 GB). Expect the full suite: <b>765 passed, 1 skipped</b>. Exit code 137 means out of memory: close other containers and rerun."),
    qa("How much memory does the stack need?",
       "Docker needs about 8 GB: Kafka 1 GB, Postgres 0.5 GB, Grafana 0.5 GB, one Spark container 4 GB (limit and heap are in <font name='Courier'>docker-compose.yml</font> and "
       "<font name='Courier'>streaming/common/spark.py</font>). Airflow (2 GB) is off by default: <font name='Courier'>docker compose --profile airflow up -d airflow</font>. "
       "Do not run the full test suite and the live pipeline at the same time."),
    qa("What are the working rules for the team?",
       *bullets(["Branch from main, one branch per task (<font name='Courier'>task-name</font>), open a pull request, get one review.",
                 "Never commit <font name='Courier'>.env</font>, <font name='Courier'>.venv</font> or <font name='Courier'>data/</font>. Each person uses their own Databricks token.",
                 "Only the Gold build writes Gold. No code outside <font name='Courier'>warehouse/store.py</font> talks to a database directly: use <font name='Courier'>get_store()</font>.",
                 "A new feature needs a test, and parity against the Python reference where a reference exists. Run the host suite before every push.",
                 "Every simulator run needs a new --seed and --universe-seed 7."])))
story.append(PageBreak())

# PART 3 ------------------------------------------------------------------
S("Part 3. Remaining tasks")
add(p("Eight tasks are open before the 13 October defense, plus one stretch task. Dates are the plan from sprint sheet v3 "
      "(<font name='Courier'>docs/FunnelStream-Sprint-Plan-v3.xlsx</font>). Owners are the sprint-sheet members; swap them as the team prefers.", BODY),
    Spacer(1, 4))
add(table([
    ["ID", "Task", "Owner", "Plan", "Hours"],
    ["T12", "agg_campaign_daily", "Member 4", "9-10 Oct", "6"],
    ["T14", "agg_city_product_daily (rec_product cut first)", "Member 4", "10 Oct", "4"],
    ["T15", "Batch runner script", "Member 4", "10-11 Oct", "6"],
    ["T16", "Five SQL checks plus manifest check", "Member 5", "10-11 Oct", "6"],
    ["G3", "Failure demo with Grafana", "Member 5", "11 Oct", "4"],
    ["T17", "Samples, benchmark, README", "Member 5", "11-12 Oct", "6"],
    ["T18", "Power BI model and pages", "Member 1", "11-12 Oct", "10"],
    ["DOC", "Refresh PDF, deck, README screenshots", "Member 5", "12 Oct", "6"],
    ["T19", "Customer 360 (STRETCH, cut first)", "Member 4", "12 Oct", "4"],
    ["M2-M4", "Failure demo run, 1-2M benchmark; rehearsal; defense", "All", "12-13 Oct", "-"],
], [14, 76, 24, 24, 14]))
add(Paragraph("Gold build slots: drop a file named <font name='Courier'>warehouse/gold/10_agg_campaign_daily.sql</font>, "
              "<font name='Courier'>11_agg_city_product_daily.sql</font>, <font name='Courier'>12_rec_product.sql</font> or "
              "<font name='Courier'>13_checks.sql</font> and the runner picks it up in order; update the test that lists empty slots in "
              "<font name='Courier'>tests/test_gold_build.py</font>.", SMALL))

add(qa("T12. agg_campaign_daily: what to build and how do I know it is done?",
       *bullets(["Inputs: <font name='Courier'>silver_campaign_snapshots</font> (latest pull per campaign and report date), <font name='Courier'>fact_attribution</font> (last_touch rows), <font name='Courier'>fact_orders</font>.",
                 "Columns are fixed in <font name='Courier'>warehouse/tables.py</font> (key campaign_id, report_date, attribution_model = 'last_touch'): ad_platform, impressions, clicks, platform_conversions, spend; orders_placed, orders_delivered, orders_cancelled, orders_returned, orders_with_failed_attempt; revenue_placed, revenue_delivered; cancel_rate, failed_attempt_rate, return_rate, open_rate; new_customer_orders, aov, ctr, conversion_rate, cpa, cost_per_delivered_order, roas_placed, roas_delivered.",
                 "Division by zero gives NULL (spend 0, orders 0). Delivered revenue is dated on the ORDER date. Two pulls of the same day never add up: use the latest.",
                 "Done when: the SQL step passes the Gold-build lint tests, a Spark parity test matches a small Python reference or hand-computed rows (zero spend, zero orders, two pulls), the real workspace run is idempotent, and docs/metrics.md matches."])),
    qa("T14. agg_city_product_daily (and rec_product if time allows)?",
       *bullets(["agg_city_product_daily (key city, product_id, date_key): sessions_viewed (<b>distinct sessions</b>, not customers), add_to_cart, units, revenue. City grain on purpose (no district).",
                 "rec_product is cut first. If built: co-view recommendations; use a city for an anchor product only if at least 20 distinct sessions viewed it and the recommended product (labelled co-view, not purchase).",
                 "Done when: parity or hand-computed tests, idempotent real run."])),
    qa("T15. Batch runner script?",
       *bullets(["One command: publish_silver, then gold_build, then the checks (T16), then print a summary and exit non-zero on any failure. Python is primary; the Airflow DAG exists only for the ad report (decision date 10 Oct).",
                 "Takes a store (no direct database imports). Writes the gold_build and publish heartbeats (already done in the two scripts).",
                 "Done when: a fresh clone can run it end to end with the documented commands and it prints a success line with counts."])),
    qa("T16. Which five SQL checks?",
       *bullets(["Silver quarantine and duplicate counts vs <font name='Courier'>noise_manifest.json</font> (tolerance 2%): add a Python check type that reads the manifest.",
                 "No duplicate keys in any Gold table; no orphan keys; share of -1 keys reported.",
                 "Revenue reconciliation: sum of order lines equals sum of orders (250,155.95 in the last real run).",
                 "District hierarchy: zero violations in Silver; reported (non-failing) check for negative delivery_lead_hours and for orders with both a counted cancel and a later shipment.",
                 "Row counts of Gold tables vs expectations. A failing check blocks the Power BI refresh and shows in Grafana."])),
    qa("G3. Failure demo with Grafana?",
       *code("""
1. .\\scripts\\watch_demo.ps1 and open Grafana; the event line is moving.
2. docker stop fs-pipeline      # kill the Spark job: the line drops to zero,
                                # the 'no events' alert fires within 2-3 minutes
3. restart the pipeline window; the line recovers; the alert resolves
4. run the duplicate check on Silver: zero duplicate event_id (a script in scripts/)
"""),
       "Done when: the demo works twice in a row and a short script proves zero duplicates after the restart."),
    qa("T17. Samples, benchmark, README?",
       *bullets(["Export 10,000-row CSV samples per Gold table to <font name='Courier'>samples/</font> (no names or phones).",
                 "Run the pipeline once at 1-2 million events and record time and throughput (10 million is a one-off benchmark only).",
                 "README: architecture, quick start, screenshots of Grafana and Power BI."])),
    qa("T18. Power BI?",
       *bullets(["Connect with the Databricks connector (server hostname and HTTP path from the SQL warehouse, a read-only token). Relationships on customer_key and product_key.",
                 "Build Campaign and Funnel pages first; extra pages only if time remains.",
                 "Done when: the numbers on the pages equal the SQL queries in Databricks for the same period."])),
    qa("DOC. What documents need refreshing?",
       "The PDF (<font name='Courier'>docs/FunnelStream-Documentation.pdf</font>) and the deck are from 5 October: update table names (agg_/rec_), the scope table (Grafana is Required), "
       "Amendments C and D (returned status, COD, platform, district, tracking URL), attribution and live monitoring. This handoff PDF can be reused as the base."),
    qa("What do we cut if we fall behind?",
       "In order: T19 customer 360, then rec_product, then the SQL checks beyond the five key ones, then Power BI pages beyond Campaign and Funnel. "
       "<b>Never cut</b>: attribution, campaign summaries (T12), Grafana, and the failure demo (G3)."))
story.append(PageBreak())

# PART 4 ------------------------------------------------------------------
S("Part 4. Decisions, limits and troubleshooting")
S2("Decisions")
add(qa("Why not a real CDC pipeline (Debezium) for orders and deliveries?",
       "Orders and deliveries already have their own topics, keyed by order id, at lower volume than app events. Real CDC needs Postgres logical replication plus Kafka Connect (about 1 GB more) and a rework of the simulator, "
       "Silver and the status model, with 5 days left. Decision: keep the event-sourced style. Production path to mention in the defense: orders in an operational database captured with Debezium. An optional Debezium-format simulation is possible after the required tasks."),
    qa("Why Databricks for Gold and not Postgres?",
       "Shared workspace for the team, SQL MERGE at scale, and a Power BI connector. Postgres holds only the small live tables. The Gold DDL also exists for Postgres as a fallback, but the Silver mirror tables and the ad report need Databricks."),
    qa("Why last touch, with a 7-day fallback?",
       "It is the simplest defensible model and it is stated on the table (attribution_model). first_touch rows are stored for comparison. The two models differ for a minority of orders (11 of 239 in the parity dataset)."),
    qa("Why does the live 'orders per minute' count purchase events?",
       "One purchase per order in the simulator, with the same late and bot filters and event-time minute as the other panels. It can differ from silver_orders if a purchase event is missing or a session places two orders."))
S2("Known limits")
add(*bullets([
    "Synthetic data: distributions are documented assumptions, not measurements.",
    "Ramadan and Eid dates in dim_date are approximate (moon sighting).",
    "Late status rows older than the Gold window are picked up only by a wider window or --full.",
    "A summary key that vanishes on a widened date outside the window is not deleted.",
    "Anonymous pre-login sessions are not linked to customers after login.",
    "Gold SQL has run on Spark 3.5 locally and on the real Databricks workspace; new SQL must be run on both.",
    "Databricks Free Edition limits apply; the Postgres fallback exists for Gold only.",
    "The live-store write can lose pending minutes if the job stops while Postgres is down (Silver is unaffected).",
]))
S2("Troubleshooting")
add(table([
    ["Symptom", "Cause and fix"],
    ["docker: failed to connect to the docker API", "Docker Desktop is not running. Start it and wait a minute."],
    ["Exit code 137 from a Spark container", "Out of memory. Stop other containers (do not run tests during the live demo) and rerun."],
    ["Missing expression after unary operator '!'", "The ! prefix works only inside Claude Code. In PowerShell run the command without it."],
    ["Silver shows no new rows after a second run", "Same --seed: the ids repeat and Silver drops them as duplicates. Use a new --seed."],
    ["Counts too high after a rerun", "Kafka still holds old messages. Use docker compose down, delete data\\lake and data\\checkpoints, start again."],
    ["Databricks mirror counts higher than local", "The mirror only adds rows. Run publish_silver.py --reset --yes, then publish again."],
    ["Grafana panels empty", "No recent data: run the generator in --live mode. Check docker compose ps for postgres (healthy) and grafana."],
    ["Grafana cannot connect to the database", "GRAFANA_DB_PASSWORD missing from .env when apply_live_ddl ran. Set it and rerun docker compose run --rm spark python scripts/apply_live_ddl.py."],
    ["Gold tables empty", "Run gold_build after publish_silver. fact_* need the Silver mirror filled first."],
    ["A Spark test fails only today", "Check for a hard-coded date against the default 7-day window; pass an explicit --from-date/--to-date window in the test."],
    ["Port 9092 in use", "Another Kafka. Stop it or docker compose down. Kafka in a browser shows nothing: it is not a web page."],
], [58, 112]))


# APPENDIX ----------------------------------------------------------------
S("Appendix")
S2("Repository map")
add(table([
    ["Folder or file", "What is inside"],
    ["simulator/, scripts/run_simulator.py", "Event generator, noise, campaigns, publisher"],
    ["mock_apis/ad_api.py", "Fake Meta/Google ads API (port 8000)"],
    ["contracts/", "Event schemas, validator, normalizers, geo.py (city and district hierarchy)"],
    ["kafka/create_topics.py", "Creates the three topics"],
    ["streaming/jobs/", "bronze_ingest.py, silver_clean.py, pipeline.py (both in one process)"],
    ["streaming/silver/, streaming/common/", "Silver rules and schemas; session, status rank, bot rule"],
    ["streaming/gold/", "Python reference builders (orders, funnel, attribution, live metrics, bots)"],
    ["warehouse/", "store.py (only database layer), tables.py (table spec), gold_build.py, gold/*.sql, live_ddl.sql, live_store.py, heartbeat.py, publish.py"],
    ["scripts/", "apply_ddl, apply_live_ddl, publish_silver, check_databricks, seed_silver_reference, show_live, alert_sink, watch_demo, stop_demo"],
    ["batch/, airflow/dags/", "Ad report logic and its Airflow DAG"],
    ["monitoring/grafana/", "Provisioned datasource, dashboard JSON and alert rules"],
    ["tests/", "Host tests, Spark tests (Docker), integration tests (compose Postgres)"],
    ["docs/", "spec, plan, RUNNING.md, metrics.md, sprint sheet v3, PDFs, this handoff"],
], [58, 112]))
S2("Vocabulary")
add(table([
    ["Term", "Meaning"],
    ["Bronze / Silver / Gold", "Raw / cleaned / business-ready layers"],
    ["Quarantine", "Table of rejected rows with an error code (not a dead-letter topic)"],
    ["Idempotent", "Running twice gives the same result; reruns never duplicate"],
    ["MERGE", "Upsert statement used for every Silver and Gold write"],
    ["SCD2", "Dimension that keeps history with valid_from, valid_to, is_current"],
    ["Unknown member", "Dimension row with key -1 for facts whose dimension row does not exist yet"],
    ["Manifest", "noise_manifest.json: exact count of every injected defect"],
    ["Last touch", "Order credited to the latest campaign click before it"],
    ["COD", "Cash on delivery (skips payment_authorised)"],
    ["District", "Official subdivision (qism or hay) of a city"],
], [48, 122]))
story.append(Spacer(1, 8))
story.append(p("Generated from <font name='Courier'>docs/handoff/build_handoff_pdf.py</font>. To update: edit the script, run it, commit the PDF.", SMALL))


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#64748B"))
    canvas.drawString(20 * mm, 10 * mm, "FunnelStream team handoff - " + AS_OF)
    canvas.drawRightString(A4[0] - 20 * mm, 10 * mm, "Page %d" % doc.page)
    canvas.restoreState()


def build(path=OUT):
    doc = BaseDocTemplate(path, pagesize=A4, leftMargin=20 * mm, rightMargin=20 * mm, topMargin=18 * mm, bottomMargin=18 * mm,
                          title="FunnelStream: team handoff (Q&A)", author="FunnelStream team",
                          subject="Handoff of the FunnelStream data platform")
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="f")
    doc.addPageTemplates([PageTemplate(id="p", frames=[frame], onPage=footer)])
    doc.build(story)
    return path


if __name__ == "__main__":
    print("wrote", os.path.abspath(build(sys.argv[1] if len(sys.argv) > 1 else OUT)))
