# Handoff: state of FunnelStream on 12 October 2026

Defense is on Tuesday 13 October 2026. Branch `funnelstream-build` holds all work (96 commits ahead of `main`, which is still at the original spec commit `dd44fbd`; `main` is an ancestor, so a pull request merges cleanly). This file replaces the handoff of 10 and 11 October. Working rules of the owner: they present the project themself and type the commands in the demo; work is planned in briefs, built by implementer agents, checked independently, and committed or pushed only when the owner says. Git author is passed per command: `git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit ...`, with the line `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>` ending the message.

The step-by-step list of everything that needs Docker or the owner is [PRE-DEFENSE-CHECKLIST.md](PRE-DEFENSE-CHECKLIST.md). Start there.

## What is done

- **Pipeline.** Simulator with injected noise, four Kafka topics (`app.events`, `orders`, `delivery.events`, `inventory.events`), Bronze and Silver Delta tables, publish to the Databricks Silver mirror, the Gold build (12 tables built, `rec_product` an empty slot) and 65 data quality checks, live and health tables in Postgres, Grafana with six dashboards (including "Express delivery SLA" and "Live stock monitor") and eight alert rules, the Airflow DAG `ads_yesterday_report`.
- **Inventory work** (plan `docs/superpowers/plans/2026-10-10-inventory-two-users.md`): all tasks 1 to 11 are built and reviewed on the host: the stock topic and validator, the Silver stock tables, the eight dark stores, the stock ledger and hero stock-out scenario, express orders with a 20-minute promise, the live Postgres tables (15), the Grafana dashboards `fs-inventory-ops`, `fs-inventory-daily` and `marketing-d1`, the alerts `fs-oos-demand` and `fs-express-sla`, `scripts/build_inventory_daily.py`, `scripts/build_d1_marketing.py` and `scripts/smoke_test.py`.
- **Benchmark (T17) is done.** `scripts/benchmark.py --events 1000000 --yes` on 12 October 2026: 1,000,000 simulated events (1,022,333 Kafka messages) through reset, simulate, Bronze, seed, Silver, DDL, publish and Gold in 654.73 seconds. The table is in `docs/benchmark.md`.
- **T18 and T19 are done**: T18 is the Power BI model and pages (next item), T19 the Customer 360 Gold table (`customer_360`, 13th Gold step) with its Power BI page. The sprint sheet `docs/FunnelStream-Sprint-Plan-v3.xlsx` marks G3, T17, T18, T19 and M2 Done; T17 (samples, benchmark, README) is closed by the benchmark above.
- **Power BI is built.** `dashboards/powerbi/FunnelStream.pbip`: 11 Databricks Gold tables and 7 Postgres marts (19 tables with the measures table), 27 relationships, 104 measures, eight pages with 112 visuals and a custom theme. The committed project is the version Power BI Desktop saved after it opened and loaded the generated one. Do not run `scripts/make_pbip.py` over it: the generator replaces the whole report folder. Credentials are not in the file; Desktop asks for a read-only Databricks token and the `grafana_ro` Postgres password.
- **Data model review** (`docs/data-model-review.md`, 25 findings): the section "Resolution status" at the end of that file gives the state of every finding with file and line evidence. On 12 October these were fixed in code with tests: anonymous orders no longer get an `order_seq` or `is_first_order` (I1), the Cart and Checkout Abandon Rate measures are blank for an empty slice and clamped to 0..1 (I7), `dim_product` tracks `is_active` (I10), the JSON schemas match `topics.yaml` and cover the delivery and inventory topics (I12, partly), and the stale test name is fixed (M10). The day rule and the exposed-spend definition are the same in the spec, the plan, `docs/metrics.md`, `MODEL.md` and the code comments.
- **LLM assistant** (`src/llm/`): reviewed and hardened on 12 October. The model can no longer read local files through DuckDB (file access is switched off and the configuration locked after the data is loaded), queries run on their own cursor with a 20 second limit, query results are wrapped and cut so data cannot pass for instructions, `.env` loading is limited to the LLM settings, the Streamlit key field is no longer pre-filled with the environment key, API calls have a 60 second timeout and 3 retries with errors turned into plain messages, the Kafka source uses `confluent-kafka` (the old import named a package that is not in `requirements.txt`), the event tables have explicit columns so a small sample loads, and `is_delivered` is true for completed orders (it was false for every completed order). `duckdb`, `anthropic` and `streamlit` are pinned in `requirements.txt` to the versions installed in `.venv`. Tests: `tests/test_llm_assistant.py`.
- **Housekeeping.** `docker-compose.yml` publishes Kafka, Postgres, Airflow and Grafana on `127.0.0.1` only (takes effect when the containers are recreated, which loses Kafka messages: see the checklist). The scratch files of `data/` (logs, one-off scripts, temporary workbooks) are in `data/_archive/`; the sprint sheet backups stay in `data/`.
- **Host test state:** `.venv\Scripts\python -m pytest tests -q -p no:cacheprovider` printed `1134 passed, 218 skipped, 0 failed` at the end of 12 October.

## Not verified

- **Every Spark test of the inventory work has never run.** Docker's WSL engine crashed several times on 10 October (low free RAM is the suspect). The list of files and counts is in the checklist. Three new Spark tests written on 12 October (anonymous orders in `test_gold_reference.py` and `test_gold_build_spark.py`, `is_active` in `test_gold_build_spark.py`) have not run either.
- **No rehearsal with `-Inventory` has run**, so `docs/RUNNING.md` holds no measured timings for the inventory demo. The sprint sheet marks G3 (the live failure demo, planned for 12 October) Done, but this repository holds no record of two passing runs in a row; if the owner has the evidence, add the numbers to `docs/RUNNING.md`. Fallback: the recorded run plus the Spark replay tests.
- **`scripts/smoke_test.py` has not run for real** (it recreates Kafka and needs the owner's confirmation; delete `data/checkpoints` afterwards).
- **`scripts/validate_pbip.py`** needs the internet. Run on 12 October: 128 files, 77 ok, 0 invalid, 47 not checkable (28 are 404 for `visualContainer/2.13.0`, the schema version Desktop writes and Microsoft does not serve; 17 network time-outs; 2 unresolved references). The 404s are schema-download failures, not violations. Details in the checklist.
- Items of the data model review that need Docker or an owner decision are listed under "Needs owner/Docker" in the review.

## Known limitations to state in the defense

- The out-of-stock flag on a demand minute is the stock when the minute was written. A replay or a late event can flag it differently. The fix (stock intervals from Silver by event time) is described in review item C2.
- No stock reversal on cancellation: stock is reserved when the order is placed.
- Express KPIs exist only in `express_daily` and `live_express_orders`; Gold delivery measures include express orders.
- Store is conformed across the Postgres marts only; Gold has City.
- Ad spend is the ad API's UTC day and orders are a Cairo day; `agg_campaign_daily.report_date` is the ad day (at most 3 hours apart, accepted).
- Exposed spend is an estimate: spend x out-of-stock landing clicks / served landing clicks.
- The out-of-stock demand path does not remove bot sessions (Gold does).

## Other open items

- Monday checklist for the live demo: at least 4 GB free host RAM, `w32tm /resync /force`, no sleep, Docker Resource Saver off; replace the CMOS battery after the defense if the BIOS clock is wrong (the PC clock jumped by days on 10 October; NTP now uses `time.google.com`).
- Stand-in report (not Power BI): artifact "FunnelStream Report", https://claude.ai/artifact/G21YwZmeREmVyLu7jK9KMX, three pages from the Gold samples, private.
- Documentation (PDF, deck, README screenshots), rehearsals for members 2 and 3, and the pull request into `main` (not agreed yet; the steps are prepared in the checklist) are open.
- Sprint sheet statuses: check for the lock file `docs/~$FunnelStream-Sprint-Plan-v3.xlsx` before editing, and do not edit the workbooks by hand.
- Never commit `data/`, `.venv`, `.env`, `.superpowers/` or secrets. Destructive Databricks actions and lake resets need the owner's explicit approval.
- The working ledger of the inventory work is in `.superpowers/sdd/2026-10-10-inventory-two-users/` (git-ignored scratch; `progress.md` is the record).
