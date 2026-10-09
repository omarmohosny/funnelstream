# Running FunnelStream locally

This guide was written from a clean smoke run on Windows 11 (Docker Desktop 29.8, Python 3.11.9, 8 GB given to Docker). All commands are PowerShell, run from the repo root. Durations are from that run; the first run is slower because Spark downloads jars.

## Prerequisites

- Docker Desktop, running, with at least 6 GB of memory allocated (Settings, Resources). Spark needs 3 GB and Kafka 1 GB.
- Python 3.11 on the host (for the simulator, Kafka topic script and host tests).
- Git.
- Internet on the first run (Docker images, Spark and Delta jars, pip packages).
- Java is NOT needed on the host. Spark runs only inside the `spark` Docker Compose service.

## First-time setup

```powershell
git clone <repo-url> funnelstream
cd funnelstream
git checkout funnelstream-build
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
Copy-Item .env.example .env
docker compose build spark
```

`.env` is git-ignored. Never commit it. Edit the password if you like; the Spark pipeline below does not use it.

Check the setup:

```powershell
docker version
.venv\Scripts\python --version
docker compose config
```

## Run the tests

Host (Spark tests are skipped on Windows). Takes about 6 seconds.

```powershell
.venv\Scripts\python -m pytest -q
```

Observed: `485 passed, 74 skipped`. A `PytestUnhandledThreadExceptionWarning` (WinError 10038, from a Kafka client thread) may print at the end; it is harmless.

Everything including Spark tests, in Docker. Takes about 6 to 8 minutes (501 s observed).

```powershell
docker compose build spark
docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests -q
```

Observed: `557 passed, 2 skipped` (about 10.5 minutes). The 2 skips are the tests that need a live Kafka (`RUN_KAFKA_TESTS=1`) and a Postgres (`PG_DSN`).

## Run the pipeline step by step

Notes:

- Scripts run both as plain files (`python scripts/run_simulator.py`) and as modules (`python -m scripts.run_simulator`), from any folder.
- Lake and checkpoint paths may be relative (`data/lake`, `data/checkpoints`); every job turns them into absolute paths itself. In the Spark container the working directory is `/app` (the repo is mounted there), so `data/lake` is the `data\lake` folder in the repo.

### 0. Clean state (only if you ran before)

```powershell
Remove-Item -Recurse -Force data\lake, data\checkpoints, noise_manifest.json -ErrorAction SilentlyContinue
docker compose down -v
```

Kafka keeps its messages inside its container. A rerun without `docker compose down` re-ingests the old Kafka messages (a new Bronze checkpoint reads the topics from the start), so counts will not match a fresh run. `docker compose down` removes the containers, and with them the Kafka topics and messages. `docker compose down -v` also deletes the named volumes: `pgdata` (Postgres data) and `ivycache` (the downloaded Spark jars, so the next Spark run downloads them again). Use plain `down` if you only want clean Kafka and want to keep the jar cache. After either, start again from step 1.

### 1. Start Kafka and create topics

```powershell
docker compose up -d kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list
.venv\Scripts\python kafka/create_topics.py
```

The `--list` command prints nothing until Kafka is up and then returns without an error (about 5 to 30 seconds after `up`); repeat it until it does not fail. Expected output of `create_topics.py`:

```
created app.events
created orders
created delivery.events
```

Partitions: `app.events` 6, `orders` 3, `delivery.events` 3. Check with:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe
```

### 2. Generate data (history mode, noise on)

```powershell
.venv\Scripts\python scripts/run_simulator.py --events 20000 --seed 7 --bootstrap localhost:9092
```

Takes a few seconds. Expected last line:

```
{'app.events': 19316, 'orders': 667, 'delivery.events': 383} total 20366
```

It also writes `noise_manifest.json` in the current folder (git-ignored). Its `total_messages` is 20366 and `injected` lists how many of each noise type were added. A harmless librdkafka line about "Coordinator load in progress" can appear right after Kafka starts.

### 3. Bronze ingest

Bronze is a continuous stream. Run it for a bounded time and let `timeout` stop it:

```powershell
docker compose run --rm spark timeout 120 python -m streaming.jobs.bronze_ingest --lake data/lake --checkpoints data/checkpoints
```

Bronze never finishes on its own, so `timeout` stops it: it ends after 120 s with exit code 124 (`$LASTEXITCODE` is 124). That is expected, not a failure. Data is safe because each 10 s micro-batch is committed to Delta. The first run also downloads Spark jars (extra minute or two). It writes Delta tables under `data/lake/bronze/` and checkpoints under `data/checkpoints/`. The data was consumed in well under 120 s.

Count the Bronze rows. This uses a one-off Spark script; save it as `data\count.py` (inside the git-ignored `data` folder) and delete it afterwards:

```python
import sys
from streaming.common.spark import get_spark
sp = get_spark("count")
sp.sparkContext.setLogLevel("ERROR")
tot = 0
for p in sys.argv[1:]:
    n = sp.read.format("delta").load(p).count(); tot += n
    print("COUNT %s %d" % (p, n))
print("COUNT total", tot)
```

```powershell
docker compose run --rm spark python data/count.py data/lake/bronze/bronze_app_events data/lake/bronze/bronze_orders data/lake/bronze/bronze_delivery_events
```

Expected: 19316, 667, 383, total 20366, equal to the simulator total and `total_messages`.

### 4. Seed reference data (products and customers)

```powershell
docker compose run --rm spark python scripts/seed_silver_reference.py --lake data/lake
```

About 15 s. Expected last line: `silver_products 200 rows offered; silver_customers 5000 rows offered`. Silver needs this to recognise valid product ids.

### 5. Silver

```powershell
docker compose run --rm spark python -m streaming.jobs.silver_clean --once --lake data/lake --checkpoints data/checkpoints
```

About 40 s. Expected log line:

```
INFO:silver_clean:batch 0: {'silver_events': 18822, 'silver_orders': 153, 'silver_order_status': 1031, 'silver_order_items': 305, 'silver_quarantine': 314} district_violations=0
```

Row counts after this step (same `count.py`, paths `data/lake/silver/<table>`):

| Table | Rows |
|---|---|
| silver_events | 18822 |
| silver_orders | 153 |
| silver_order_status | 1031 |
| silver_order_items | 305 |
| silver_quarantine | 314 |
| silver_products | 200 |
| silver_customers | 5000 |

The quarantine count equals the sum of the manifest counts for the defects the validator can see: malformed_json 80 + missing_field 50 + unknown_type 21 + bad_version 20 + negative_price 14 + future_timestamp 17 + null_product_id 65 + unknown_product_id 47 + cancel_after_ship 0 = 314. Also: 20366 - 314 quarantined - 199 duplicates = 19853 = silver_events + silver_order_status (18822 + 1031). The manifest also lists dirty_district 345 and dirty_utm 68: Silver cleans both (no quarantine), and every district ends valid (`district_violations=0`). In history mode every row is flagged `is_late`; that is expected.

Running Silver again changes nothing (checked: same counts, and also the same after deleting `data/checkpoints/silver_clean` and replaying).

After Amendment C (step C2) `silver_events` and `silver_orders` have a nullable `platform` column (`app` or `web`) right after `channel`, and `silver_order_status` can hold `order_returned` rows (rank 7). The counts above were measured on a fresh run after Amendment D (seed 7, 20,000 events). `silver_campaign_snapshots` (Databricks mirror, no local Silver job yet) gains `platform_conversions` right after `clicks`.

After Amendment D, `silver_events` has `district` (right after `city`), `utm_medium` (after `utm_campaign`) and `page_url` (after `click_id`); `silver_orders` has `district` and `utm_medium` in the same places; `silver_customers` has `district` after `city`. `utm_source`, `utm_medium` and `utm_campaign` of an event are parsed from its `page_url` (older Bronze data without `page_url` falls back to the payload fields, `utm_medium` NULL). A district outside its city becomes `unknown` and the row is kept; `silver_clean` logs the number of such events and orders per batch as `district_violations=N`. Existing Silver Delta tables must be recreated (delete `data/lake/silver` and the Silver checkpoint, rerun `seed_silver_reference.py` and Silver) because the schemas changed. No counts were measured for this change in this document.

### 6. Gold reference build

```powershell
docker compose run --rm spark python -m streaming.gold.build --lake data/lake --out data/lake/gold_ref
```

About 25 s. Expected:

```
fact_orders: <n> rows
fact_order_lines: <n> rows
fact_attribution: <n> rows
agg_orders_daily: <n> rows
agg_funnel_daily: <n> rows
```

The row counts `<n>` depend on the simulated run and were not measured on a full local run. The Gold test dataset of 30,000 events (a test-dataset number, not a full-run number) gives 239 orders, 478 `fact_attribution` rows (two per order), 29 `agg_orders_daily` rows and 48 `agg_funnel_daily` rows. `fact_orders.campaign_id` is the `last_touch` campaign here. `fact_orders` carries `platform`, `returned_at`, `has_failed_attempt` and `is_returned`; its `customer_key` is -1 here because the reference build reads no dimensions.

This is a local reference build under `data/lake/gold_ref`, not the real Gold store (see "What is not built yet").

### 7. Live metrics

```powershell
docker compose run --rm spark python -m streaming.gold.live --lake data/lake --minutes 60
docker compose run --rm spark python scripts/show_live.py --lake data/lake
```

Options of `streaming.gold.live`: `--lake` (default `data/lake`), `--minutes` (window, default 60), `--lookback-hours` (bot detection lookback, default 6). It prints `WROTE <path> (<n> minute rows)`; after the history run: `WROTE /app/data/lake/live/live_metrics_minute (0 minute rows)`.

With history-mode data this table is empty (it only counts non-late events from the last 60 minutes; history events are 1 to 7 days old and flagged late). `show_live.py` then prints an empty table. Use the live demo below to see rows.

## Live demo

Needs three terminals (Kafka must be up and topics created). Bronze and Silver stream until stopped (Ctrl+C), or bound them with `timeout`.

Terminal 1, Bronze:

```powershell
docker compose run --rm spark python -m streaming.jobs.bronze_ingest
```

Terminal 2, Silver streaming:

```powershell
docker compose run --rm spark python -m streaming.jobs.silver_clean --lake data/lake --checkpoints data/checkpoints
```

Terminal 3, simulator:

```powershell
.venv\Scripts\python scripts/run_simulator.py --live --rate 50 --events 3000 --seed 11 --bootstrap localhost:9092
```

Live options: `--rate` (sessions are spaced so the app-event rate is about this value), `--time-scale` (default 21600: order lifecycle delays are divided by it, so 6 hours become 1 second), `--session-scale` (default 60: the time spread inside one session is divided by it, so a 5 minute session takes 5 s), `--max-tail-seconds` (default 60: no order event comes later than this after its order_created). Event order is kept.

Observed after the pacing fix: `{'app.events': 2853, 'orders': 115, 'delivery.events': 70} total 3038` in 66 s. Before the fix the same command took 6 min 22 s (about 8 events per second): a session lasts up to 5 minutes and was replayed in real time, so events kept coming for minutes after the last session started. Use a smaller `--events` for a shorter demo. If you bound Bronze and Silver with `timeout`, make it longer than the simulator run, otherwise finish with one more Bronze run (`timeout 60 ... bronze_ingest`) and `silver_clean --once`.

Then refresh and show:

```powershell
docker compose run --rm spark python -m streaming.gold.live --lake data/lake --minutes 60
docker compose run --rm spark python scripts/show_live.py --lake data/lake --n 5
```

Observed after a history run plus the live run above (then `timeout 60` Bronze and `silver_clean --once`): `WROTE /app/data/lake/live/live_metrics_minute (2 minute rows)`. Columns: minute_start, country, events, active_sessions, product_views, add_to_cart, orders_placed, newest first:

```
2026-10-04 20:23:00  EG  1572  144  479  49  14
2026-10-04 20:22:00  EG  1151  111  349  45  12
```

## Live and health tables

The Silver job fills three small Postgres tables that Grafana reads (spec 17.3, 17.17; the schema is `warehouse/live_ddl.sql`):

| Table | One row per | Holds |
|---|---|---|
| `live_metrics_minute` | minute (UTC) and country | `events`, `active_sessions`, `product_views`, `add_to_cart`, `orders_placed` of non-late, non-bot events (the rules of `streaming/gold/live.py`) |
| `pipeline_health_minute` | minute (UTC) | `events` (Silver events by Kafka arrival minute), `late_events`, `quarantined` (by `received_at`), `silver_batches` (batches finished in the minute) |
| `pipeline_heartbeat` | component | `last_seen_at` and `details` for `silver_job` (`batch N` after every batch and `alive` every 15 s while the Silver query is active), `gold_build` and `publish` (after a successful run) |

`pipeline_batch_log` (one row per finished Silver batch, key `job_run_id`, `batch_id`) is the source of `silver_batches`. Dashboards and alerts read these tables only, through the read-only role `grafana_ro` (SELECT on these four tables, password from `GRAFANA_DB_PASSWORD` in `.env`; `.env.example` lists the names).

Apply the DDL (idempotent, safe to repeat; it prints what it created and never prints a password):

```powershell
docker compose up -d postgres
docker compose run --rm spark python scripts/apply_live_ddl.py      # PG_DSN inside the container points at host "postgres"
```

Without `GRAFANA_DB_PASSWORD` the tables are created, the role is skipped, a red error is printed and the exit code is 3 (`watch_demo.ps1` prints a warning and carries on).

How the Silver job fills them: after each micro-batch `process_batch` succeeds, the job takes the minutes the batch touched (event-time minutes of its Silver events, Kafka-arrival minutes, quarantine minutes, and the minute the batch finished) and recomputes the full totals for those minutes from the Silver tables. Each measure is written with `INSERT ... ON CONFLICT DO UPDATE SET measure = EXCLUDED.measure`, so a total is always replaced, never incremented, and running a batch twice changes nothing. It is on when `PG_DSN` is set (the `spark` service sets it, host `postgres`) or `--live-store` is passed, and off otherwise (no psycopg import, the Silver flow is unchanged). The log shows one line per batch: `live_store: N metric minutes, M health minutes`. A Postgres outage never fails a batch: a warning is logged, the minutes stay pending and are written with the next batch that reaches the database (retry after 30 seconds). Limitations: the bot rule looks back 6 hours before the first touched minute, as in `streaming/gold/live.py`; minutes pending after an outage are lost if the job stops before the database is back (restart repairs only minutes that later batches touch).

Check what was written:

```powershell
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT component, details, now() - last_seen_at AS age FROM pipeline_heartbeat" -c "SELECT * FROM live_metrics_minute ORDER BY minute_start DESC LIMIT 5" -c "SELECT * FROM pipeline_health_minute ORDER BY minute_start DESC LIMIT 5"'
```

History-mode data is all late (by rule), so `live_metrics_minute` fills only from `--live` runs; `pipeline_health_minute` fills for both.

The heartbeats of `publish_silver.py` and `warehouse/gold_build.py` are best effort (3 second connect timeout, errors swallowed, nothing happens without `PG_DSN`); on the host set `PG_DSN` (the value in `.env` already points at `localhost:5432`) to have them written.

### One combined streaming job

```powershell
docker compose run --rm spark python -m streaming.jobs.pipeline --lake data/lake --checkpoints data/checkpoints --show 3 --live-store
```

`streaming.jobs.pipeline` runs the three Bronze queries and the Silver query in one process and one Spark session (one container instead of two, about 3 GB). `--show N` prints the same `[bronze]` progress lines and `[silver]` batch views as the two separate jobs; Ctrl+C stops all four queries. Missing Bronze tables are created empty first so the Silver stream can start at once. `streaming.jobs.bronze_ingest` and `streaming.jobs.silver_clean` still work on their own.

## Watch the data move (generator, Kafka, pipeline)

`scripts\watch_demo.ps1` starts Kafka (topics created), Postgres (waits for `pg_isready`, applies the live DDL) and Grafana, then opens three windows: a Kafka listener (every message with its topic), `FS 2 Pipeline` (one Spark job: a `[bronze]` line per batch with row counts and Kafka offsets plus the newest raw messages, a `[silver]` view per batch with counts, the newest cleaned rows and quarantine rows; it also writes the live and health tables) and the generator (one line per published event). Memory target for the whole stack: Kafka 1 GB, Postgres 0.5 GB, Grafana 0.5 GB, one Spark container 3 GB. It gives Spark about a minute to start, picks a new seed each run (kept in `data\state\next_seed.txt`) so ids never repeat, and prints the Grafana URL at the end: http://localhost:3000.

```powershell
.\scripts\watch_demo.ps1                     # rounds of 3000 events at 50 per second, until Ctrl+C in the generator window
.\scripts\watch_demo.ps1 -Events 20000 -ShowEvery 20
.\scripts\watch_demo.ps1 -Once               # stop after one round of -Events events
.\scripts\watch_demo.ps1 -SyncEvery 120      # Gold sync every 2 minutes (default 300; 0 = no sync window)
.\scripts\stop_demo.ps1                      # stops the pipeline and sync containers; then close the windows
```

By default the generator runs with `--forever` (rounds of `--events` events; round i uses seed `seed*100000+i`, so ids never repeat). A fourth window, `FS 4 Gold sync` (`scripts\sync_gold.ps1`), runs `publish_silver.py` and then `warehouse.gold_build --days 7` every `-SyncEvery` seconds in the 1.5 GB `sync` container, so data reaches Gold without manual commands. A failed round is retried on the next one. The manual commands in the sections below still work unchanged. If `localhost:9092` or `localhost:3000` stop answering after Docker Desktop restarts (the port forwarding goes stale), run `docker compose restart kafka` or `docker compose up -d --force-recreate grafana`.

The same views by hand: `streaming.jobs.pipeline --show [N]`, or `bronze_ingest --show [N]` and `silver_clean --show [N]` as two jobs (N = rows per batch, default 3), and `run_simulator.py --show [--show-every N]`. `--universe-seed 7` keeps the same customers and products on every run; `--seed` must be new on each run (the same seed regenerates the same ids and Silver drops them as duplicates).

## Publish to Databricks

Pushes the allow-listed Silver columns to a Databricks Silver mirror. Verified against a real Databricks workspace (Free Edition, catalog `funnelstream`, SQL warehouse, volume `silver_upload`) with a 5000-event run: `check_databricks.py --upload-test`, `apply_ddl.py --backend databricks`, `publish_silver.py` (dry run, full run twice, `--upload-only`) all ran for real.

Prerequisites:

- A Databricks workspace (Free Edition is enough), a running SQL warehouse, and a personal access token (Settings, Developer, Access tokens).
- A catalog. Free Edition may not let you create one, so set `DATABRICKS_CATALOG=workspace` (an existing catalog) in `.env`. The default is `funnelstream`, which must then exist.
- `.env` (copied from `.env.example`) with `DATABRICKS_HOST` (workspace URL), `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID` (or `DATABRICKS_HTTP_PATH`), `DATABRICKS_CATALOG`, `DATABRICKS_VOLUME` (default `silver_upload`). The `spark` container reads `.env` when it exists; the one variable `DATABRICKS_CATALOG` drives `apply_ddl`, `publish_silver` and `check_databricks`.
- Rebuild the image once (`docker compose build spark`): it now contains the Databricks SDK, SQL connector and pyarrow.

Commands (PowerShell, repo root):

```powershell
# 1. Settings and connection (run: prints the missing-settings message without a workspace)
docker compose run --rm spark python scripts/check_databricks.py
# 2. Create schemas gold and silver, the upload volume and all tables (run: --dry-run only)
docker compose run --rm spark python scripts/apply_ddl.py --backend databricks --dry-run
docker compose run --rm spark python scripts/apply_ddl.py --backend databricks
# 2b. Schema change on existing tables (Amendment C added columns; CREATE IF NOT EXISTS does not alter them).
#     Drops the tables AND their data, then recreates them; prints the plan and exits 2 without --yes. Verified for real (see "Migrating an existing workspace").
#     docker compose run --rm spark python scripts/apply_ddl.py --backend databricks --recreate silver_events,silver_orders,silver_campaign_snapshots [--yes]
# 3. Connectivity including a tiny upload to the volume (verified against a real workspace)
docker compose run --rm spark python scripts/check_databricks.py --upload-test
# 4. Publish (verified for real): dry run first (exports locally, prints the MERGE it would run, uploads nothing)
docker compose run --rm spark python scripts/publish_silver.py --dry-run
docker compose run --rm spark python scripts/publish_silver.py
docker compose run --rm spark python scripts/publish_silver.py --upload-only   # verified: re-uploads data/export and merges
```

Observed on a real workspace after `--events 5000 --seed 7` (local Silver count equals Databricks mirror count for every table): silver_events 4649, silver_orders 39, silver_order_status 291, silver_order_items 72, silver_quarantine 79, silver_products 200, silver_customers 5000, silver_campaign_snapshots 0 (skipped). `sum(line_total)` 51763.44 on both sides, `silver_quarantine.raw_payload` NULL everywhere, `silver_customers` has no name/phone columns. A second full publish and an `--upload-only` run left every count unchanged. `MERGE ... USING (SELECT ... FROM parquet.`/Volumes/...`)` works as written on the SQL warehouse. Gotcha fixed during this run: Spark names Parquet parts with a new UUID per export, so earlier exports piled up in the volume folder and the MERGE source held duplicate rows; the upload now deletes old `*.parquet` files in the table's volume folder first.

`--help` on all three scripts was run. `apply_ddl.py --catalog NAME` overrides the catalog; `publish_silver.py` also takes `--tables`, `--export-only`, `--upload-only`, `--lake`, `--export-dir`. Publishing is insert-only and idempotent (a second run changes nothing); the MERGE source is de-duplicated on the table keys (QUALIFY ROW_NUMBER), and the only update is a narrow repair: `silver_products.name` is filled on an already published row when the mirror holds NULL. `silver_quarantine.raw_payload` is never published (the mirror column stays NULL). Product titles (`silver_products.name`) are published; this is the single exception to the ban on columns called name, phone, email or address. `search_term` and `reason` are free-text columns that are published because the data is synthetic: review them before using real customer data. Verified on a real workspace (seed 7, 5,000 events, two publishes in a row): `silver_events` count 6386 = distinct event_id 6386, `silver_products` 200 rows with 200 non-null names, `silver_customers` 5000 = 5000 distinct keys, `silver_order_items` 127 = 127 distinct keys. `silver_campaign_snapshots` has no local Delta table yet and is skipped with a warning. If `check_databricks` says the catalog is not found, create it in Catalog Explorer or point `DATABRICKS_CATALOG` at an existing one such as `workspace`.

### Migrating an existing workspace after a model change

Verified against the real workspace (old Gold and Silver mirror tables, all synthetic) after Amendment C. Without `--yes` the command prints the DROP plan and exits 2 (dry run).

```powershell
docker compose run --rm spark python scripts/apply_ddl.py --backend databricks --recreate dim_customer,dim_product,dim_campaign,dim_date,fact_orders,fact_order_lines,fact_attribution,agg_orders_daily,agg_funnel_daily,agg_campaign_daily,agg_city_product_daily,rec_product,customer_360,silver_events,silver_orders,silver_customers,silver_campaign_snapshots --yes
```

Observed: `applied 43 statements`, exit 0. The explicit key -1 insert into the `GENERATED BY DEFAULT` identity columns (seed MERGE), the partitioned Silver mirror tables and the DECIMAL columns all worked on the real warehouse. `dim_customer`, `dim_product` and `dim_campaign` hold exactly one row with key -1 each, `dim_date` none; a second plain `apply_ddl.py --backend databricks` (27 statements) left them at one each. A MERGE into `dim_product` without a key got a positive generated key (1); the probe row was deleted afterwards.

`--recreate` only knows the tables of the new model, so Gold tables of the old model (`fact_funnel_daily`, `fact_campaign_daily`, `fact_city_product_daily`, `fact_product_recs`, `fact_customer_360`) stay behind. Drop them once by hand (one-off, names fixed in the script, run through the store's `execute` with `DROP TABLE IF EXISTS <catalog>.gold.<name>`); `SHOW TABLES IN <catalog>.gold` then lists exactly the 13 new Gold tables.

Silver mirror tables not in the recreate list keep their old rows, because publishing is insert-only. After a new local run (`--events 5000 --seed 7`) the mirror counts for `silver_order_status`, `silver_order_items` and `silver_quarantine` were higher than local (707, 189, 227 against 250, 74, 83) until these three were recreated too and republished; then every count and distinct key equalled the local count on all tables, and a second publish changed nothing. Recreate every Silver mirror table you want to match a new run exactly.

## Yesterday's ad report (Airflow DAG `ads_yesterday_report`)

Pulls the mock ad API for yesterday (UTC day), stores it in `silver_campaign_snapshots` and writes `data/reports/ads_report_<date>.json`. The first run of a day pulls (`pulled`); any rerun the same day returns the stored report without calling the API (`cached`). The pull is final (as of 00:00 UTC today), so numbers never change on a rerun. Rows with negative spend or clicks above impressions go to `silver_quarantine`. Schedule: 03:00 UTC daily. Needs `GOLD_BACKEND=databricks` (the Silver mirror tables exist only there).

```powershell
.venv\Scripts\python -m mock_apis.ad_api                        # terminal 1: mock ad API on :8000
docker compose --profile airflow up -d airflow                  # terminal 2: Airflow UI on http://localhost:8080 (2 GB, off by default)
docker compose --profile airflow logs airflow | Select-String password   # standalone admin password
# Without Airflow, same logic from the host (the host does not read .env: export GOLD_BACKEND=databricks and the DATABRICKS_* values first):
.venv\Scripts\python -m batch.ads_report                        # --report-date YYYY-MM-DD, --force
```

In the UI trigger the DAG with config `{"report_date": "2026-10-03"}` to backfill a day or `{"force": true}` to pull again. Verified for real: `airflow dags test ads_yesterday_report` twice against the real workspace printed `pulled` then `cached` with identical numbers.

### Migrating after Amendment D (district, utm_medium, page_url)

Amendment D added columns to existing tables, and `CREATE TABLE IF NOT EXISTS` does not alter them. Verified for real on the workspace: recreate the six Silver mirror tables that changed or hold older rows and the four Gold tables that gained `district`, then republish. `silver_products`, `silver_campaign_snapshots` (it keeps the ad-report rows) and the other Gold tables stay.

```powershell
docker compose run --rm spark python scripts/apply_ddl.py --backend databricks --recreate silver_events,silver_orders,silver_order_status,silver_order_items,silver_quarantine,silver_customers,dim_customer,fact_orders,fact_order_lines,customer_360 --yes
docker compose run --rm spark python scripts/publish_silver.py
```

Observed: `applied 37 statements`, then every Silver mirror count equalled the local count after a fresh run (seed 7, 20,000 events): events 18,822, orders 153, status 1,031, items 305, quarantine 314, products 200, customers 5,000. In the mirror: 0 NULL districts in events, orders and customers; 1,340 events with `utm_source` and `utm_medium`; 7,387 with `page_url`; 0 duplicate `event_id`; `raw_payload` NULL in every quarantine row; `dim_customer`, `dim_product` and `dim_campaign` each hold exactly one -1 row (the customer one has district `unknown`). The Gold tables are empty until the Gold build runs (see "Gold build").

## Gold build

`warehouse/gold_build.py` recomputes the Gold tables on Databricks from the Silver mirror (publish first). It is one job with SQL files `warehouse/gold/NN_<step>.sql` (MERGE ... SELECT, Databricks SQL / Spark SQL with Delta; not portable to the Postgres fallback). It only uses the store interface (`store.execute` / `store.query`).

```powershell
docker compose run --rm spark python -m warehouse.gold_build --dry-run                      # print the rendered statements, execute nothing (needs no Databricks settings)
docker compose run --rm spark python -m warehouse.gold_build                                # default window: the last 7 days up to today (Cairo)
docker compose run --rm spark python -m warehouse.gold_build --days 30                      # the last 30 days up to today
docker compose run --rm spark python -m warehouse.gold_build --from-date 2026-10-01 --to-date 2026-10-05
docker compose run --rm spark python -m warehouse.gold_build --full                         # rebuild everything
docker compose run --rm spark python -m warehouse.gold_build --steps fact_orders,agg_orders_daily
```

Order (fixed, whatever `--steps` says): `dim_customer`, `dim_product`, `dim_campaign`, `dim_date`, `fact_orders`, `fact_order_lines`, `fact_attribution`, `agg_orders_daily`, `agg_funnel_daily`, `agg_campaign_daily`, `agg_city_product_daily`, `rec_product`, checks. The run stops at the first failing step and names it (exit 1); rerunning is safe because every step is a MERGE of recomputed rows. Every step starts with `SET TIME ZONE 'UTC'` (the Cairo dates are computed with `from_utc_timestamp` and only come out right when the session zone is UTC, so the build pins it itself, per step, so it also holds after a reconnect). It prints, per step, the statement count and the row count of the target table, the number of orders without `order_created_at` (they are left out of `fact_orders`, like the reference builder does) and the three attribution counts.

The three window options are `--from-date/--to-date` (both, ISO dates, Cairo days), `--days N` and `--full`; with none of them the window is the last 7 days. The window selects what is recomputed:

- Orders: an order is recomputed when its `order_created_at` Cairo date or any of its status rows `event_date` is in the window. Later orders of the same customers are recomputed too (their `order_seq` and `is_first_order` depend on the earlier order).
- Summaries (`agg_orders_daily`, `agg_funnel_daily`, `agg_campaign_daily`, `agg_city_product_daily`): one atomic MERGE recomputes the rows: measures of matching keys are updated, new keys inserted, and a key inside the window that has no rows any more is deleted (`WHEN NOT MATCHED BY SOURCE`), so a failure never leaves the window empty. A key that vanishes on a widened date outside the window (see below) is not deleted. `agg_orders_daily` also recomputes the order dates of every recomputed order (a late delivery for an old order fixes the day the order was placed). `agg_funnel_daily` also recomputes the dates of sessions that touch the window and the dates of late events (`event_date` before the window, `kafka_timestamp` inside it). Bots are recomputed over all events of the sessions involved (more than 100 events in 10 seconds).
- Late status rows older than the window: `silver_order_status` has no ingestion time, so orders are found only by the order date or the status row `event_date` (the Cairo date of `status_time`). A late status row whose `status_time` is older than the window (for example a delivered row 9 days after the order, in the default 7-day window) is NOT seen: the order, its lines and its `agg_orders_daily` day stay stale until a window that covers that date, a wider `--days`, or `--full` runs. The funnel is not affected (it also looks at `kafka_timestamp`). Run `--full` periodically (for example weekly) or use a wider `--days`.
- Dimensions are always rebuilt in full (they are small): SCD2 with `valid_from = updated_at`, one new version per day at most, new versions only when city, district and primary_device (customer) or category and price (product) change. `dim_campaign` comes from `simulator/campaigns.py`, `dim_date` covers 2025-01-01 to 2027-12-31.
- The -1 repair: a fact row whose customer or product had no dimension version gets key -1 and keeps the natural id. Every recomputed order or line looks the key up again, so once the dimension row exists the next build that recomputes that order repairs the key. Orders outside the window keep their key until a window (or `--full`) covers them again.
- Attribution (`fact_attribution`, `warehouse/gold/07_fact_attribution.sql`, after `fact_order_lines`): exactly two rows per order, `last_touch` and `first_touch`, key `(order_id, model)`, `order_total` copied from the order. Rules (full text in `docs/metrics.md`, "Attribution model"): (1) the order's own session click, found by the order's `click_id`; (2) if there is none and the order has a `customer_id`, the customer's clicks from any session in the 7 days before `order_created_at` (from `order_created_at - 7 days` inclusive to `order_created_at` exclusive), `last_touch` the latest and `first_touch` the earliest; (3) otherwise direct: `campaign_id` and `click_id` NULL. A click is the first `page_view` or `product_view` of a session that carries a `utm_campaign`; bot sessions (recomputed, same rule as the funnel) are excluded; the campaign is `silver_events.campaign_id` when set, else the `dim_campaign` row with that `utm_campaign`; an unresolved UTM is not a click. Ties on click time go to the smallest `click_id`.
  - Window: the step recomputes the orders of the `fact_orders` window (order date or status row in the window), the orders that have no attribution row yet, and the orders whose inputs changed: orders reached (own `click_id`, or same customer within 7 days after the click) by a click candidate of any session that has an event dated in the window or a late event (`event_date` before the window, `kafka_timestamp` Cairo date inside it). It is one atomic MERGE; an order that is no longer in `fact_orders` is deleted from the same MERGE (tombstone rows: `NOT MATCHED BY SOURCE` cannot be limited to the window orders because the table has no date column). A second statement of the file MERGE-updates `fact_orders.campaign_id` from the `last_touch` rows (NULL for direct orders), writing only changed values; it is the only column of `fact_orders` this step touches, and the `fact_orders` step never overwrites it.
  - First run and gaps: the first run (or any `--full`) recomputes every order. Orders with a NULL `order_created_at` or no status row are not in `fact_orders`, so they get no attribution row. If a `silver_events` row is removed or rewritten, an order outside the window keeps its old attribution until a covering window or `--full` runs.
  - Real workspace check: on the Databricks workspace (351 orders, seed 7 data plus two small live runs) the step wrote 702 rows (two per order); 145 orders were attributed to a campaign and 206 were direct, and the result equalled the local reference build with 0 mismatches in all five Gold tables.
  - Reported after the step: `orders_without_click` (orders whose own session has no usable click), `orders_direct` (orders with no campaign in `last_touch`) and `unresolved_clicks` (first UTM events of non-bot sessions that no campaign could be matched to). Counts only, over the whole table.
  - Limits: a click row that arrives late for an old session with an `event_date` and a `kafka_timestamp` both before the window is found only by a covering window or `--full`. Linking an anonymous pre-login session to a customer after login is out of scope (spec 17.11): such orders can show as direct, and a click only belongs to the customer id its own event carries.
- `agg_campaign_daily` (`warehouse/gold/10_agg_campaign_daily.sql`, after `agg_funnel_daily`): one row per campaign, Cairo report date and attribution model (always `last_touch`), for every campaign-day that has ad data or attributed orders. Ad measures (`ad_platform`, `impressions`, `clicks`, `platform_conversions`, `spend`) come from `silver_campaign_snapshots`: only the pull with the latest `pulled_at` per campaign and day counts (earlier pulls stay in Silver and are never summed; on a tie in `pulled_at` the row with the greatest impressions, then clicks, platform_conversions, spend, ad_platform wins; rows with a NULL `campaign_id` or `report_date` are ignored). Order measures come from `fact_orders` joined to the `last_touch` rows of `fact_attribution` (direct orders and the `unknown` campaign are left out) and sit on the Cairo date the order was PLACED, so delivered revenue is counted on the order day. A day with ad data and no orders has order counts 0, revenue 0.00 and NULL order rates; a day with orders and no ad row has NULL ad measures and NULL ctr, conversion_rate, cpa, cost_per_delivered_order, roas_placed and roas_delivered. Rates are `DECIMAL(8,4)` (4 decimals), money ratios `DECIMAL(12,2)`, both from a double division and NULL when the denominator is 0 or NULL. `open_rate` uses `current_timestamp()`: an order counts as open when it is not delivered, cancelled or returned and was placed more than 14 days ago, so the value changes with the wall clock and only refreshes when the row is recomputed. Window: one atomic MERGE recomputes every campaign-day whose `report_date` is in the window plus the order days of every order the `fact_orders` step recomputed (a late status row for an old order moves its day), and deletes a campaign-day inside the window that has neither ad data nor orders any more. Limits: a vanished key on a day outside the window is not deleted, and a late snapshot pull for an old report day is picked up only when that day is in the window (or is the day of a recomputed order). `silver_campaign_snapshots` is empty in the local lake, so the local reference build (`streaming/gold/agg_campaign.py`) only has ad rows when a test supplies them.
- `agg_city_product_daily` (`warehouse/gold/11_agg_city_product_daily.sql`, after `agg_campaign_daily`): one row per city, product and Cairo day (`date_key`, yyyyMMdd, the same day as `fact_order_lines.date_key`), at city grain (no district). `sessions_viewed` is the number of DISTINCT sessions with at least one `product_view` event for the product in that city that day, and `add_to_cart` the number of distinct sessions with at least one `add_to_cart` event for the product (the Silver `product_id` column is filled for both event types). Both count sessions, not events, so a session that views a product three times counts once; a session that has events in two cities on one day counts once in each. The event day is the Silver `event_date`; the city is the event's own `city`. Same exclusions as the funnel: bot sessions are recomputed over all events of the sessions involved (more than 100 events in 10 seconds, the stored `is_bot` is not read), events without `session_id` are left out, events without `product_id` are ignored, late events are included. `units` and `revenue` (EGP) are the sum of `quantity` and `line_total` of the lines in `fact_order_lines` by the line's `city`, `product_id` and `date_key`; all placed lines count whatever the order status, cancelled orders included (the same basis as `revenue_placed` of `agg_orders_daily`), so these two columns are placed units and placed revenue, not delivered. A NULL or empty city is stored as `unknown`; lines without `product_id` are ignored. The row set is the union of the event keys and the order-line keys: a missing side is 0 (`0.00` for revenue), not NULL, so a product that was only viewed has `units` 0 and `revenue` 0.00 and a product that was only ordered has `sessions_viewed` 0. Window: one atomic MERGE recomputes every `date_key` of the window, the dates of all events of a session that has an event in the window (a session that crosses midnight, or becomes a bot), the dates of late events (`event_date` before the window, `kafka_timestamp` Cairo date inside it), and the dates of the lines of every order the `fact_order_lines` step recomputed; it updates matching keys, inserts new ones and deletes a key inside the window that has neither events nor lines any more. Limits: a key that vanishes on a widened date outside the window is not deleted; an order whose lines moved to another day leaves the old day stale until a window covers it; a late status row older than the window is not seen (see "Late status rows" above), so its order day stays stale until a covering window or `--full`. Local reference: `streaming/gold/city_product.py`, also part of `streaming.gold.build`.
- Empty slots: `rec_product` (not built yet) and the SQL checks (Task 16) have no SQL file; the build lists them as `empty` and skips them.

The SQL was tested on local Spark with Delta only (`tests/test_gold_build_spark.py`); it has not been run against the real workspace yet.

`publish_silver --reset` truncates the Silver mirror tables the publish owns (never drops them, never `silver_campaign_snapshots`), then uploads and merges again. Without `--yes` it prints the plan and exits 2; `--reset --dry-run` shows the `TRUNCATE` statements and executes nothing. Use it when the mirror holds rows that the local Silver no longer has (insert-only publishing never removes them), then run the Gold build with `--full`.

### Gold build: verified on the real workspace

`python -m warehouse.gold_build --full` ran for real on the Databricks workspace over the Silver mirror of the fresh run (seed 7, 20,000 events, noise on), twice in a row. Rows built: dim_customer 5,001 (5,000 versions plus the -1 row), dim_product 201, dim_campaign 4, dim_date 1,095, fact_orders 153, fact_order_lines 305, agg_orders_daily 27, agg_funnel_daily 41. Compared column by column with the local reference build (`streaming.gold.build`): 0 cell mismatches and no row on only one side in all four tables (surrogate keys excluded, the reference uses -1). Every order and line resolved to a real dimension key (0 NULL, 0 left at -1). Order revenue and line revenue both sum to 250,155.95. The second run changed nothing: identical row counts, key sums and revenue (no identity key was reassigned).

## Grafana (live monitoring)

Grafana (`grafana/grafana-oss:11.3.0`, 512 MB limit) starts with the default `docker compose up -d`. It reads the four Postgres live/health tables through the read-only role `grafana_ro` and is fully provisioned from `monitoring/grafana/`.

Setup (once): put `GRAFANA_DB_PASSWORD` and `GRAFANA_ADMIN_PASSWORD` in `.env` (names in `.env.example`), start Postgres, then create the tables and the role: `docker compose run --rm spark python scripts/apply_live_ddl.py` (the host does not read `.env`; the container gets `PG_DSN` and `GRAFANA_DB_PASSWORD` from it). Without `GRAFANA_DB_PASSWORD` the script creates the tables, prints a red error and exits with code 3, and the datasource health check fails.

- URL: http://localhost:3000. The dashboard "FunnelStream: live pipeline" (uid `fs-live`, refresh 5 s, last 30 minutes, Cairo time) opens as the home page without a login (anonymous Viewer). To edit or manage alerts log in as `admin` with `GRAFANA_ADMIN_PASSWORD`. Provisioned items are read-only in the UI; change the files under `monitoring/grafana/` instead (dashboards reload within 30 s, alert and datasource files after `docker compose restart grafana`).
- Panels: events per minute (all countries) and the last full minute; active sessions, orders placed, product views and add-to-cart per minute (from `live_metrics_minute`, non-late non-bot events); late-event rate (gauge for the last 5 minutes and a 5 minute sliding line, red at 5%); quarantine share (gauge and line, red at 2%) and quarantine rows in the trailing hour (from `pipeline_health_minute`); seconds since the Silver job last reported (alive every 15 s or batch; amber 30 s, red 120 s), last Gold update and last publish (amber 24 h, red 48 h; from `pipeline_heartbeat`); a text panel with the three alerts and what to do.
- Alerts (folder "FunnelStream alerts", group `fs-pipeline`, evaluated every minute): late-event rate above 5% over 5 minutes (needs 20 events, live runs only because history mode is all late by rule); quarantine share above 2% over 5 minutes (needs 20 rows); no events for 2 minutes (no data also fires). All notify the webhook contact point `fs-webhook` (`http://host.docker.internal:9099/alert`).
- Alert sink: in a separate terminal run `python scripts/alert_sink.py` (`--port` to change). It prints one line per alert (time, FIRING or RESOLVED, rule, summary) and appends the raw JSON to `logs/alerts.log` (git-ignored). `GET http://localhost:9099/` returns `ok`.
- Empty panels or "no data" are normal until the Silver job has written to Postgres (the stat panels show "no data" until a heartbeat exists). The "no events" alert fires while nothing is flowing.
- Reset: `docker compose rm -sf grafana; docker volume rm funnelstream_grafanadata` (drops Grafana's own state; the provisioned files are re-read on the next start). To empty the data: `TRUNCATE live_metrics_minute, pipeline_health_minute, pipeline_batch_log, pipeline_heartbeat;` in Postgres.
- Verified on 2026-10-08 against a real Grafana 11.3.0 container and the compose Postgres with sample rows (datasource health OK, every panel query returned rows, the three rules fired and the webhook reached the sink). Not verified with live Silver job output; that depends on the G1 writers.

## What is not built yet

- The Gold build runs the dimensions, facts, attribution and the four summaries (see "Gold build"); the recommendation table, the SQL checks and the Postgres loader are later tasks. The local `streaming.gold.build` output is a reference only.
- Power BI dashboards.
- Postgres and Airflow containers exist in `docker-compose.yml` but are not part of this run; the Postgres loader and batch jobs are not exercised here.

## Stop and clean up

```powershell
docker compose stop
Remove-Item -Recurse -Force data\lake, data\checkpoints, data\count.py, noise_manifest.json -ErrorAction SilentlyContinue
```

`stop` keeps the images and the Kafka container (and its messages). For a fully fresh Kafka use `docker compose down` (add `-v` to also delete the Postgres volume and the Spark jar cache; see step 0).

## Troubleshooting

Problems hit during the smoke runs:

- `ModuleNotFoundError: No module named 'simulator'` when running `python scripts/run_simulator.py` (older checkouts). Fixed: every script in `scripts/` puts the repo root on `sys.path` itself.
- `AnalysisException: data/lake/silver/silver_products is not a valid name for tables/databases` (older checkouts). Fixed: all jobs and writer helpers turn paths into absolute paths, so relative `data/lake` works.
- Bash on Windows (Git Bash) rewrites `/opt/kafka/...` into a Windows path (`exec: "C:/Users/.../kafka-topics.sh": no such file`). Use PowerShell, or set `MSYS_NO_PATHCONV=1`.
- PowerShell prints `NativeCommandError` or red text for Docker progress lines (for example "Container ... Creating"). Docker writes progress to stderr; it is not a failure. Check the exit code (`$LASTEXITCODE`) or the final output.
- `silver_clean` without `--once` never exits; stop with Ctrl+C. Bronze ingest also never exits, hence `timeout 120` (exit code 124 is normal).
- Bronze or Silver counts too high on a rerun: Kafka still holds the earlier messages. Run `docker compose down` (or `down -v`), delete `data\lake` and `data\checkpoints`, then start again from step 1.
- Live demo slower than expected (older checkouts took 6 min for 3000 events at `--rate 50`). Fixed with `--session-scale` and `--max-tail-seconds`; 3000 events at `--rate 50` now take about 66 s.
- Live metrics table empty after the history run. Expected, see step 7.

General:

- Docker memory: (not reproduced in this run) if Spark containers die with exit code 137 or the Docker engine becomes slow, give Docker at least 6 GB (Settings, Resources, Memory) and do not run Bronze and Silver streaming plus the tests at once. Spark uses a 3 GB limit and Kafka 1 GB.
- Port 9092 in use: another Kafka or process holds it. (not reproduced) Find it with `netstat -ano | findstr :9092`, stop it, or stop the old stack with `docker compose down`. The simulator on the host needs `localhost:9092`; Spark inside Docker uses `kafka:19092`.
- Spark first start is slow: the first Spark job downloads Kafka and Delta jars (into the `ivycache` Docker volume), taking a few minutes. Later runs reuse them. The first `docker compose build spark` also pulls Python and Java layers.
- Windows line endings: the repo has a `.gitattributes` that forces LF. If the generated DDL drift test fails after checkout with CRLF files, re-clone with `git config --global core.autocrlf false` set first. (Not reproduced in this run.)
- Java is not needed on the host. Do not install it for this project; Spark runs in the container.
- pytest skips the Spark tests on Windows (reason "Spark needs winutils on Windows; run in Docker"). Run them in Docker with `RUN_SPARK_TESTS=1` as shown above.
