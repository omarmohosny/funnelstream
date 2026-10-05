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

Observed: `246 passed, 45 skipped`. A `PytestUnhandledThreadExceptionWarning` (WinError 10038, from a Kafka client thread) may print at the end; it is harmless.

Everything including Spark tests, in Docker. Takes about 6 to 8 minutes (501 s observed).

```powershell
docker compose build spark
docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests -q
```

Observed: `289 passed, 2 skipped` (about 8 minutes). The 2 skips are the tests that need a live Kafka (`RUN_KAFKA_TESTS=1`) and a Postgres (`PG_DSN`).

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
{'app.events': 19053, 'orders': 751, 'delivery.events': 402} total 20206
```

It also writes `noise_manifest.json` in the current folder (git-ignored). Its `total_messages` is 20206 and `injected` lists how many of each noise type were added. A harmless librdkafka line about "Coordinator load in progress" can appear right after Kafka starts.

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

Expected: 19053, 751, 402, total 20206, equal to the simulator total and `total_messages`.

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
INFO:silver_clean:batch 0: {'silver_events': 18577, 'silver_orders': 151, 'silver_order_status': 1125, 'silver_order_items': 286, 'silver_quarantine': 301}
```

Row counts after this step (same `count.py`, paths `data/lake/silver/<table>`):

| Table | Rows |
|---|---|
| silver_events | 18577 |
| silver_orders | 151 |
| silver_order_status | 1125 |
| silver_order_items | 286 |
| silver_quarantine | 301 |
| silver_products | 200 |
| silver_customers | 5000 |

The quarantine count equals the sum of the manifest counts for the defects the validator can see: malformed_json 73 + missing_field 46 + unknown_type 20 + bad_version 24 + negative_price 9 + future_timestamp 21 + null_product_id 63 + unknown_product_id 44 + cancel_after_ship 1 = 301. By error code: BAD_JSON 73, MISSING_FIELD 109 (missing_field plus null_product_id), UNKNOWN_EVENT_TYPE 20, BAD_VERSION 24, FUTURE_TIMESTAMP 21, UNKNOWN_PRODUCT 44, NEGATIVE_PRICE 9, CANCEL_AFTER_SHIPMENT 1. Also: 20206 - 301 quarantined - 203 duplicates = 19702 = silver_events + silver_order_status. In history mode every row is flagged `is_late`; that is expected.

Running Silver again changes nothing (checked: same counts, and also the same after deleting `data/checkpoints/silver_clean` and replaying).

### 6. Gold reference build

```powershell
docker compose run --rm spark python -m streaming.gold.build --lake data/lake --out data/lake/gold_ref
```

About 25 s. Expected:

```
fact_orders: 151 rows
fact_order_lines: 286 rows
fact_funnel_daily: 53 rows
```

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
# 3. Connectivity including a tiny upload to the volume (verified against a real workspace)
docker compose run --rm spark python scripts/check_databricks.py --upload-test
# 4. Publish (verified for real): dry run first (exports locally, prints the MERGE it would run, uploads nothing)
docker compose run --rm spark python scripts/publish_silver.py --dry-run
docker compose run --rm spark python scripts/publish_silver.py
docker compose run --rm spark python scripts/publish_silver.py --upload-only   # verified: re-uploads data/export and merges
```

Observed on a real workspace after `--events 5000 --seed 7` (local Silver count equals Databricks mirror count for every table): silver_events 4649, silver_orders 39, silver_order_status 291, silver_order_items 72, silver_quarantine 79, silver_products 200, silver_customers 5000, silver_campaign_snapshots 0 (skipped). `sum(line_total)` 51763.44 on both sides, `silver_quarantine.raw_payload` NULL everywhere, `silver_customers` has no name/phone columns. A second full publish and an `--upload-only` run left every count unchanged. `MERGE ... USING (SELECT ... FROM parquet.`/Volumes/...`)` works as written on the SQL warehouse. Gotcha fixed during this run: Spark names Parquet parts with a new UUID per export, so earlier exports piled up in the volume folder and the MERGE source held duplicate rows; the upload now deletes old `*.parquet` files in the table's volume folder first.

`--help` on all three scripts was run. `apply_ddl.py --catalog NAME` overrides the catalog; `publish_silver.py` also takes `--tables`, `--export-only`, `--upload-only`, `--lake`, `--export-dir`. Publishing is insert-only and idempotent (a second run changes nothing); the MERGE source is de-duplicated on the table keys (QUALIFY ROW_NUMBER), and the only update is a narrow repair: `silver_products.name` is filled on an already published row when the mirror holds NULL. `silver_quarantine.raw_payload` is never published (the mirror column stays NULL). Product titles (`silver_products.name`) are published; this is the single exception to the ban on columns called name, phone, email or address. `search_term` and `reason` are free-text columns that are published because the data is synthetic: review them before using real customer data. Verified on a real workspace (seed 7, 5,000 events, two publishes in a row): `silver_events` count 6386 = distinct event_id 6386, `silver_products` 200 rows with 200 non-null names, `silver_customers` 5000 = 5000 distinct keys, `silver_order_items` 127 = 127 distinct keys. `silver_campaign_snapshots` has no local Delta table yet and is skipped with a warning. If `check_databricks` says the catalog is not found, create it in Catalog Explorer or point `DATABRICKS_CATALOG` at an existing one such as `workspace`.

## What is not built yet

- Databricks and the Gold store: Gold build, surrogate keys and the Gold tables in Databricks or Postgres (plan tasks 10b, 10c). The local `streaming.gold.build` output is a reference only.
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
