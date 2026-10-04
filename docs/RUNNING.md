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

Observed: `231 passed, 43 skipped`. A `PytestUnhandledThreadExceptionWarning` (WinError 10038, from a Kafka client thread) may print at the end; it is harmless.

Everything including Spark tests, in Docker. Takes about 6 minutes (373 s observed).

```powershell
docker compose build spark
docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests -q
```

Observed: `272 passed, 2 skipped`. The 2 skips are the tests that need a live Kafka (`RUN_KAFKA_TESTS=1`) and a Postgres (`PG_DSN`).

## Run the pipeline step by step

Two rules for the Spark steps (see Troubleshooting for why):

1. Run Python modules with `-m` (for example `python -m scripts.run_simulator`), not as `python scripts/run_simulator.py`, from the repo root.
2. For jobs that create Delta tables through the MERGE API (seed, Silver) pass absolute container paths, `/app/data/lake` and `/app/data/checkpoints`. The repo is mounted at `/app` in the container.

### 0. Clean state (only if you ran before)

```powershell
Remove-Item -Recurse -Force data\lake, data\checkpoints, noise_manifest.json -ErrorAction SilentlyContinue
docker compose down
```

`docker compose down` removes the Kafka container and its topics. Without it, old messages stay in Kafka and Bronze will ingest them again, so counts will not match a fresh run.

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
created app.events.dlq
created orders.dlq
created delivery.events.dlq
```

Partitions: `app.events` 6, `orders` 3, `delivery.events` 3, each DLQ topic 1. Check with:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe
```

### 2. Generate data (history mode, noise on)

```powershell
.venv\Scripts\python -m scripts.run_simulator --events 20000 --seed 7 --bootstrap localhost:9092
```

Takes a few seconds. Expected last line:

```
{'app.events': 19053, 'orders': 751, 'delivery.events': 402} total 20206
```

It also writes `noise_manifest.json` in the current folder (git-ignored). Its `total_messages` is 20206 and `injected` lists how many of each noise type were added. A harmless librdkafka line about "Coordinator load in progress" can appear right after Kafka starts.

### 3. Bronze ingest

Bronze is a continuous stream. Run it for a bounded time and let `timeout` stop it:

```powershell
docker compose run --rm spark timeout 120 python -m streaming.jobs.bronze_ingest
```

It ends after 120 s with exit code 124 (expected, that is `timeout`). The first run also downloads Spark jars (extra minute or two). It writes Delta tables under `data/lake/bronze/` and checkpoints under `data/checkpoints/`. The data was consumed in well under 120 s.

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
docker compose run --rm spark python -m scripts.seed_silver_reference --lake /app/data/lake
```

About 15 s. Expected last line: `silver_products 200 rows offered; silver_customers 5000 rows offered`. Silver needs this to recognise valid product ids.

### 5. Silver

```powershell
docker compose run --rm spark python -m streaming.jobs.silver_clean --once --lake /app/data/lake --checkpoints /app/data/checkpoints
```

About 40 s. Expected log line:

```
INFO:silver_clean:batch 0: {'silver_events': 18577, 'silver_orders': 151, 'silver_order_status': 1125, 'silver_order_items': 286, 'silver_quarantine': 301}
```

Row counts after this step (same `count.py`, paths `/app/data/lake/silver/<table>`):

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
docker compose run --rm spark python -m streaming.gold.build --out data/lake/gold_ref
```

About 25 s. Expected:

```
fact_orders: 151 rows
fact_order_lines: 286 rows
fact_funnel_daily: 53 rows
```

This is a local reference build under `data/lake/gold_ref`, not the real Gold store (see "What is not built yet").

### 7. Live metrics

There is no command-line entry point for the refresh; call the function:

```powershell
docker compose run --rm spark python -c "from streaming.common.spark import get_spark; from streaming.gold.live import refresh_live_metrics; print('WROTE', refresh_live_metrics(get_spark('live'), '/app/data/lake', minutes=60))"
docker compose run --rm spark python scripts/show_live.py --lake /app/data/lake
```

With history-mode data this table is empty (it only counts non-late events from the last 60 minutes; history events are 1 to 7 days old and flagged late). `show_live.py` then prints an empty table. Use the live demo below to see rows.

## Live demo

Needs three terminals (Kafka must be up and topics created). Bronze and Silver stream until stopped (Ctrl+C), or bound them with `timeout`.

Terminal 1, Bronze:

```powershell
docker compose run --rm spark python -m streaming.jobs.bronze_ingest
```

Terminal 2, Silver streaming:

```powershell
docker compose run --rm spark python -m streaming.jobs.silver_clean --lake /app/data/lake --checkpoints /app/data/checkpoints
```

Terminal 3, simulator:

```powershell
.venv\Scripts\python -m scripts.run_simulator --live --rate 50 --events 3000 --seed 11 --bootstrap localhost:9092
```

Observed: `{'app.events': 2999, 'orders': 116, 'delivery.events': 70} total 3185`. It took 6 min 22 s, not the roughly 1 minute `--rate 50` suggests: the real rate was about 8 events per second. Use a smaller `--events` (for example 1000) for a shorter demo. Silver processed micro-batches every 10 s while it ran (about 200 to 370 events per batch). If you bound Bronze and Silver with `timeout`, make it longer than the simulator run, otherwise finish with one more Bronze run and `silver_clean --once`.

Then refresh and show:

```powershell
docker compose run --rm spark python -c "from streaming.common.spark import get_spark; from streaming.gold.live import refresh_live_metrics; print('WROTE', refresh_live_metrics(get_spark('live'), '/app/data/lake', minutes=60))"
docker compose run --rm spark python scripts/show_live.py --lake /app/data/lake --n 5
```

Observed output (columns minute_start, country, events, active_sessions, product_views, add_to_cart, orders_placed), newest first:

```
2026-10-04 20:01:00 EG    4   1   0   1  1
2026-10-04 20:00:00 EG   44  20   7   9  6
2026-10-04 19:59:00 EG  283  86  72  31  8
```

After this run Bronze held 23391 rows (20206 + 3185, equal to the messages published) and Silver 21510 events, 174 orders, 1309 order_status, 335 order_items, 337 quarantine.

## What is not built yet

- Databricks and the Gold store: Gold build, surrogate keys and the Gold tables in Databricks or Postgres (plan tasks 10b, 10c). The local `streaming.gold.build` output is a reference only.
- Power BI dashboards.
- Postgres and Airflow containers exist in `docker-compose.yml` but are not part of this run; the Postgres loader and batch jobs are not exercised here.

## Stop and clean up

```powershell
docker compose stop
Remove-Item -Recurse -Force data\lake, data\checkpoints, data\count.py, noise_manifest.json -ErrorAction SilentlyContinue
```

`stop` keeps the images and the Kafka container (and its messages). For a fully fresh Kafka use `docker compose down`.

## Troubleshooting

Problems hit during the smoke run:

- `ModuleNotFoundError: No module named 'simulator'` when running `.venv\Scripts\python scripts/run_simulator.py ...`. The script is not run as a module so the repo root is not on the path. Use `.venv\Scripts\python -m scripts.run_simulator ...` from the repo root (or set `$env:PYTHONPATH = "."`). The Spark container sets PYTHONPATH itself, which is why `python scripts/show_live.py` works there.
- `AnalysisException: data/lake/silver/silver_products is not a valid name for tables/databases` from `scripts.seed_silver_reference` (and the same code path in Silver). Diagnosis: Delta's table builder treats the relative path as a table name. Pass absolute container paths: `--lake /app/data/lake` (and `--checkpoints /app/data/checkpoints` for Silver). Not tried: Silver with relative paths, but it uses the same writer.
- Bash on Windows (Git Bash) rewrites `/opt/kafka/...` into a Windows path (`exec: "C:/Users/.../kafka-topics.sh": no such file`). Use PowerShell, or set `MSYS_NO_PATHCONV=1`.
- PowerShell prints `NativeCommandError` or red text for Docker progress lines (for example "Container ... Creating"). Docker writes progress to stderr; it is not a failure. Check the exit code (`$LASTEXITCODE`) or the final output.
- `silver_clean` without `--once` never exits; stop with Ctrl+C. Bronze ingest also never exits, hence `timeout 120` (exit code 124 is normal).
- Bronze or Silver counts too high on a rerun: Kafka still holds the earlier messages. Run `docker compose down`, delete `data\lake` and `data\checkpoints`, then start again from step 1.
- Live demo simulator much slower than `--rate` suggests (6 min for 3000 events at `--rate 50`). Use fewer events.
- Live metrics table empty after the history run. Expected, see step 7.

General:

- Docker memory: (not reproduced in this run) if Spark containers die with exit code 137 or the Docker engine becomes slow, give Docker at least 6 GB (Settings, Resources, Memory) and do not run Bronze and Silver streaming plus the tests at once. Spark uses a 3 GB limit and Kafka 1 GB.
- Port 9092 in use: another Kafka or process holds it. (not reproduced) Find it with `netstat -ano | findstr :9092`, stop it, or stop the old stack with `docker compose down`. The simulator on the host needs `localhost:9092`; Spark inside Docker uses `kafka:19092`.
- Spark first start is slow: the first Spark job downloads Kafka and Delta jars (into the `ivycache` Docker volume), taking a few minutes. Later runs reuse them. The first `docker compose build spark` also pulls Python and Java layers.
- Windows line endings: the repo has a `.gitattributes` that forces LF. If the generated DDL drift test fails after checkout with CRLF files, re-clone with `git config --global core.autocrlf false` set first. (Not reproduced in this run.)
- Java is not needed on the host. Do not install it for this project; Spark runs in the container.
- pytest skips the Spark tests on Windows (reason "Spark needs winutils on Windows; run in Docker"). Run them in Docker with `RUN_SPARK_TESTS=1` as shown above.
