# Pre-defense checklist (owner only)

Everything here needs Docker, the owner's accounts or the owner's decision. None of it was run by the automated work of 12 October 2026; the expected results below are derived from the code and the test files, not measured. Record the real numbers in `docs/RUNNING.md` as you go. The defense is on 13 October 2026.

Stop at the first step that fails and read the matching section of [RUNNING.md](RUNNING.md). Do not skip steps 0 and 1: three earlier sessions lost their results to a Docker engine restart or a wrong clock.

## 0. Machine

1. Close browsers and other heavy programs. At least 6 GB of RAM must be free for one Spark container, and 4 GB is the absolute minimum. Check Docker Desktop, Settings, Resources: memory limit 8 GB or more, and Resource Saver off.
2. Administrator PowerShell: `w32tm /resync /force`, then `w32tm /query /status`. It must show `Leap Indicator: 0` and a real source such as `time.google.com`.
3. `docker info --format '{{.SystemTime}}'` and `(Get-Date).ToUniversalTime()` must agree to the second. Turn off sleep for the whole session.
4. From the repo root: `docker version`, `docker compose config` (no error).

## 1. Start the stack (once, before any rehearsal)

`docker-compose.yml` now publishes Kafka, Postgres, Airflow and Grafana on `127.0.0.1` only. The next `docker compose up -d` therefore recreates those containers, and a recreated Kafka container loses its messages. Do this before the first rehearsal, never in the middle of one.

```powershell
docker compose up -d kafka postgres grafana
docker compose up -d --force-recreate grafana
docker compose restart kafka
.venv\Scripts\python kafka/create_topics.py
```

Expected output of `create_topics.py` on a fresh broker: `created app.events`, `created orders`, `created delivery.events`, `created inventory.events`. On an old broker the existing topics print `skip <topic> ...`; that is fine as long as `inventory.events` exists (three partitions).

`docker compose build spark` once, if the image is older than the last change to `requirements.txt` (duckdb, anthropic and streamlit were pinned).

## 2. Host tests

```powershell
.venv\Scripts\python -m pytest tests -q -p no:cacheprovider
```

Expected: `0 failed`. The last run of the automated work printed the line recorded in [HANDOFF.md](HANDOFF.md). The skipped tests are the Spark tests below plus a few that need Kafka or Postgres.

## 3. Spark tests, one container at a time

One file per command, and wait for each container to exit before the next. The count is the number of Spark tests in the file (the tests the host skips); every one must pass.

```powershell
docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests/<file> -q -p no:cacheprovider
```

| Order | File | Spark tests |
|---|---|---|
| 1 | `tests/test_inventory_silver_spark.py` | 3 |
| 2 | `tests/test_silver_clean_spark.py` | 16 |
| 3 | `tests/test_silver_parallel_spark.py` | 4 |
| 4 | `tests/test_g1_live_spark.py` (also `-k ensure_bronze` for the Bronze table) | 10 |
| 5 | `tests/test_live_sink_parallel_spark.py` | 4 |
| 6 | `tests/test_live_stock_spark.py` | 8 |
| 7 | `tests/test_live_express_spark.py` | 6 |
| 8 | `tests/test_inventory_daily_spark.py` | 15 |
| 9 | `tests/test_express_silver_spark.py` | 4 |
| 10 | `tests/test_bronze_ensure_spark.py` | 2 |
| 11 | `tests/test_bronze_once_spark.py` | 1 |
| 12 | `tests/test_gold_reference.py` (includes the new anonymous-order test) | 47 |
| 13 | `tests/test_gold_build_spark.py` (includes the new anonymous-order and `is_active` tests) | 25 |
| 14 | `tests/test_agg_campaign_spark.py` | 8 |
| 15 | `tests/test_agg_city_product_spark.py` | 12 |
| 16 | `tests/test_attribution_spark.py` | 12 |
| 17 | `tests/test_checks_spark.py` | 19 |
| 18 | `tests/test_customer_360_spark.py` | 5 |
| 19 | `tests/test_publish_spark.py` | 6 |
| 20 | `tests/test_check_duplicates_spark.py` | 4 |
| 21 | `tests/test_relative_paths_spark.py` | 2 |
| 22 | `tests/test_show_spark.py` | 2 |

Files 1 to 11 are the inventory work and have never run. Files 12 and 13 contain three tests written on 12 October that have also never run: if one fails, the failure is most likely in the test (a fixture detail), so read it before changing the SQL. Files 14 to 22 were run in earlier sessions (the logs are in `data/_archive/`) but before the changes of 12 October.

The first three failures of the Docker WSL engine happened while a Spark container started with low free RAM. If the engine drops, run `docker compose up -d`, `docker compose up -d --force-recreate grafana`, `docker compose restart kafka` and `.\scripts\failure_demo.ps1 restart`, then continue with the next file.

## 4. Live tables

```powershell
docker compose run --rm spark python scripts/apply_live_ddl.py
```

Expected: 16 live tables created or already present, the eight stores and their districts loaded, and `dim_product_current` filled when `silver_products` exists. It needs `GRAFANA_DB_PASSWORD` in `.env`; without it the tables are created, the read-only role is skipped and the exit code is 3. Then open http://localhost:3000 (user `admin`) and check that the dashboards "FunnelStream: live pipeline", "Operations, live inventory", "Inventory insights, daily", "Marketing, D-1", "Express delivery SLA" and "Live stock monitor" load without a red panel (pick stores in the Store filter), and that Alerting shows 8 rules.

## 5. Inventory rehearsal

Terminal 1: `python scripts\alert_sink.py` (it listens on 0.0.0.0:9099 so that the Grafana container can reach it).

```powershell
.\scripts\watch_demo.ps1 -Rate 10 -SyncEvery 0 -Inventory
```

Watch the timeline in [RUNNING.md](RUNNING.md), "Live inventory demo": opening snapshots at once, a hero product out of stock after roughly one to two minutes, the alert "Demand on out-of-stock products" about two to three minutes later, a `received` movement 90 seconds after the stock-out. Write the real times into RUNNING.md. Then stop it and repeat with the express alert:

```powershell
.\scripts\stop_demo.ps1
.\scripts\watch_demo.ps1 -Rate 10 -SyncEvery 0 -Inventory -ExpressPromiseMinutes 2 -LateExpressEvery 15
```

`fs-express-sla` should fire in about three minutes. After the run, build the day-after marts:

```powershell
docker compose run --rm spark python scripts/build_inventory_daily.py --date today --days 2
docker compose run --rm spark python scripts/build_d1_marketing.py --date <the Cairo date of the data>
```

The second command needs the Databricks settings and a finished Gold batch.

## 6. Failure demo, twice in a row

Follow "Failure demo (G3)" in RUNNING.md. In short, with the stack from step 5 running at `-Rate 20 -SyncEvery 0`:

```powershell
python scripts\alert_sink.py
.\scripts\failure_demo.ps1 status       # heartbeat under 30 s
.\scripts\failure_demo.ps1 verify       # baseline, about 1 minute
.\scripts\failure_demo.ps1 kill
.\scripts\failure_demo.ps1 status       # repeat every 30 s
.\scripts\failure_demo.ps1 restart
.\scripts\failure_demo.ps1 status
.\scripts\failure_demo.ps1 verify       # expected: NO DUPLICATES for every Silver table
```

Run the whole sequence a second time. Wait until the sink prints RESOLVED before the second kill. The demo counts as passed only when both runs end in `NO DUPLICATES`. If the lake holds old duplicates, start from an empty lake (RUNNING.md, step 0).

## 7. Smoke test and benchmark (they recreate Kafka)

Both recreate the Kafka container, so every message is lost and the demo's Bronze checkpoints become invalid. Run them only when the rehearsals above are done.

```powershell
.venv\Scripts\python scripts\smoke_test.py --dry-run     # read the plan first
.venv\Scripts\python scripts\smoke_test.py --yes         # writes docs\smoke-test.md
Remove-Item -Recurse -Force data\checkpoints             # always, after the smoke test
```

The benchmark is done (1,000,000 events in 654.73 s, `docs/benchmark.md`); do not repeat it before the defense.

## 8. Power BI

1. Open `dashboards\powerbi\FunnelStream.pbip` in Power BI Desktop. When it asks for credentials: a read-only Databricks token for the Gold tables and the `grafana_ro` password for the Postgres marts. Never save either into the project.
2. Refresh after the Gold batch and the two Postgres builders (RUNNING.md, "One command"). Check the Cart Abandon Rate and Checkout Abandon Rate cards on the Funnel page: they must be blank, not 100 percent, for an empty slice.
3. Do not run `scripts\make_pbip.py`: it overwrites the report that Desktop saved.
4. `scripts\validate_pbip.py` needs the internet and takes about 20 minutes. Result of 12 October: 128 files, 77 `ok`, 0 `INVALID`, 47 `CANNOT CHECK`, exit code 1 (the script counts every file it could not check as a problem). Of the 47, 28 are `HTTP Error 404` and every one of those visuals has `$schema` `visualContainer/2.13.0`, the version Power BI Desktop wrote when it saved the project and which Microsoft does not serve at that address; 17 are network time-outs; 2 are references inside a downloaded schema that could not be resolved. So the 404s are failures to download a schema, not violations of one, and no file was found invalid. Re-run it on a good connection and read only lines that start with `INVALID`.

## 9. Pull request into `main` (prepared, not run)

`main` is at `dd44fbd` and `funnelstream-build` is 96 commits ahead of it, so the pull request can be merged without conflicts. The changes of 12 October are not committed yet. Commit them first, with the author passed per command:

```powershell
git status
git add -A                     # check first that data/, .env and .venv are not listed; they are git-ignored
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Resolve the data model review, harden the assistant, update the documents"
git push origin funnelstream-build
gh pr create --base main --head funnelstream-build --title "FunnelStream: pipeline, Gold model, inventory, Power BI" --body "State, tests and limitations: docs/HANDOFF.md and docs/PRE-DEFENSE-CHECKLIST.md."
```

Write your own description if you prefer. Review the diff on GitHub once, then merge with a merge commit (not squash, so the 96 commits stay readable) and keep the branch. Do not force-push `main`. Never commit `data/`, `.venv`, `.env`, `.superpowers/` or secrets.

## 10. Day of the defense

1. Repeat step 0, then `docker compose up -d`, `docker compose restart kafka` and `.\scripts\failure_demo.ps1 restart` if the machine was restarted.
2. Open Grafana, Power BI and `docs/FunnelStream-Documentation.pdf`. Keep the recorded failure-demo run and the Spark replay tests as the fallback if the live demo fails.
3. Known limitations to say aloud (each has its one-line consequence in `docs/data-model-review.md`, "Resolution status"): the out-of-stock flag is the stock when the minute was written and is not replay-safe; no stock reversal on cancellation; express KPIs only in the Postgres marts; Store is conformed across the Postgres marts only; ad spend is a UTC day and orders a Cairo day, at most 3 hours apart; exposed spend is an estimate.
