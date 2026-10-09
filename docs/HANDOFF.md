# Handoff: state of FunnelStream on 10 October 2026

Defense is on 13 October 2026. Branch `funnelstream-build` holds all work; `main` is still at the original spec commit `dd44fbd`. The working tree was clean at commit `55887fd` plus this file.

## What is done

- Pipeline: simulator, Kafka, Bronze and Silver Delta (local), `publish_silver` to the Databricks Silver mirror, Gold build (SQL MERGE steps), live tables in Postgres read by Grafana.
- Gold steps built: `dim_customer`, `dim_product`, `dim_campaign`, `dim_date`, `fact_orders`, `fact_order_lines`, `fact_attribution` (T13), `agg_orders_daily`, `agg_funnel_daily`, `agg_campaign_daily` (T12), `agg_city_product_daily` (T14). `rec_product` is the only empty slot and is cut first.
- One command for the batch (T15): `docker compose run --rm spark python scripts/run_batch.py`. It runs publish, Gold build and the checks, and prints `BATCH OK` or `BATCH FAILED at <phase>`.
- Checks (T16): 63 checks, 0 failed on the last full run (`--skip-publish --full`, `BATCH OK`). Checks `recon.orders_count` and `recon.lines_count` compare whole tables, so run `--full` once after new data older than the 7-day window arrives. A default-window run failed this check by 72 orders until a `--full` run.
- Ad report: the Airflow DAG `ads_yesterday_report` now works. It always writes to Databricks (`get_store("databricks")` in the DAG and in `batch/ads_report.py`, and `GOLD_BACKEND: databricks` on the `airflow` service), whatever `.env` says. Verified for real: `silver_campaign_snapshots` has rows for 2026-10-08 and 2026-10-05, and `agg_campaign_daily` carries the same spend and clicks with correct CTR, CPA and ROAS.
- Failure demo tooling (G3): `scripts/failure_demo.ps1` (`status`, `kill`, `restart`, `verify`) and `scripts/check_duplicates.py`. `kill` refuses with exit 3 when the events line is already stale; `-Force` overrides.
- Sprint sheet v3: T12 to T16 are Done, G3 is In progress. The workbook is set to recalculate when opened.

## What is not done

1. **G3 live demo has not passed twice in a row.** The code is verified. The runs so far failed because the alert was already firing (the pipeline lagged at 50 events per second, or no generator was running). The procedure below is the one to follow.
2. T17: samples, a 1 to 2 million event benchmark, README (Member 5).
3. T18: Power BI model and pages, Campaign and Funnel first (Member 1). Use `agg_campaign_daily` with `attribution_model = 'last_touch'`.
4. DOC: refresh the PDF, deck and README screenshots (Member 5).
5. T19 customer 360 is a stretch task. `rec_product` is cut first.
6. The old duplicates in the local lake (silver_events 20, silver_orders 23, silver_order_status 146, silver_order_items 0) come from reused simulator seeds. They are not caused by a crash. The demo proof is that these counts stay the same after the kill and restart. Cleaning them needs a lake reset, which needs the user's explicit approval. A `verify` mode that saves a baseline and prints UNCHANGED or NEW DUPLICATES was offered and not built.
7. Open the pull request from `funnelstream-build` into `main`. The user has not agreed to it yet.
8. Two host tests fail when the repo's `noise_manifest.json` has been rewritten by a simulator run: `test_expected_quarantine_is_the_sum_of_the_nine_kinds_and_ignores_the_rest` and `test_the_repo_manifest_loads` expect `duplicate=25`. The file is not tracked by git. Restore it to 25 or change the tests to use a fixed fixture.

## How to run the G3 demo

All steps start in the project folder. Allow scripts in each PowerShell window with `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`, or prefix calls with `powershell -ExecutionPolicy Bypass -File`.

1. `docker compose up -d postgres kafka grafana`, wait until Postgres is healthy, then `.venv\Scripts\python scripts\apply_live_ddl.py`.
2. Window 2: `.venv\Scripts\python scripts\alert_sink.py`. Start it before the demo.
3. Window 1: `.\scripts\watch_demo.ps1 -Rate 10 -SyncEvery 0`. Use a low rate and no Gold sync window, because the pipeline falls behind at 50 events per second. Stop the Airflow container first if memory is short: `docker compose --profile airflow stop airflow`.
4. Window 3: `.\scripts\failure_demo.ps1 status` every 30 seconds. Continue when the heartbeat is under 30 seconds, the newest minute is under about 90 seconds ago, and the sink printed RESOLVED.
5. `verify` for the baseline, then `kill`. FIRING should appear in the sink within 1 to 3 minutes. Then `restart`, wait for RESOLVED (2 to 4 minutes after events return), then `verify` again. The duplicate counts must equal the baseline.
6. Repeat step 5 once. When both runs pass, set G3 to Done in the sprint sheet and commit.

The alert only notifies on a state change, so it must be Normal before the kill. A stale `status` line means it is already firing, and the `kill` guard says so.

## Useful facts

- Seeds: every simulator run needs a new `--seed`. `watch_demo.ps1` takes the next one from `data/state/next_seed.txt` (36 at the end of this session). `--universe-seed 7` fixes customers and products.
- Docker has about 8 GB. The Airflow container needs 2 GB and reinstalls its pip packages on every start, so wait about 5 minutes after `up`. Its admin password is in `/opt/airflow/standalone_admin_password.txt` inside the container.
- The `skipping silver_campaign_snapshots: no local Delta table` line in the publish output is normal. The DAG writes that table in Databricks directly.
- Host tests: 729 passed, 164 skipped, 2 failed (the manifest tests above). Spark tests skip on the host and run in Docker per file with `RUN_SPARK_TESTS=1`.
- Never commit `data/`, `.venv` or secrets. Destructive Databricks actions and lake resets need the user's explicit approval.

## Commits of this session

`b11999b` sprint note, `260d960` demo runs until Ctrl+C plus Gold sync window, `903ad39` T12, `88d6551` T14, `c7fe3a6` T15, `6f74c3c` T16, `6ee3389` G3, `c0f4019` kill guard, `60623f6` ad report pinned to Databricks, `55887fd` sprint sheet statuses.
