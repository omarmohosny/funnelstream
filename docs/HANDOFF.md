# Handoff: state of FunnelStream on 13 October 2026, for continuing on another laptop

This file replaces the handoff of 12 October. It is written so that a person, or a new Claude Code session, on a different laptop can pick the work up without the conversation that produced it. Read it first, then `docs/PRE-DEFENSE-CHECKLIST.md`.

The defense was planned for 13 October 2026. The owner has one more day, so the defense moves by one day. The repository, the documentation and the Power BI subtitles still print 13 October; update them once the new date is confirmed (search for `13 October` and `2026-10-13`).

## 1. How to use this file

1. Set up the second laptop with section 3.
2. Copy the files that are not in git (section 4).
3. Check that the platform works with the commands in section 5.
4. Continue with the plan for the extra day (section 7). The working agreements of the owner are in section 8 and apply to every session.
5. If you use Claude Code on the new laptop, start it in the repository folder and paste the prompt in section 11.

## 2. Where the work is

- Repository: `https://github.com/omarmohosny/funnelstream`. All work is on the branch `funnelstream-build`. `main` is behind it (at `627c7a1`); a pull request or a fast-forward merge brings it up to date. No pull request exists yet.
- Last pushed commit when this file was written: `f6015e7` (Grafana dashboard polish), on top of `48c47bf` (smoke test fixes and the scratch Gold reset).
- **Not committed on the first laptop when this file was written:** this file, `README.md` (one added link), `docs/FunnelStream-Project-Documentation.md`, `.html` and `.pdf`, `docs/images/` (figures for the documentation), `scripts/docs_build/` (the scripts that rebuild the documentation) and a line in `.gitignore`. `docs/smoke-test.md` is also untracked; it is the report of the first smoke run and shows three failures that were fixed afterwards, so do not commit it as it is. Delete it, or replace it with the report of a new full run (section 7, item 2). Check `git status` on the first laptop and push before you leave it; without that push the second laptop cannot see this file.

## 3. Setting up the second laptop

Requirements: Windows 11, at least 16 GB of RAM (the project needs about 8 GB for Docker plus 4 GB or more free for the host), about 20 GB of free disk, internet.

1. Install Git, Python 3.11, Docker Desktop (WSL 2 backend), Power BI Desktop and Google Chrome (Chrome is only for screenshots and the documentation PDF). Do not install Java or Spark on the host; they live in the Docker image.
2. Give Docker about 8 GB. Create `%UserProfile%\.wslconfig` with a `[wsl2]` section and `memory=8GB` if the default is lower. Switch Docker Resource Saver off. Run `w32tm /resync /force` in an administrator PowerShell so the clock is right (the first laptop's clock jumped by days on 10 October).
3. Clone and check out the branch:
   ```powershell
   git clone https://github.com/omarmohosny/funnelstream.git
   cd funnelstream
   git checkout funnelstream-build
   ```
4. Create the virtual environment and install the pinned dependencies:
   ```powershell
   python -m venv .venv
   .venv\Scripts\python -m pip install -r requirements.txt
   ```
5. Create `.env` from `.env.example` and fill in the values from section 4. Never commit `.env`.
6. Build the Spark image (the first build takes several minutes) and start the base services:
   ```powershell
   docker compose build spark
   docker compose up -d kafka postgres grafana
   .venv\Scripts\python kafka/create_topics.py
   docker compose run --rm spark python scripts/apply_live_ddl.py
   docker compose run --rm spark python scripts/seed_silver_reference.py --lake data/lake
   ```
   `apply_live_ddl.py` prints a red error and exits with code 3 if `GRAFANA_DB_PASSWORD` is missing from `.env`; the tables still exist but Grafana cannot read them.
7. Run the host tests: `.venv\Scripts\python -m pytest tests -q -p no:cacheprovider`. The expected result is about 1,169 passed and 218 skipped (the Spark tests), 0 failed. `tests/test_checks` needs `noise_manifest.json` in the repository root, which is git-ignored (section 4).
8. Open Grafana at `http://localhost:3000` (anonymous viewers see the dashboards; the user `admin` and `GRAFANA_ADMIN_PASSWORD` log in for alerting).
9. Power BI: open `dashboards/powerbi/FunnelStream.pbip` in Desktop. It asks for a read-only Databricks token and the `grafana_ro` Postgres password. Refresh only after the Gold batch and the two Postgres builders have run (section 5).

## 4. What is not in git, and how to move it

| Item | Why it is not in git | What to do on the new laptop |
|---|---|---|
| `.env` | Secrets | Copy it by USB stick or a password manager. It holds the Postgres and Grafana passwords, the Databricks host, token, warehouse id, catalog and volume, and optionally `ANTHROPIC_API_KEY`. Do not send it by chat or email |
| Databricks access | Token is personal | The same workspace works from any laptop with the host, token, warehouse id and catalog `funnelstream` (real) or `workspace` (scratch). Create a new read-only token for Power BI if the old one expires |
| `noise_manifest.json` (root) | Generated by the simulator, git-ignored | Copy the file from the first laptop, or run the simulator once so it is written |
| `data/` (lake, checkpoints, `_archive`, `smoke`) | Large, regenerated, git-ignored | Not needed. The lake is rebuilt by the demo. `data/_archive` holds screenshots and logs of 12 and 13 October; copy it only if you want them |
| Power BI cache and credentials | Local to Desktop | Re-enter the credentials |
| Claude Code memory and installed skills | Local to the first laptop | Section 8 restates every rule that matters. The Power BI design skills (`pbi-report-design`, `modifying-theme-json`, `pbip`) were installed globally; they are optional |
| The working ledger of the Power BI redesign | `.superpowers/` is git-ignored | Not needed; the spec and plan are in `docs/superpowers/` and the commits record the work |

## 5. Quick health check of the platform

```powershell
docker compose ps                                                                  # kafka, postgres, grafana up
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt"'   # live tables exist
.venv\Scripts\python -m pytest tests -q -p no:cacheprovider                        # host tests
.\scripts\watch_demo.ps1 -Inventory -ExpressPromiseMinutes 2 -LateExpressEvery 15 -SyncEvery 0   # live rehearsal, then open Grafana
.\scripts\stop_demo.ps1
```

Batch lane (needs Databricks settings in `.env`; use the scratch catalog `workspace` for tests, never `funnelstream` without the owner's approval):

```powershell
docker compose run --rm spark python scripts/run_batch.py --full
docker compose run --rm spark python scripts/build_inventory_daily.py --date today --days 2
docker compose run --rm spark python scripts/build_d1_marketing.py
```

## 6. State on the first laptop (it will not follow you)

- Kafka, Postgres and Grafana containers were running. The Postgres live tables and the Kafka topics still hold the data of the 13 October rehearsal, and `data\lake` and `data\checkpoints` still hold its lake. The clean-up that was planned (move the lake and checkpoints aside, recreate Kafka, empty the live tables) was **not** done, because the tool that runs commands in the Claude session could not run it. If you present from the first laptop, run these in a normal PowerShell window in the repository folder, in this order:
  ```powershell
  Move-Item data\lake data\_old_lake_13oct
  Move-Item data\checkpoints data\_old_checkpoints_13oct_b
  docker compose rm -sf kafka
  docker compose up -d kafka
  docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "TRUNCATE live_metrics_minute, pipeline_health_minute, pipeline_batch_log, live_stock_now, live_stock_store_minute, live_oos_demand_minute, live_express_orders; DELETE FROM pipeline_heartbeat WHERE component = ''silver_job'';"'
  ```
  The `TRUNCATE` empties only the live tables of this rehearsal; the dimensions and the Gold heartbeat rows stay. On a fresh second laptop none of this applies, because it starts empty.
- Old lakes and checkpoints of earlier days are in `data\_old_*`. They can be deleted after the defense.

## 7. Plan for the extra day

In order of value. The first three protect the demo; the rest is polish.

1. **Clean dress rehearsal on the machine you will present from (about 2 hours).** Start from an empty state, run the live inventory demo, then the failure demo twice with `NO DUPLICATES` both times (`docs/RUNNING.md`, "Failure demo (G3)"). The presenter types the commands. This also shows whether the hero stock-out comes about 4 minutes in on this machine. `-Events 60000` was suggested to keep traffic on longer and has not been rehearsed.
2. **One uninterrupted green smoke test with the Spark test stage (about 1 hour, mostly waiting).** Close Power BI and browsers first (6 GB free RAM). `scripts/smoke_test.py --dry-run` first, then `--yes`. It recreates Kafka and uses the scratch catalog `workspace`; delete `data\checkpoints` afterwards. The report it writes replaces `docs/smoke-test.md`. Its history: the first full run on 13 October failed three stages for reasons that were then fixed in the script (late events and `live_metrics_minute`, a cast in the Grafana macro expansion, and stale rows in the scratch catalog). The Gold stages passed in a manual rerun (66 checks, 0 failed).
3. **Power BI (about 2 hours).** Confirm in Desktop the three charts changed after the screenshots in the documentation: Operations (oversold units and the "Products with oversold units" table), Customers (average spend by favourite category, no *(Blank)* bar) and Funnel (labels no longer "2K"). Then retake those three screenshots into `docs/images/powerbi/`. Deferred minor items: overlapping labels on the Executive leak chart, CPA 171 on the scorecard against cost per delivered order 377 (they differ by definition, say so on the page), Top-N limits on tables with many rows, `docs/metrics.md` line 123 still lists the removed `Lapsed Buyers (30+ days)`. Do a credential and refresh test on the presenting machine.
4. **Marketing D-1 on real data.** `build_d1_marketing.py` ran only on the scratch catalog. Run it against `funnelstream` after a finished Gold batch, with the owner's approval.
5. **Safety net.** Record a short screen video of every Grafana dashboard and every Power BI page. Keep a zip of the repository and `.env` on a USB stick. Check the Databricks token has not expired.
6. **Merge and tidy.** Update the defense date, update the test counts in `README.md` (1,169 host tests), merge into `main`, tag it. Optional: overlap the generator rounds so live traffic never goes quiet between rounds (a simulator change, only if the rehearsal shows it hurts), and fix the flaky test `test_idle_silver_stream_keeps_reporting_alive`, which failed twice under disk load and then passed four times.
7. **Rehearse the talk.** Questions to prepare: why Delta, what happens on a failure and how duplicates are avoided, what each KPI means, why ROAS delivered is lower than ROAS placed, why exposed spend is an estimate, why the data has one day with 55% of the revenue.

## 8. Working agreements of the owner (apply to every session)

- The owner presents the project and types the demo commands. Do not run the demo for them.
- Commit and push only when the owner says so. Git author is passed per command: `git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit ...`, and the message ends with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`. Commit messages, documents and code comments are plain full sentences. Branch work happens on `funnelstream-build`.
- Destructive actions need explicit approval each time: lake resets, `TRUNCATE`, recreating Kafka, anything that writes to Databricks. Use the scratch catalog `workspace` for tests and refuse the real catalog `funnelstream`. Move data aside instead of deleting it.
- Never commit `.env`, `.venv`, `data/`, `.superpowers/` or any secret. Do not print values from `.env`.
- Do not run `scripts/make_pbip.py` over `dashboards/powerbi/` except deliberately. It replaces the whole report folder. Backups are in `data/_archive/powerbi_before_stage1` and `powerbi_before_stage2` on the first laptop.
- Do not restart the long smoke test or any heavy job on your own; low free memory has killed background jobs before.
- The owner prefers short replies.

## 9. Pitfalls learned

- Running commands through a tool or an automatic classifier can block `TRUNCATE`, Kafka recreation and similar commands. Run those yourself in a normal PowerShell window in the repository folder.
- In Git Bash, a path argument that starts with `/` is rewritten. Prefix such commands with `MSYS_NO_PATHCONV=1` when you call a tool inside a container (for example `kafka-topics.sh`).
- Grafana reads provisioned dashboards from a bind mount that does not always notice changes on Windows. After `scripts/gen_dashboards.py`, run `docker compose restart grafana`.
- Dashboard JSON is generated: change `monitoring/dashboards.py` or `monitoring/live_dashboards.py`, run `.venv\Scripts\python scripts/gen_dashboards.py`, and the tests fail if a file drifts from its builder. `monitoring/grafana/dashboards/pipeline-live.json` is the exception; it is edited by hand.
- A Silver micro-batch can take longer than the 10 second trigger on a laptop; the "batch is falling behind" warning is normal. Keep the Gold sync window off during a demo (`-SyncEvery 0`).
- The generator runs sequential rounds. Each round's stock and express timeline finishes before the next round starts, so traffic is quiet for a few minutes between rounds.
- Express orders and the alert state of an earlier run can keep an alert in the firing state, so a new breach shows no new notification. Empty the live tables before an on-camera demo.
- The Databricks Silver mirror and the Gold build are `MERGE`s and never remove rows. Use `publish_silver.py --reset --yes` and `scripts/reset_scratch_gold.py --catalog workspace --yes` to start a scratch run clean.
- `streamlit run` binds to all network interfaces by default. If you start the assistant's web interface, stop it afterwards or add `--server.address 127.0.0.1`.

## 10. Recipes

**Screenshot a Grafana dashboard with headless Chrome** (anonymous viewers are allowed; use an absolute time range to capture a past run):

```powershell
& "C:\Program Files\Google\Chrome\Application\chrome.exe" --headless=new --disable-gpu --hide-scrollbars --window-size=1900,1450 --virtual-time-budget=25000 --screenshot=out.png "http://localhost:3000/d/fs-express-sla?kiosk&from=now-30m&to=now"
```

Dashboard uids: `fs-live`, `fs-inventory-ops`, `fs-express-sla`, `fs-live-stock`, `fs-inventory-daily`, `fs-marketing-d1`.

**Rebuild the documentation:** see `scripts/docs_build/README.md`. The source is `docs/FunnelStream-Project-Documentation.md`; edit it, then run `make_diagrams.py` (only if a diagram changed) and `build_doc.py`.

**Check every Grafana panel query against the running database:** the smoke test does it inside its isolated database; for a one-off run, expand the macros with `scripts/smoke_test.py:expand_macros` and send each query to `psql`.

## 11. Decisions waiting for the owner

- The new defense date, so the documents can be updated.
- Whether to clean the demo state on the first laptop or present from the second one.
- Whether to merge `funnelstream-build` into `main` before the defense, and whether to open a pull request first.
- Whether to run `build_d1_marketing.py` against the real catalog.
- Whether the simulator rounds should be changed to overlap (section 7, item 6).

## 12. Prompt to start Claude Code on the new laptop

Open a terminal in the repository folder, start Claude Code, and paste:

```text
Read docs/HANDOFF.md first, then docs/PRE-DEFENSE-CHECKLIST.md and docs/FunnelStream-Project-Documentation.md (sections 14 and 15). This is a graduation project, FunnelStream, and the defense is one day away. The first laptop is not available. Follow the working agreements in section 8 of the handoff: I present and type the demo commands, you commit and push only when I say so, destructive actions (truncate, Kafka recreate, lake reset, Databricks writes) need my approval and the real catalog is off limits, never print or commit .env, and keep replies short. Start by running the health check in section 5 and tell me what is missing on this laptop. Then we continue with the plan for the extra day in section 7.
```

## 13. Evidence and where to find it

| What | Where |
|---|---|
| Full documentation with diagrams and screenshots | `docs/FunnelStream-Project-Documentation.pdf` (source `.md`, also `.html`) |
| Run guide with measured timings | `docs/RUNNING.md` |
| Step list before the defense | `docs/PRE-DEFENSE-CHECKLIST.md` |
| Design specs and plans | `docs/superpowers/specs/` and `docs/superpowers/plans/` |
| Metric dictionary | `docs/metrics.md` |
| Data model review and its resolution status | `docs/data-model-review.md` |
| Benchmark (1,000,000 events in 654.73 s) | `docs/benchmark.md` |
| Power BI model guide and measures | `dashboards/powerbi/MODEL.md`, `dashboards/powerbi/measures.dax` |
| Logs and screenshots of 12 and 13 October (first laptop only) | `data/_archive/` |
