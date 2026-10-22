# Smoke test

Date: 2026-10-22

Command: `python scripts/smoke_test.py --yes --skip-spark-tests`

Git commit: 97166bf

Environment: Windows-10-10.0.26300-SP0; Python 3.11.9; Docker memory 7.7 GB; host memory 6.2 GB free of 15.9 GB; clock offset to Docker 0.02 s; NTP Last Successful Sync Time: unspecified; Source: Local CMOS Clock

| # | stage | status | evidence | seconds |
|---|---|---|---|---|
| 1 | Preflight | pass | Docker: engine answers, Docker memory 7.7 GB, clock offset 0.02 s; NTP: Last Successful Sync Time: unspecified; Source: Local CMOS Clock; Host memory: 6.2 GB free of 15.9 GB; Demo containers: no fs- container running; Kafka handshake: 1 broker(s), 4 topic(s); Grafana: health 200, data source fs-postgres OK; Postgres: answers | 6.7 |
| 2 | Host tests | pass | 1179 passed, 0 failed, 0 errors, 218 skipped | 71.9 |
| 3 | Spark tests | skipped | skipped: --skip-spark-tests | 0.0 |
| 4 | Static checks | pass | gen_ddl --check ok; 6 dashboards valid; 5 builders equal their files; 8 alert rules with condition C and a runbook | 0.4 |
| 5 | Reset and generate | pass | seed 706164; app.events=19721, orders=705, delivery.events=351, inventory.events=1930; total 22707 | 9.9 |
| 6 | Bronze and Silver | pass | Bronze 22707 rows = simulator total; Silver: events=19257, orders=157, order_status=1049, order_items=301, quarantine=299, products=200, customers=5000, stock_movements=204, stock_snapshots=1726, express_orders=108 | 100.3 |
| 7 | Data invariants | pass | no duplicate key in any Silver table; live_express_orders filled from Silver: 108 orders; quarantine 299 (manifest 299); stock: 1552 pairs, replay never below zero, live rule = replay; express: 108 orders, slowest 17.2 min against a promise of 20; live tables populated, heartbeat silver_job present | 53.1 |
| 8 | Daily marts | pass | inventory_daily: 1552 rows; oos_demand_daily: 1807 rows; express_daily: 8 rows; closing stock of 2026-10-16 = live_stock_now (549521) | 26.4 |
| 9 | Dashboards run | pass | 64 panel queries ran without an error (read-only role) | 131.7 |
| 10 | Alert logic | pass | clean: fs-express-ontime=100 not firing; fs-express-sla=0 not firing; fs-late-rate=- not firing; fs-no-events=0 firing; fs-oos-demand=0 not firing; fs-quarantine-share=- not firing; fs-silver-silent=0 firing; fs-stock-outs=0 not firing / breach run: fs-express-ontime=0 firing; fs-express-sla=40 firing; fs-late-rate=100 firing; fs-no-events=3731 not firing; fs-oos-demand=0 not firing; fs-quarantine-share=0 not firing; fs-silver-silent=1 not firing; fs-stock-outs=0 not firing / demand rule on controlled rows: fires with 5 add-to-carts in two minutes (value 1); quiet with 4 | 130.9 |
| 11 | Replay safety | pass | replay of Bronze: 10 Silver tables unchanged, no duplicate key | 95.4 |
| 12 | Databricks batch | pass | BATCH OK in catalog workspace, checks: 66 run, 0 failed | 228.0 |
| 13 | Marketing D-1 mart | pass | marketing_d1_campaign: 3 rows; marketing_d1_product: 0 rows; no product rows: no demand was flagged out of stock at write time on 2026-10-16 (history runs flag against the final stock) | 7.5 |
| 14 | Power BI source check | pass | 11 Gold and 7 Postgres source tables exist; 79 column references of 68 measures resolve | 2.1 |

1 stage skipped; a skipped stage proves nothing.

SMOKE TEST PASSED
