# Benchmark, 2,000,000 events

Date: 2026-10-22

Git commit: 55b3d7a (the benchmark script also gained a `--start` argument that day, see below)

Machine: 12 CPUs, Docker memory 7.7 GB. Databricks stages ran in the scratch catalog `workspace` on a Databricks Free Edition serverless 2X-Small SQL warehouse.

The local stages and the Databricks stages were run as separate commands on the same data (the Databricks workspace had been deactivated when the first full run reached it), so this is one pipeline measured in two parts, not one continuous run. The `ddl` and `publish` seconds are taken from file times and are accurate to about one second; the others come from the benchmark report and the batch output.

| stage | seconds | rows | rows per second | 1M run of 12 Oct (seconds) |
|---|---|---|---|---|
| reset | 5.14 | - | - | 5.16 |
| simulate | 52.14 | 2,042,134 | 39,165.61 | 26.19 |
| bronze | 30.36 | 2,042,134 | 67,266.18 | 24.50 |
| seed | 15.17 | - | - | 14.30 |
| silver | 147.00 | 2,042,134 | 13,892.07 | 82.48 |
| ddl | about 56 | - | - | 66.11 |
| publish | about 288 | 2,073,748 | about 7,200 | 272.66 |
| gold (build 92.0 + checks 36.3) | 128.6 | - | - | 163.33 |

Total: about 724 seconds for 2,000,000 simulated events (2,042,134 Kafka messages), including the Kafka reset. The 1M run took 654.73 seconds.

Silver result: 1,886,073 events, 15,534 orders, 105,392 order status rows, 30,918 order items, 30,631 quarantined rows (injected noise). The published rows equal the Silver rows in every table.

Gold result: 66 data quality checks, 0 failed (`BATCH OK`). The check that compares the injected noise with the quarantined rows matched, 30,631 expected and 30,631 observed.

Notes:

- The simulator refuses event times in the future and runs at about one week of history per million events. `scripts/benchmark.py` now passes `--start` going back one week per million events plus three days; before, a 2M run failed at the simulate stage.
- The Gold build only adds and updates rows. A second run on the same scratch catalog fails the reconciliation checks until `scripts/reset_scratch_gold.py` and `scripts/apply_ddl.py` have run; this run did that first.
- Synthetic data on one laptop, Docker limited to about 8 GB. Numbers are a measurement of this setup, not a capacity claim.
