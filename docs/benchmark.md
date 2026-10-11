# Benchmark

Date: 2026-10-12

Command: `python scripts/benchmark.py --events 1000000 --yes`

Git commit: 8951114

Machine: 12 CPUs, Docker memory 7.7 GB

| stage | seconds | rows | rows per second |
|---|---|---|---|
| reset | 5.16 | - | - |
| simulate | 26.19 | 1,022,333 | 39,039.71 |
| bronze | 24.50 | 1,022,333 | 41,727.88 |
| seed | 14.30 | - | - |
| silver | 82.48 | 1,022,333 | 12,394.32 |
| ddl | 66.11 | - | - |
| publish | 272.66 | 1,040,576 | 3,816.44 |
| gold | 163.33 | - | - |

Total: 654.73 seconds for 1,000,000 simulated events (1,022,333 Kafka messages), including the Kafka reset.

Synthetic data on one laptop, Docker limited to about 8 GB. Numbers are a measurement of this setup, not a capacity claim.

The Databricks stages ran in the scratch catalog `workspace` on a Databricks Free Edition SQL warehouse.
