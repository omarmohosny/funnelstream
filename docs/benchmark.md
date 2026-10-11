# Benchmark

Date: 2026-10-11

Command: `python scripts/benchmark.py --events 400000 --yes`

Git commit: 29239cb

Machine: 12 CPUs, Docker memory 7.7 GB

| stage | seconds | rows | rows per second |
|---|---|---|---|
| reset | 5.20 | - | - |
| simulate | 9.72 | 409,133 | 42,096.20 |
| bronze | 21.25 | 409,133 | 19,253.32 |
| seed | 16.56 | - | - |
| silver | 58.62 | 409,133 | 6,978.81 |
| ddl | 33.92 | - | - |
| publish | 119.75 | 419,667 | 3,504.53 |
| gold | 123.62 | - | - |

Total: 388.64 seconds for 400,000 simulated events (409,133 Kafka messages), including the Kafka reset.

Synthetic data on one laptop, Docker limited to about 8 GB. Numbers are a measurement of this setup, not a capacity claim.

The Databricks stages ran in the scratch catalog `workspace` on a Databricks Free Edition SQL warehouse.
