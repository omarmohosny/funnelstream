# Benchmark

Date: 2026-10-22

Command: `python scripts/benchmark.py --events 2000000 --skip-gold --yes --report docs/benchmark-2m-local.md`

Git commit: 55b3d7a

Machine: 12 CPUs, Docker memory 7.7 GB

| stage | seconds | rows | rows per second |
|---|---|---|---|
| reset | 5.14 | - | - |
| simulate | 52.14 | 2,042,134 | 39,165.61 |
| bronze | 30.36 | 2,042,134 | 67,266.18 |
| seed | 15.17 | - | - |
| silver | 147.00 | 2,042,134 | 13,892.07 |

Total: 249.81 seconds for 2,000,000 simulated events (2,042,134 Kafka messages), including the Kafka reset.

Synthetic data on one laptop, Docker limited to about 8 GB. Numbers are a measurement of this setup, not a capacity claim.
