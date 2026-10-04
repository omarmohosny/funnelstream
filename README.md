# FunnelStream

Real-time e-commerce and campaign analytics for an Egyptian online store: Kafka, Spark Structured Streaming, Delta Lake (Bronze, Silver, Gold), Postgres, Airflow and Power BI.

Defense: 13 October 2026. Design: `docs/superpowers/specs/2026-10-04-funnelstream-design.md`. Metrics: `docs/metrics.md`. Build plan: `docs/superpowers/plans/2026-10-04-funnelstream.md`. Sprint sheet: `docs/FunnelStream-Sprint-Plan.xlsx`.

## Repository layout

| Folder | Purpose | Plan task |
|---|---|---|
| `contracts/` | Event schemas, topic config, validator | 2 |
| `simulator/` | Customer, order and delivery event generators, publisher | 3, 4, 6 |
| `mock_apis/` | Mock Meta and Google Ads API | 5 |
| `kafka/` | Topic creation | 6 |
| `streaming/` | Spark jobs: Bronze ingest, Silver clean, Gold stream tables | 7, 8, 9 |
| `batch/` | SCD2, campaigns, attribution, recommendations, batch runner | 11-15, 19 |
| `airflow/dags/` | Airflow DAG (calls `batch/run_batch.py`) | 15 |
| `warehouse/` | Postgres DDL, loader, data-quality checks | 10, 16 |
| `dashboards/` | Power BI file, measures, screenshots | 18 |
| `scripts/` | Run simulator, benchmark, failure demo, sample export | 6, 17 |
| `samples/` | Sample CSVs (10,000 rows per table) | 17 |
| `tests/` | pytest tests | all |
| `docs/` | Spec, plan, metrics, PDF, sprint sheet | n/a |

Folders appear as their tasks are built.

## Getting started

```bash
cp .env.example .env            # set your own passwords
python -m venv .venv
.venv/Scripts/activate          # Windows
pip install -r requirements.txt
pytest
```

Spark tasks (7 onward) need Python 3.11 and Java 17. See the comment at the top of `requirements.txt`.

## Working together

- Branch from `main`, one branch per task (`task-N-short-name`), open a pull request, get one review.
- Do not commit `.env`, `.venv/`, or data under `data/`.
- Each Gold table has one writer. Spark owns the order and funnel tables. Airflow owns the rest. See the spec, section 3.
