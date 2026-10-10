# Live inventory for operations and D-1 marketing: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add simulated dark-store inventory to FunnelStream so one platform serves two users: an operations user who watches live traffic and real stock in Grafana, and a marketing user who reads yesterday's campaign numbers including the estimated ad spend exposed to out-of-stock products.

**Architecture:** Two new event types (`stock_snapshot`, `stock_movement`) travel on a new Kafka topic `inventory.events` through the existing Bronze and Silver path (two new Silver tables). The live sink recomputes `live_stock_now` and `live_oos_demand_minute` in Postgres for the touched store and product pairs. Grafana reads those tables for an operations dashboard and one new alert. A daily job joins the Databricks `agg_campaign_daily` spend with the out-of-stock landing clicks into a Postgres mart that a second Grafana dashboard shows.

**Tech Stack:** Python 3.11, PySpark 3.5 with Delta (in Docker), Kafka (confluent-kafka), Postgres 16 (psycopg 3), Grafana 11.3 provisioning, pytest.

**Spec:** `docs/superpowers/specs/2026-10-10-inventory-two-users-design.md` (read it first; this plan implements it).

## Global Constraints

- Defense date is 13 October 2026. The D-1 mart (Task 9) is the first thing to cut if time runs out; Tasks 1 to 8 and 10 are the core.
- The inventory is simulated and is labeled as simulated wherever a number is shown (dashboard descriptions, README).
- Eight dark stores exactly: Cairo 3 (Nasr City, Maadi, New Cairo), Giza 1, Alexandria 2, Mansoura 1, Tanta 1. Assiut and "Other" are "not served" and excluded from out-of-stock metrics.
- Stock event envelope: `event_id, event_type, event_version (1), event_time, store_id, product_id, payload`. A flat event without `payload` is quarantined.
- `available = on_hand - reserved_qty`; `reserved_qty` is 0 by assumption. Out of stock means `available <= 0`; low stock means `0 < available < reorder_point`.
- No noise injection on inventory events. Inventory is opt-in (`--inventory`, `--scenario hero-stockout`); with the flags off the simulator stream and every existing count must stay byte for byte identical.
- Stock now is a full recompute per touched `(store_id, product_id)`: the latest snapshot (by `event_time`, ties by `event_id`) plus the sum of `qty_delta` of movements with `event_time` strictly after that snapshot. A replay or a late event never double counts.
- The marketing metric is titled "estimated spend exposed to out-of-stock". Spend is allocated per click: `exposed_spend = spend * oos_clicks / clicks`.
- Never commit `data/`, `.venv`, `.env` or secrets. Destructive Databricks actions and lake resets need the user's explicit say-so.
- Run only ONE Spark container at a time (Docker has 8 GB). Spark tests run in Docker per file: `docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests/<file>.py -q -p no:cacheprovider`. Host tests: `.venv\Scripts\python -m pytest -q`. Two host tests fail before this work and are not yours: `tests/test_checks.py::test_expected_quarantine_is_the_sum_of_the_nine_kinds_and_ignores_the_rest` and `::test_the_repo_manifest_loads` (they expect `duplicate=25` in the git-ignored `noise_manifest.json`).
- Windows PowerShell 5.1 strips inner double quotes when it passes arguments to native programs. Send SQL through stdin: `"SELECT 1;" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'`.
- Commit steps run only when your human partner has said to commit. The author is set per command, nothing is stored in git config: `git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit ...`, and the message ends with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Plain prose in files and commit messages; no emoji.

## Review Focus

Failure modes the spec implies but no happy-path test exercises, most likely first. Each has its test in the task named at the end of the line.

1. A stock event replayed or delivered twice (same `event_id`) must not change stock (Task 2 Spark replay test, Task 7 recompute test).
2. A movement that arrives late with an `event_time` at or before the latest snapshot must not change stock now (Task 7 recompute test).
3. A customer in Assiut or "Other" (no store) buys: no stock event, no crash (Task 3 and Task 5 tests).
4. A sale larger than the stock: the movement is clamped to what was there, `requested_qty` records the ask, stock never goes below zero, and a sale of zero units emits nothing (the validator rejects a zero `qty_delta`) (Task 5 tests).
5. Postgres down or a Spark compute failing in the middle of a batch: `on_batch` returns None, never raises, and the stock pairs stay pending for the next batch (Task 7 host test).

## Map to the sprint sheet

| Sprint row | Tasks of this plan |
|---|---|
| INV1 contract | 1, 2 (transform part) |
| INV2 simulator | 3, 5, 6 |
| INV3 Bronze and Silver tables | 2 (tables part), 4 |
| INV4 live tables and sink | 7 |
| INV5 Grafana operations dashboard and alert | 8 |
| INV6 D-1 mart and page | 9 |
| documentation and rehearsal | 10 |

Order: 1, 3 (independent), 2, 4, 5, 6, 7, 8, then 9 and 10. Tasks 1 and 3 can run in parallel.

## File structure

| File | Responsibility |
|---|---|
| `contracts/topics.yaml` | the topic and the two event type names |
| `contracts/validate.py` | stock envelope and payload validation |
| `contracts/stores.py` (new) | the one source of the eight stores and the district to store map |
| `streaming/silver/schemas.py`, `spark_schemas.py`, `writer.py`, `transform.py` | two Silver tables, their keys, the stock transform |
| `streaming/jobs/bronze_ingest.py` | the Bronze table of the new topic |
| `simulator/publish.py`, `simulator/noise.py` | routing of `inventory` events, noise bypass |
| `simulator/stock.py` (new) | `StockLedger`: deterministic stock model and hero scenario |
| `simulator/core.py`, `simulator/orders.py`, `scripts/run_simulator.py` | scenario hooks, `--inventory`, `--scenario`, live post-pass |
| `warehouse/live_common.py` (new) | `_read` and `_utc`, moved out of `live_store.py` |
| `warehouse/live_stock.py` (new) | Spark computes for stock now and out-of-stock demand, Postgres writers |
| `warehouse/live_store.py`, `warehouse/live_tables.py`, `warehouse/live_ddl.sql`, `scripts/apply_live_ddl.py` | sink integration, table specs, DDL, store dimension seeding |
| `monitoring/dashboards.py` (new), `scripts/gen_dashboards.py` (new) | dashboard builders and generator |
| `monitoring/grafana/dashboards/*.json`, `monitoring/grafana/provisioning/alerting/alerting.yaml` | the two new dashboards, the new alert |
| `warehouse/d1_marketing.py` (new), `scripts/build_d1_marketing.py` (new) | the D-1 mart |

---

### Task 1: Stock event contract and validator

**Files:**
- Modify: `contracts/topics.yaml`
- Modify: `contracts/validate.py`
- Create: `tests/test_inventory_contract.py`

**Interfaces:**
- Consumes: the existing `validate_event(event, now=None, known_products=None) -> (ok, code)`.
- Produces: `INVENTORY_EVENT_TYPES` (set of `stock_snapshot`, `stock_movement`), `STOCK_REASONS`, `KNOWN_TYPES` including them; the topic `inventory.events` in `contracts/topics.yaml`.

- [ ] **Step 1: Write the failing test**

`tests/test_inventory_contract.py`:

```python
from datetime import datetime, timezone

import pytest

from contracts.validate import INVENTORY_EVENT_TYPES, KNOWN_TYPES, STOCK_REASONS, validate_event

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


def snapshot(**over):
    e = {"event_id": "evt_s1", "event_type": "stock_snapshot", "event_version": 1,
         "event_time": "2026-10-10T11:00:00+00:00", "store_id": "store_cairo_maadi", "product_id": "prod_0001",
         "payload": {"on_hand": 40, "reserved_qty": 0, "reorder_point": 10}}
    e.update(over)
    return e


def movement(**over):
    e = {"event_id": "evt_m1", "event_type": "stock_movement", "event_version": 1,
         "event_time": "2026-10-10T11:00:05+00:00", "store_id": "store_cairo_maadi", "product_id": "prod_0001",
         "payload": {"qty_delta": -2, "reason": "sold", "order_id": "ord_1", "requested_qty": 2}}
    e.update(over)
    return e


def with_payload(base, **payload):
    out = dict(base)
    out["payload"] = {**base["payload"], **payload}
    return out


def test_the_two_types_are_known():
    assert INVENTORY_EVENT_TYPES == {"stock_snapshot", "stock_movement"}
    assert INVENTORY_EVENT_TYPES <= KNOWN_TYPES
    assert STOCK_REASONS == {"received", "sold", "shrinkage", "adjustment"}


def test_valid_events_pass():
    assert validate_event(snapshot(), NOW) == (True, "")
    assert validate_event(movement(), NOW) == (True, "")
    assert validate_event(with_payload(movement(), qty_delta=60, reason="received", order_id=None), NOW) == (True, "")
    assert validate_event(with_payload(movement(), qty_delta=2, reason="adjustment", order_id=None), NOW) == (True, "")
    assert validate_event(with_payload(snapshot(), reserved_qty=None), NOW) == (True, "")  # reserved defaults to 0


@pytest.mark.parametrize("field", ["store_id", "product_id"])
def test_missing_store_or_product_is_missing_field(field):
    assert validate_event(snapshot(**{field: None}), NOW) == (False, "MISSING_FIELD")
    assert validate_event(movement(**{field: ""}), NOW) == (False, "MISSING_FIELD")


def test_a_flat_event_without_payload_is_missing_field():
    flat = snapshot()
    del flat["payload"]
    assert validate_event(flat, NOW) == (False, "MISSING_FIELD")


@pytest.mark.parametrize("bad", [-1, 1.5, "3", True])
def test_snapshot_on_hand_must_be_a_non_negative_integer(bad):
    assert validate_event(with_payload(snapshot(), on_hand=bad), NOW) == (False, "BAD_VALUE")


def test_snapshot_without_on_hand_is_missing_field():
    e = snapshot()
    del e["payload"]["on_hand"]
    assert validate_event(e, NOW) == (False, "MISSING_FIELD")


@pytest.mark.parametrize("delta", [0, 1.5, "2", None])
def test_movement_qty_delta_must_be_a_non_zero_integer(delta):
    code = "MISSING_FIELD" if delta is None else "BAD_VALUE"
    assert validate_event(with_payload(movement(), qty_delta=delta), NOW) == (False, code)


def test_movement_reason_and_sign_rules():
    assert validate_event(with_payload(movement(), reason="stolen"), NOW) == (False, "BAD_VALUE")
    assert validate_event(with_payload(movement(), qty_delta=2), NOW) == (False, "BAD_VALUE")  # sold must be negative
    assert validate_event(with_payload(movement(), qty_delta=-5, reason="received", order_id=None), NOW) == (False, "BAD_VALUE")
    assert validate_event(with_payload(movement(), qty_delta=2, reason="shrinkage", order_id=None), NOW) == (False, "BAD_VALUE")


def test_sold_needs_an_order_id_and_requested_qty_must_be_positive():
    assert validate_event(with_payload(movement(), order_id=None), NOW) == (False, "BAD_VALUE")
    assert validate_event(with_payload(movement(), requested_qty=0), NOW) == (False, "BAD_VALUE")


def test_future_timestamp_and_unknown_product():
    assert validate_event(snapshot(event_time="2026-10-10T13:00:00+00:00"), NOW) == (False, "FUTURE_TIMESTAMP")
    assert validate_event(snapshot(), NOW, known_products={"prod_0002"}) == (False, "UNKNOWN_PRODUCT")
    assert validate_event(snapshot(), NOW, known_products={"prod_0001"}) == (True, "")


def test_wrong_version_is_bad_version():
    assert validate_event(snapshot(event_version=2), NOW) == (False, "BAD_VERSION")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_contract.py -q`
Expected: collection error `ImportError: cannot import name 'INVENTORY_EVENT_TYPES'` (feature missing).

- [ ] **Step 3: Implement**

In `contracts/topics.yaml` add the topic under `topics:` and the type list at the end:

```yaml
  inventory.events: {partitions: 3, key: store_id, retention_hours: 72}
```

```yaml
inventory_event_types:
  - stock_snapshot
  - stock_movement
```

In `contracts/validate.py`, replace the two lines that define `DELIVERY_EVENT_TYPES` and `KNOWN_TYPES` with:

```python
DELIVERY_EVENT_TYPES = set(_TOPICS["delivery_event_types"])
INVENTORY_EVENT_TYPES = set(_TOPICS["inventory_event_types"])
KNOWN_TYPES = APP_EVENT_TYPES | ORDER_EVENT_TYPES | DELIVERY_EVENT_TYPES | INVENTORY_EVENT_TYPES
STOCK_REASONS = {"received", "sold", "shrinkage", "adjustment"}
```

Add this helper above `validate_event`:

```python
def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _check_stock(event_type, payload):
    """'' if the payload of a stock event is fine, else MISSING_FIELD or BAD_VALUE (integers only, no strings)."""
    if event_type == "stock_snapshot":
        if payload.get("on_hand") is None:
            return "MISSING_FIELD"
        for key in ("on_hand", "reserved_qty", "reorder_point"):
            value = payload.get(key)
            if value is not None and (not _is_int(value) or value < 0):
                return "BAD_VALUE"
        return ""
    qty = payload.get("qty_delta")
    if qty is None:
        return "MISSING_FIELD"
    if not _is_int(qty) or qty == 0:
        return "BAD_VALUE"
    reason = payload.get("reason")
    if not isinstance(reason, str) or reason not in STOCK_REASONS:
        return "BAD_VALUE"
    if reason == "received" and qty < 0:
        return "BAD_VALUE"
    if reason in ("sold", "shrinkage") and qty > 0:
        return "BAD_VALUE"
    if reason == "sold" and payload.get("order_id") in (None, ""):
        return "BAD_VALUE"
    requested = payload.get("requested_qty")
    if requested is not None and (not _is_int(requested) or requested < 1):
        return "BAD_VALUE"
    return ""
```

In `_validate`, extend the id check:

```python
    elif event_type in ORDER_EVENT_TYPES or event_type in DELIVERY_EVENT_TYPES:
        if event.get("order_id") in (None, ""):
            return False, "MISSING_FIELD"
    elif event_type in INVENTORY_EVENT_TYPES:
        for field in ("store_id", "product_id"):
            if event.get(field) in (None, ""):
                return False, "MISSING_FIELD"
```

and add the payload check between `_check_numbers` and the product block:

```python
    code = _check_numbers(payload)
    if code:
        return False, code

    if event_type in INVENTORY_EVENT_TYPES:
        code = _check_stock(event_type, payload)
        if code:
            return False, code
        if known_products is not None and event["product_id"] not in known_products:
            return False, "UNKNOWN_PRODUCT"

    if event_type in PRODUCT_EVENT_TYPES:
```

- [ ] **Step 4: Run the test and the host suite**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_contract.py -q` then `.venv\Scripts\python -m pytest -q`
Expected: the new file passes. In the full suite, a test that pins the set of known types or topics may fail; update its expected set to include the new names (that is the only allowed edit to existing tests here). Anything else failing stops the task.

- [ ] **Step 5: Commit** (only when told to commit)

```bash
git add contracts/topics.yaml contracts/validate.py tests/test_inventory_contract.py
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Add the stock event contract and its validation"
```

---

### Task 2: Silver tables and the stock transform

**Files:**
- Modify: `streaming/silver/schemas.py`, `streaming/silver/spark_schemas.py`, `streaming/silver/writer.py`, `streaming/silver/transform.py`, `streaming/jobs/silver_clean.py`
- Create: `tests/test_inventory_silver_transform.py`, `tests/test_inventory_silver_spark.py`

**Interfaces:**
- Consumes: Task 1 (`INVENTORY_EVENT_TYPES`, valid stock events).
- Produces: Silver tables `silver_stock_movements` and `silver_stock_snapshots` (columns below, keys `event_id`, partition `event_date`); `transform_message` returns `[("silver_stock_snapshots", row)]` or `[("silver_stock_movements", row)]`; `process_batch(..., stats=...)` sets `stats["stock_pairs"]`, a sorted list of `(store_id, product_id)` tuples of the batch, when `stats["collect_minutes"]` is true.

Columns:
- `silver_stock_movements`: `event_id, store_id, product_id, qty_delta, reason, order_id, requested_qty, event_time, kafka_timestamp, event_date`
- `silver_stock_snapshots`: `event_id, store_id, product_id, on_hand, reserved_qty, reorder_point, event_time, kafka_timestamp, event_date`

- [ ] **Step 1: Write the failing host test**

`tests/test_inventory_silver_transform.py`:

```python
import json
from datetime import datetime, timezone

from streaming.silver.schemas import TABLE_COLUMNS
from streaming.silver.transform import transform_message

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)
KAFKA_TS = datetime(2026, 10, 10, 11, 0, 2, tzinfo=timezone.utc)


def raw(event):
    return json.dumps(event)


def run(event):
    return transform_message(raw(event), "inventory.events", KAFKA_TS, NOW, now=NOW)


SNAP = {"event_id": "evt_s1", "event_type": "stock_snapshot", "event_version": 1,
        "event_time": "2026-10-10T11:00:00+00:00", "store_id": "store_cairo_maadi", "product_id": "prod_0001",
        "payload": {"on_hand": 40, "reorder_point": 10}}
MOVE = {"event_id": "evt_m1", "event_type": "stock_movement", "event_version": 1,
        "event_time": "2026-10-10T11:00:05+00:00", "store_id": "store_cairo_maadi", "product_id": "prod_0001",
        "payload": {"qty_delta": -2, "reason": "sold", "order_id": "ord_1", "requested_qty": 3}}


def test_the_new_tables_have_their_columns():
    assert TABLE_COLUMNS["silver_stock_snapshots"] == [
        "event_id", "store_id", "product_id", "on_hand", "reserved_qty", "reorder_point", "event_time",
        "kafka_timestamp", "event_date"]
    assert TABLE_COLUMNS["silver_stock_movements"] == [
        "event_id", "store_id", "product_id", "qty_delta", "reason", "order_id", "requested_qty", "event_time",
        "kafka_timestamp", "event_date"]


def test_a_snapshot_becomes_one_snapshot_row_with_reserved_defaulting_to_zero():
    [(table, row)] = run(SNAP)
    assert table == "silver_stock_snapshots"
    assert list(row) == TABLE_COLUMNS["silver_stock_snapshots"]
    assert (row["store_id"], row["product_id"], row["on_hand"], row["reserved_qty"], row["reorder_point"]) == \
        ("store_cairo_maadi", "prod_0001", 40, 0, 10)
    assert row["event_time"] == "2026-10-10T11:00:00+00:00" and row["kafka_timestamp"] == KAFKA_TS
    assert row["event_date"] == "2026-10-10"  # the Cairo date of 14:00 local time


def test_a_movement_becomes_one_movement_row():
    [(table, row)] = run(MOVE)
    assert table == "silver_stock_movements"
    assert list(row) == TABLE_COLUMNS["silver_stock_movements"]
    assert (row["qty_delta"], row["reason"], row["order_id"], row["requested_qty"]) == (-2, "sold", "ord_1", 3)


def test_the_cairo_date_follows_local_midnight():
    late = dict(SNAP, event_time="2026-10-10T22:30:00+00:00")  # 01:30 on the 11th in Cairo
    [(_, row)] = run(late)
    assert row["event_date"] == "2026-10-11"


def test_an_invalid_stock_event_is_quarantined_not_parsed_as_an_order():
    bad = dict(MOVE, payload={"qty_delta": 0, "reason": "sold", "order_id": "ord_1"})
    [(table, row)] = run(bad)
    assert table == "silver_quarantine" and row["error_code"] == "BAD_VALUE" and row["source_topic"] == "inventory.events"


def test_a_flat_stock_event_is_quarantined():
    flat = {k: v for k, v in SNAP.items() if k != "payload"}
    [(table, row)] = run(flat)
    assert table == "silver_quarantine" and row["error_code"] == "MISSING_FIELD"
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_silver_transform.py -q`
Expected: FAIL (`KeyError: 'silver_stock_snapshots'`).

- [ ] **Step 3: Implement the columns, schemas, keys and transform**

`streaming/silver/schemas.py`: add before `TABLE_COLUMNS`

```python
SILVER_STOCK_MOVEMENTS_COLUMNS = [
    "event_id", "store_id", "product_id", "qty_delta", "reason", "order_id", "requested_qty", "event_time",
    "kafka_timestamp", "event_date",
]
SILVER_STOCK_SNAPSHOTS_COLUMNS = [
    "event_id", "store_id", "product_id", "on_hand", "reserved_qty", "reorder_point", "event_time",
    "kafka_timestamp", "event_date",
]
```

and add the two entries to `TABLE_COLUMNS`:

```python
    "silver_stock_movements": SILVER_STOCK_MOVEMENTS_COLUMNS,
    "silver_stock_snapshots": SILVER_STOCK_SNAPSHOTS_COLUMNS,
```

`streaming/silver/spark_schemas.py`: add to `_TYPES`

```python
    "silver_stock_movements": {"qty_delta": _INT, "requested_qty": _INT, "event_time": _TS, "kafka_timestamp": _TS,
                               "event_date": _DATE},
    "silver_stock_snapshots": {"on_hand": _INT, "reserved_qty": _INT, "reorder_point": _INT, "event_time": _TS,
                               "kafka_timestamp": _TS, "event_date": _DATE},
```

`streaming/silver/writer.py`: add to `SILVER_KEYS` and `PARTITION_COLS`

```python
    "silver_stock_movements": ["event_id"],
    "silver_stock_snapshots": ["event_id"],
```

```python
    "silver_stock_movements": "event_date",
    "silver_stock_snapshots": "event_date",
```

`streaming/silver/transform.py`: extend the imports (`INVENTORY_EVENT_TYPES` from `contracts.validate`; `SILVER_STOCK_MOVEMENTS_COLUMNS` and `SILVER_STOCK_SNAPSHOTS_COLUMNS` from `streaming.silver.schemas`), add

```python
def _stock_event(e, p, kafka_timestamp):
    et = to_utc_iso(e["event_time"])
    common = dict(event_id=_text(e["event_id"]), store_id=_text(e["store_id"]), product_id=_text(e["product_id"]),
                  event_time=et, kafka_timestamp=kafka_timestamp, event_date=_cairo_date(et))
    if e["event_type"] == "stock_snapshot":
        return [("silver_stock_snapshots", _row(
            SILVER_STOCK_SNAPSHOTS_COLUMNS, **common, on_hand=_int(p.get("on_hand"), "on_hand"),
            reserved_qty=_int(p.get("reserved_qty") or 0, "reserved_qty"),
            reorder_point=_int(p.get("reorder_point"), "reorder_point")))]
    return [("silver_stock_movements", _row(
        SILVER_STOCK_MOVEMENTS_COLUMNS, **common, qty_delta=_int(p.get("qty_delta"), "qty_delta"),
        reason=_text(p.get("reason")), order_id=_text(p.get("order_id")),
        requested_qty=_int(p.get("requested_qty"), "requested_qty")))]
```

and in `transform_message` replace the two dispatch lines with

```python
        if e["event_type"] in APP_EVENT_TYPES:
            return _app_event(e, p, kafka_timestamp)
        if e["event_type"] in INVENTORY_EVENT_TYPES:
            return _stock_event(e, p, kafka_timestamp)
        return _order_event(e, p)
```

- [ ] **Step 4: Run the host test and fix the count-pinned tests**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_silver_transform.py -q` (expected PASS), then `.venv\Scripts\python -m pytest -q`.
Tests that pin the number or set of Silver tables fail now (the data engineer review listed `tests/test_warehouse_tables.py`, `tests/test_silver_transform.py` and `tests/test_silver_spark_schemas.py`; the list is verified here by running the suite). For each failure that is only about the two new tables, update the expected value (for example `len(SCHEMAS) == 9` and `set(TABLE_COLUMNS)` including the new names). If `scripts/gen_ddl.py --check` fails, it means the stock tables leaked into the generated DDL; they must not: the Databricks mirror list lives in `warehouse/tables.py` and is not changed by this task. Any other failure stops the task.

The Spark helpers in `tests/test_silver_clean_spark.py` assume every Silver table exists once data was processed. With two more tables that stay empty when no stock message arrives, update exactly two things there. In `test_process_batch_counts_and_quarantine_match_manifest` change `assert all(on_disk[t] > 0 for t in SILVER_TABLES), on_disk` to

```python
    assert all(on_disk[t] > 0 for t in SILVER_TABLES if "stock" not in t), on_disk
```

and make `assert_unique_keys` skip a table that was never created:

```python
def assert_unique_keys(spark, lake):
    from delta.tables import DeltaTable

    for t in SILVER_TABLES:
        if not DeltaTable.isDeltaTable(spark, silver_path(str(lake), t)):
            continue
        df = silver(spark, lake, t)
        assert df.count() == df.select(*SILVER_KEYS[t]).distinct().count(), t
```

- [ ] **Step 5: Write the failing Spark test**

`tests/test_inventory_silver_spark.py`:

```python
"""Spark tests (Docker, RUN_SPARK_TESTS=1): stock events through process_batch."""
import json
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.spark

from streaming.jobs.silver_clean import process_batch  # noqa: E402
from test_silver_clean_spark import NOW, bronze_df, silver, silver_counts  # noqa: E402

T = datetime(2026, 10, 1, 11, 0, tzinfo=timezone.utc)  # before the module's NOW (4 Oct) so nothing is "future"


def snap(i, store, product, on_hand, minute=0):
    return {"event_id": "evt_s%d" % i, "event_type": "stock_snapshot", "event_version": 1,
            "event_time": T.replace(minute=minute).isoformat(), "store_id": store, "product_id": product,
            "payload": {"on_hand": on_hand, "reserved_qty": 0, "reorder_point": 10}}


def move(i, store, product, delta, reason="sold", minute=1):
    payload = {"qty_delta": delta, "reason": reason}
    if reason == "sold":
        payload.update(order_id="ord_%d" % i, requested_qty=-delta)
    return {"event_id": "evt_m%d" % i, "event_type": "stock_movement", "event_version": 1,
            "event_time": T.replace(minute=minute).isoformat(), "store_id": store, "product_id": product,
            "payload": payload}


def messages(events):
    kafka_ts = T.replace(tzinfo=None)
    return [("inventory.events", json.dumps(e), kafka_ts) for e in events]


EVENTS = [snap(1, "store_a", "prod_1", 40), snap(2, "store_a", "prod_2", 30), snap(3, "store_b", "prod_1", 50),
          move(1, "store_a", "prod_1", -2), move(2, "store_b", "prod_1", -1), move(3, "store_a", "prod_2", 60, "received")]


def test_stock_events_land_in_the_two_tables_and_report_their_pairs(spark, tmp_path):
    lake = str(tmp_path / "lake")
    stats = {"collect_minutes": True}
    counts = process_batch(spark, bronze_df(spark, messages(EVENTS)), lake, None, now=NOW, stats=stats)
    assert counts["silver_stock_snapshots"] == 3 and counts["silver_stock_movements"] == 3
    assert silver_counts(spark, lake)["silver_stock_movements"] == 3
    assert stats["stock_pairs"] == [("store_a", "prod_1"), ("store_a", "prod_2"), ("store_b", "prod_1")]
    row = silver(spark, lake, "silver_stock_movements").where("event_id = 'evt_m1'").first()
    assert (row["qty_delta"], row["reason"], row["order_id"], row["requested_qty"]) == (-2, "sold", "ord_1", 2)


def test_a_replay_and_a_duplicate_change_nothing(spark, tmp_path):
    lake = str(tmp_path / "lake")
    df = bronze_df(spark, messages(EVENTS + [EVENTS[3]]))  # one movement delivered twice in the same batch
    process_batch(spark, df, lake, None, now=NOW)
    first = silver_counts(spark, lake)
    process_batch(spark, df, lake, None, now=NOW)  # the batch replayed
    assert silver_counts(spark, lake) == first
    assert first["silver_stock_movements"] == 3


def test_a_bad_stock_event_goes_to_quarantine(spark, tmp_path):
    lake = str(tmp_path / "lake")
    bad = move(9, "store_a", "prod_1", 0)
    counts = process_batch(spark, bronze_df(spark, messages([EVENTS[0], bad])), lake, None, now=NOW)
    assert counts["silver_stock_snapshots"] == 1 and counts["silver_stock_movements"] == 0
    assert counts["silver_quarantine"] == 1
```

- [ ] **Step 6: Run to verify it fails, then implement `stock_pairs`**

Run: `docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests/test_inventory_silver_spark.py -q -p no:cacheprovider`
Expected: FAIL with `KeyError: 'stock_pairs'` in the first test (the tables themselves already work after Step 3).

In `streaming/jobs/silver_clean.py` `process_batch`, next to `to_merge = {}` add `pairs = set()`, inside the table loop after the `quarantine_minutes` branch add

```python
            if want_minutes and n and table in ("silver_stock_movements", "silver_stock_snapshots"):
                pairs.update((r["store_id"], r["product_id"])
                             for r in df.select("store_id", "product_id").distinct().collect())
```

and after `_merge_tables(...)` and before the `district_violations` line add

```python
        if stats is not None and want_minutes:
            stats["stock_pairs"] = sorted(pairs)
```

- [ ] **Step 7: Run the Spark tests and the existing Silver suite**

Run, one after the other (never in parallel): `tests/test_inventory_silver_spark.py`, `tests/test_silver_clean_spark.py`, `tests/test_silver_parallel_spark.py`, `tests/test_g1_live_spark.py`, `tests/test_live_sink_parallel_spark.py`.
Expected: all PASS (3, 16, 4, 10, 4). A failure that comes only from a helper that loops over every Silver table and needs the two empty stock tables to exist is fixed in the helper, like the two above; any other failure stops the task.

- [ ] **Step 8: Commit** (only when told to commit)

```bash
git add streaming tests
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Add the stock Silver tables and the stock transform"
```

---

### Task 3: The store map

**Files:**
- Create: `contracts/stores.py`, `tests/test_stores.py`

**Interfaces:**
- Consumes: `contracts/geo.py` `DISTRICTS`.
- Produces: `STORES` (dict store_id to `(city, name, districts)`), `store_for(city, district) -> str | None`, `store_rows() -> list[dict]` (`store_id, store_name, city`), `district_rows() -> list[dict]` (`city, district, store_id`).

- [ ] **Step 1: Write the failing test**

`tests/test_stores.py`:

```python
from contracts.geo import DISTRICTS
from contracts.stores import STORES, district_rows, store_for, store_rows

SERVED = ["Cairo", "Giza", "Alexandria", "Mansoura", "Tanta"]


def test_eight_stores_with_the_agreed_split_per_city():
    by_city = {}
    for _, (city, _, _) in STORES.items():
        by_city[city] = by_city.get(city, 0) + 1
    assert len(STORES) == 8
    assert by_city == {"Cairo": 3, "Giza": 1, "Alexandria": 2, "Mansoura": 1, "Tanta": 1}


def test_every_district_of_a_served_city_has_exactly_one_store():
    for city in SERVED:
        for district in DISTRICTS[city]:
            owners = [s for s, (c, _, ds) in STORES.items() if c == city and district in ds]
            assert len(owners) == 1, (city, district, owners)
            assert store_for(city, district) == owners[0]


def test_a_store_serves_only_districts_of_its_own_city():
    for store_id, (city, _, districts) in STORES.items():
        assert set(districts) <= set(DISTRICTS[city]), store_id


def test_unserved_places_have_no_store():
    assert store_for("Assiut", "Assiut First") is None
    assert store_for("Other", "Other") is None
    assert store_for("Cairo", "unknown") is None
    assert store_for(None, None) is None
    assert store_for("Atlantis", "Maadi") is None


def test_the_helpers_match_the_map():
    assert store_for("Cairo", "Maadi") == "store_cairo_maadi"
    assert len(store_rows()) == 8
    assert {r["store_id"] for r in store_rows()} == set(STORES)
    assert len(district_rows()) == sum(len(DISTRICTS[c]) for c in SERVED)
    assert {(r["city"], r["district"]): r["store_id"] for r in district_rows()}[("Giza", "Dokki")] == "store_giza_dokki"
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_stores.py -q`
Expected: FAIL (`ModuleNotFoundError: contracts.stores`).

- [ ] **Step 3: Implement**

`contracts/stores.py`:

```python
"""The eight simulated dark stores and the district to store map (spec 2026-10-10 inventory design, section 5).

One source for everything: the simulator picks the store of a sale from it, and the same rows are written to the
Postgres tables dim_store and dim_store_district, where the live queries join demand to stock in SQL. A real
system uses delivery zones; here each district has exactly one primary store. Assiut and "Other" have no store
("not served") and are left out of every out-of-stock metric.
"""

STORES = {
    "store_cairo_nasr_city": ("Cairo", "Nasr City dark store",
                              ("Nasr City", "Heliopolis", "Shubra", "El Matareya", "El Marg", "El Zaytoun")),
    "store_cairo_maadi": ("Cairo", "Maadi dark store",
                          ("Maadi", "Zamalek", "Downtown", "Garden City", "Helwan", "Sayeda Zeinab")),
    "store_cairo_new_cairo": ("Cairo", "New Cairo dark store", ("New Cairo", "El Shorouk", "El Moqattam")),
    "store_giza_dokki": ("Giza", "Dokki dark store",
                         ("Dokki", "Agouza", "Haram", "Imbaba", "Omraniya", "Bulaq El Dakrour", "El Warraq",
                          "Giza (centre)")),
    "store_alex_raml": ("Alexandria", "Raml dark store",
                        ("Raml", "Sidi Gaber", "Moharam Bek", "Karmouz", "Attarin", "Manshiya")),
    "store_alex_montaza": ("Alexandria", "Montaza dark store", ("Montaza", "Amreya", "Dekhela", "Labban")),
    "store_mansoura": ("Mansoura", "Mansoura dark store", ("Mansoura First", "Mansoura Second")),
    "store_tanta": ("Tanta", "Tanta dark store", ("Tanta First", "Tanta Second")),
}

_STORE_OF = {(city, district): store_id
             for store_id, (city, _, districts) in STORES.items() for district in districts}


def store_for(city, district):
    """store_id that serves the (canonical city, canonical district), or None when it is not served."""
    return _STORE_OF.get((city, district))


def store_rows():
    return [{"store_id": store_id, "store_name": name, "city": city} for store_id, (city, name, _) in STORES.items()]


def district_rows():
    return [{"city": city, "district": district, "store_id": store_id}
            for (city, district), store_id in sorted(_STORE_OF.items())]
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv\Scripts\python -m pytest tests\test_stores.py -q`
Expected: 5 passed.

- [ ] **Step 5: Commit** (only when told to commit)

```bash
git add contracts/stores.py tests/test_stores.py
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Add the dark store map"
```

---

### Task 4: Topic wiring: routing, Bronze table, noise bypass

**Files:**
- Modify: `simulator/publish.py`, `simulator/noise.py`, `streaming/jobs/bronze_ingest.py`
- Create: `tests/test_inventory_routing.py`

**Interfaces:**
- Consumes: Task 1 (`inventory.events` in `topics.yaml`).
- Produces: `route("inventory", event) -> ("inventory.events", store_id)`; `BRONZE_TABLES["inventory.events"] == "bronze_inventory_events"`; `apply_noise` yields `inventory` hints unchanged.

- [ ] **Step 1: Write the failing test**

`tests/test_inventory_routing.py`:

```python
import random

import yaml
from pathlib import Path

from simulator.noise import NoiseConfig, NoiseCounter, apply_noise
from simulator.publish import route
from streaming.jobs.bronze_ingest import BRONZE_TABLES, bronze_path

TOPICS = yaml.safe_load((Path(__file__).parent.parent / "contracts" / "topics.yaml").read_text())["topics"]


def stock_event(i):
    return {"event_id": "evt_%d" % i, "event_type": "stock_movement", "event_version": 1,
            "event_time": "2026-10-10T11:00:%02d+00:00" % (i % 60), "store_id": "store_cairo_maadi",
            "product_id": "prod_0001", "payload": {"qty_delta": -1, "reason": "sold", "order_id": "ord_%d" % i}}


def test_inventory_events_go_to_their_topic_keyed_by_store():
    assert route("inventory", stock_event(1)) == ("inventory.events", "store_cairo_maadi")
    assert route("inventory", {"event_type": "stock_snapshot"}) == ("inventory.events", "unparsable")


def test_every_topic_has_a_bronze_table_and_the_new_one_is_named_as_agreed():
    assert set(BRONZE_TABLES) == set(TOPICS)
    assert BRONZE_TABLES["inventory.events"] == "bronze_inventory_events"
    assert bronze_path("lake", "inventory.events").endswith("bronze_inventory_events")


def test_noise_never_touches_inventory_events():
    events = [("inventory", stock_event(i)) for i in range(400)]
    before = [dict(e) for _, e in events]
    counter = NoiseCounter()
    out = list(apply_noise(iter(events), random.Random(3), NoiseConfig().scaled(20), set(), counter))
    assert [h for h, _ in out] == ["inventory"] * 400
    assert [p for _, p in out] == before  # same events, same order, nothing changed or dropped or duplicated
    assert sum(counter.counts.values()) == 0
    assert counter.total_messages == 400
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_routing.py -q`
Expected: 3 failures (`route` sends it to `orders`; `BRONZE_TABLES` lacks the topic; noise changes events).

- [ ] **Step 3: Implement**

`simulator/publish.py`: change the docstring first line to `source is 'app' for app/web events, 'order' for order and delivery events, 'inventory' for stock events.` and add as the first lines of `route`:

```python
    if source == "inventory":
        if isinstance(event, str):  # not produced today (no noise on inventory), kept for symmetry
            return "inventory.events", UNPARSABLE_KEY
        return "inventory.events", event.get("store_id") or UNPARSABLE_KEY
```

`streaming/jobs/bronze_ingest.py`: add `"inventory.events": "bronze_inventory_events",` to `BRONZE_TABLES`.

`simulator/noise.py`, in `_apply`, directly before the line `new_session = False` add:

```python
        if hint == "inventory":  # stock events are never noised (inventory design, section 2)
            yield out(hint, e, pace)
            continue
```

- [ ] **Step 4: Run, then fix pinned tests**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_routing.py -q` (PASS), then the full host suite. Tests that pin the topic set (`tests/test_bronze.py`, `tests/test_c1_changes.py` per the review) get the new topic added to their expected value. Any other failure stops the task.

- [ ] **Step 5: Spark check of the pipeline start**

Run: `docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests/test_g1_live_spark.py -q -p no:cacheprovider -k ensure_bronze`
Expected: PASS (the test creates every Bronze table, so it now creates four).

- [ ] **Step 6: Commit** (only when told to commit)

```bash
git add simulator streaming tests
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Route stock events to their topic and Bronze table, and keep noise off them"
```

---

### Task 5: The stock ledger and the hero scenario

**Files:**
- Create: `simulator/stock.py`, `tests/test_stock_ledger.py`
- Modify: `simulator/core.py` (`journey`), `simulator/orders.py` (`make_order`), `scripts/run_simulator.py` (`iter_batch`)

**Interfaces:**
- Consumes: Task 3 (`STORES`, `store_for`); `make_catalog`, `make_customers`.
- Produces:
  - `StockLedger(seed, catalog, start, scenario=False, hero_units=DEFAULT_HERO_UNITS, snapshot_every=timedelta(minutes=10), restock_after=timedelta(seconds=90))` with `.opening() -> list[dict]`, `.order(order_created_event, store_id)`, `.due(t) -> list[dict]`, `.finish() -> list[dict]`, attributes `.stock`, `.reorder`, `.oversold_units`, `.scenario` (a `HeroScenario` or None).
  - `HeroScenario(store_id, products, share=0.6)`.
  - `journey(..., hot=None)`, `make_order(..., first_item=None)`, `iter_batch(..., ledger=None, scenario=None)`; with all three left at None the stream is unchanged.

- [ ] **Step 1: Write the failing tests**

`tests/test_stock_ledger.py`:

```python
import hashlib
import json
import random
from datetime import datetime, timedelta, timezone

from contracts.stores import STORES, store_for
from scripts.run_simulator import iter_batch
from simulator.catalog import make_catalog, make_customers
from simulator.stock import DEFAULT_HERO_UNITS, HERO_STORE, StockLedger

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
CATALOG = make_catalog(7)
CUSTOMERS = make_customers(7, 1000)
ACTIVE = [p for p in CATALOG if p["is_active"]]

# digest of the stream before the inventory work (seed 5, universe 7, 300 events): proves "flags off = unchanged"
BASELINE_DIGEST = "4c861df1e4ef8b827014a7eee5e42490c9fc492d"
BASELINE_COUNT = 320


def order_event(order_id, product_id, qty, t):
    return {"event_type": "order_created", "order_id": order_id, "event_time": t.isoformat(),
            "payload": {"items": [{"product_id": product_id, "quantity": qty}]}}


def replay(events):
    """Apply emitted events in order like a consumer: snapshots set the level, movements add to it."""
    level, low = {}, 0
    for e in events:
        key = (e["store_id"], e["product_id"])
        if e["event_type"] == "stock_snapshot":
            level[key] = e["payload"]["on_hand"]
        else:
            level[key] = level.get(key, 0) + e["payload"]["qty_delta"]
        low = min(low, level[key])
    return level, low


def test_the_stream_without_inventory_is_unchanged():
    out = list(iter_batch(random.Random(5), CUSTOMERS, CATALOG, T0, max_events=300))
    assert len(out) == BASELINE_COUNT
    assert hashlib.sha1(json.dumps(out, sort_keys=True).encode()).hexdigest() == BASELINE_DIGEST


def test_opening_snapshots_cover_every_store_and_active_product_and_are_deterministic():
    a, b = StockLedger(1, CATALOG, T0), StockLedger(1, CATALOG, T0)
    opening = a.opening()
    assert len(opening) == len(STORES) * len(ACTIVE)
    assert opening == b.opening()
    assert opening != StockLedger(2, CATALOG, T0).opening()
    assert all(200 <= e["payload"]["on_hand"] <= 500 for e in opening)
    assert all(e["event_time"] == T0.isoformat() and e["event_type"] == "stock_snapshot" for e in opening)


def test_a_sale_larger_than_the_stock_is_clamped_counted_and_never_negative():
    ledger = StockLedger(1, CATALOG, T0, scenario=True, hero_units=5)
    hero = ledger.scenario.products[0]["product_id"]
    ledger.order(order_event("ord_1", hero, 3, T0 + timedelta(minutes=1)), HERO_STORE)
    ledger.order(order_event("ord_2", hero, 3, T0 + timedelta(minutes=2)), HERO_STORE)
    ledger.order(order_event("ord_3", hero, 1, T0 + timedelta(minutes=3)), HERO_STORE)  # nothing left: no event
    events = ledger.due(T0 + timedelta(minutes=2, seconds=5))
    sold = [e for e in events if e["event_type"] == "stock_movement" and e["payload"]["reason"] == "sold"]
    assert [e["payload"]["qty_delta"] for e in sold] == [-3, -2]
    assert sold[1]["payload"]["requested_qty"] == 3 and sold[1]["payload"]["order_id"] == "ord_2"
    assert ledger.stock[(HERO_STORE, hero)] == 0 and ledger.oversold_units == 1
    rest = ledger.due(T0 + timedelta(hours=1))
    assert [e for e in rest if e["payload"].get("reason") == "sold"] == []  # the zero sale emitted nothing
    assert ledger.oversold_units == 2
    received = [e for e in rest if e["payload"].get("reason") == "received"]
    assert len(received) == 1 and received[0]["payload"]["qty_delta"] >= 60
    assert received[0]["event_time"] == (T0 + timedelta(minutes=2, seconds=90)).isoformat()  # 90 s after the stock-out
    assert ledger.stock[(HERO_STORE, hero)] == received[0]["payload"]["qty_delta"]


def test_emitted_events_never_drive_stock_below_zero_and_end_where_the_ledger_ends():
    ledger = StockLedger(3, CATALOG, T0, scenario=True)
    events = [e for h, e in iter_batch(random.Random(9), CUSTOMERS, CATALOG, T0, max_events=1500, mean_gap=1.2,
                                       ledger=ledger, scenario=ledger.scenario) if h == "inventory"]
    assert events, "no inventory events were produced"
    level, low = replay(events)
    assert low >= 0
    assert level == ledger.stock


def test_the_sink_rule_gives_the_ledger_stock():
    """latest snapshot per pair plus the movements strictly after it (the rule of live_stock_now)."""
    ledger = StockLedger(3, CATALOG, T0, scenario=True)
    events = [e for h, e in iter_batch(random.Random(9), CUSTOMERS, CATALOG, T0, max_events=1500, mean_gap=1.2,
                                       ledger=ledger, scenario=ledger.scenario) if h == "inventory"]
    snaps, moves = {}, {}
    for e in events:
        key = (e["store_id"], e["product_id"])
        if e["event_type"] == "stock_snapshot":
            if key not in snaps or (e["event_time"], e["event_id"]) > (snaps[key]["event_time"], snaps[key]["event_id"]):
                snaps[key] = e
        else:
            moves.setdefault(key, []).append(e)
    for key, snap in snaps.items():
        after = sum(m["payload"]["qty_delta"] for m in moves.get(key, []) if m["event_time"] > snap["event_time"])
        assert snap["payload"]["on_hand"] + after == ledger.stock[key], key


def test_the_hero_scenario_runs_a_product_out_and_restocks_it():
    ledger = StockLedger(3, CATALOG, T0, scenario=True, hero_units=DEFAULT_HERO_UNITS)
    hero_ids = {p["product_id"] for p in ledger.scenario.products}
    events = [e for h, e in iter_batch(random.Random(9), CUSTOMERS, CATALOG, T0, max_events=3000, mean_gap=1.2,
                                       ledger=ledger, scenario=ledger.scenario) if h == "inventory"]
    restocks = [e for e in events if e["store_id"] == HERO_STORE and e["product_id"] in hero_ids
                and e["event_type"] == "stock_movement" and e["payload"]["reason"] == "received"]
    assert restocks, "no hero product ran out within 3000 events: lower DEFAULT_HERO_UNITS in simulator/stock.py"


def test_hero_sessions_come_from_customers_of_the_hero_store_and_other_cities_are_untouched():
    ledger = StockLedger(3, CATALOG, T0, scenario=True)
    stream = list(iter_batch(random.Random(9), CUSTOMERS, CATALOG, T0, max_events=1500, mean_gap=1.2,
                             ledger=ledger, scenario=ledger.scenario))
    hero_ids = {p["product_id"] for p in ledger.scenario.products}
    views = [e for h, e in stream if h == "app" and e["event_type"] == "product_view"
             and e["payload"]["product_id"] in hero_ids]
    assert views
    served = [e for e in views if e["city"] == "Cairo" and store_for("Cairo", e["district"]) == HERO_STORE]
    assert len(served) / len(views) > 0.8
    assert any(e["event_type"] == "add_to_cart" and e["payload"]["product_id"] in hero_ids for h, e in stream if h == "app")


def test_customers_without_a_store_buy_without_stock_events():
    ledger = StockLedger(3, CATALOG, T0)
    unserved = [c for c in CUSTOMERS if store_for(c["city"], c.get("district")) is None]
    assert unserved, "the universe has no Assiut or Other customer"
    stream = list(iter_batch(random.Random(4), unserved, CATALOG, T0, max_events=1500, ledger=ledger))
    assert [e for h, e in stream if h == "order"], "no order was placed"
    assert [e for h, e in stream if h == "inventory" and e["payload"].get("reason") == "sold"] == []
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_stock_ledger.py -q`
Expected: collection error (`ModuleNotFoundError: simulator.stock`). The digest test must pass once the module exists, because the stream is unchanged.

- [ ] **Step 3: Implement the ledger**

`simulator/stock.py`:

```python
"""Deterministic stock model for the inventory events (inventory design, section 6).

Time order is the whole point. A sale is queued at the time of its order and applied only when the clock passes
that time, so every snapshot carries exactly the stock at its own event_time and the live rule "latest snapshot plus
the movements strictly after it" gives the right level. Everything draws from one private rng derived from the run
seed; the main event stream is never touched.
"""
import hashlib
import heapq
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from contracts.stores import STORES

HERO_STORE = "store_cairo_nasr_city"
DEFAULT_HERO_UNITS = 12
RESTOCK_UNITS = 60
_ORDER = {"sell": 0, "restock": 1, "extra": 2, "grid": 3}  # at equal times: movements before the snapshot grid


@dataclass
class HeroScenario:
    store_id: str
    products: list  # product dicts of the catalog
    share: float = 0.6  # share of sessions drawn from customers served by the hero store


class StockLedger:
    def __init__(self, seed, catalog, start, scenario=False, hero_units=DEFAULT_HERO_UNITS,
                 snapshot_every=timedelta(minutes=10), restock_after=timedelta(seconds=90)):
        self.seed = seed
        self._rng = random.Random(seed * 7919 + 13)
        self._start = start
        self._snapshot_every = snapshot_every
        self._restock_after = restock_after
        self._n = 0
        self._seq = 0
        self._heap = []
        self._dirty = set()
        self._restock_pending = set()
        self.oversold_units = 0
        self.stock, self.reorder, self._opening = {}, {}, {}
        active = [p for p in catalog if p["is_active"]]
        for store_id in sorted(STORES):
            for p in active:
                qty = self._rng.randint(200, 500)
                key = (store_id, p["product_id"])
                self.stock[key] = self._opening[key] = qty
                self.reorder[key] = max(10, qty // 5)
        self.scenario = None
        if scenario:
            hero = self._rng.sample(active, 3)
            for p in hero:
                key = (HERO_STORE, p["product_id"])
                self.stock[key] = self._opening[key] = hero_units
                self.reorder[key] = 3
            self.scenario = HeroScenario(HERO_STORE, hero)
            others = [k for k in sorted(self.stock) if k[0] != HERO_STORE]
            self._push(start + timedelta(minutes=3), "extra", ("shrinkage", self._rng.choice(others), -1))
            self._push(start + timedelta(minutes=4), "extra", ("adjustment", self._rng.choice(others), 2))
        self._push(start + snapshot_every, "grid", None)

    # -- events ------------------------------------------------------------------------------------------
    def _event_id(self, kind):
        self._n += 1
        return "evt_" + hashlib.sha1(("%d|%s|%d" % (self.seed, kind, self._n)).encode()).hexdigest()[:16]

    def _event(self, event_type, key, t, payload):
        return {"event_id": self._event_id(event_type), "event_type": event_type, "event_version": 1,
                "event_time": t.isoformat(), "store_id": key[0], "product_id": key[1], "payload": payload}

    def _snapshot(self, key, t):
        return self._event("stock_snapshot", key, t, {"on_hand": self.stock[key], "reserved_qty": 0,
                                                      "reorder_point": self.reorder[key]})

    def _movement(self, key, t, qty_delta, reason, order_id=None, requested=None):
        payload = {"qty_delta": qty_delta, "reason": reason}
        if order_id:
            payload["order_id"] = order_id
        if requested is not None:
            payload["requested_qty"] = requested
        return self._event("stock_movement", key, t, payload)

    # -- clock ---------------------------------------------------------------------------------------------
    def _push(self, when, kind, data):
        heapq.heappush(self._heap, (when, _ORDER[kind], self._seq, kind, data))
        self._seq += 1

    def opening(self):
        """The opening snapshot of every store and active product, at the start time."""
        return [self._snapshot(key, self._start) for key in sorted(self.stock)]

    def order(self, order_created, store_id):
        """Queue the lines of an order_created event as sales at the order time."""
        t = datetime.fromisoformat(order_created["event_time"])
        for line in order_created["payload"]["items"]:
            self._push(t, "sell", ((store_id, line["product_id"]), int(line["quantity"]), order_created["order_id"]))

    def due(self, t):
        """Every stock event whose time is at or before t, in time order."""
        out = []
        while self._heap and self._heap[0][0] <= t:
            when, _, _, kind, data = heapq.heappop(self._heap)
            out.extend(self._apply(when, kind, data))
        return out

    def finish(self):
        """What is still queued at the end of a run, except the snapshot grid."""
        out = []
        while any(item[3] != "grid" for item in self._heap):
            item = min(i for i in self._heap if i[3] != "grid")
            self._heap.remove(item)
            heapq.heapify(self._heap)
            out.extend(self._apply(item[0], item[3], item[4]))
        return out

    def _apply(self, when, kind, data):
        if kind == "sell":
            key, qty, order_id = data
            have = self.stock.get(key)
            if have is None:  # an inactive product is not stocked
                return []
            taken = min(qty, have)
            self.oversold_units += qty - taken
            if taken == 0:
                return []
            self.stock[key] = have - taken
            self._dirty.add(key)
            if self.stock[key] == 0 and key not in self._restock_pending:
                self._restock_pending.add(key)
                self._push(when + self._restock_after, "restock", key)
            return [self._movement(key, when, -taken, "sold", order_id, qty)]
        if kind == "restock":
            key = data
            units = max(RESTOCK_UNITS, self._opening[key] // 2)
            self._restock_pending.discard(key)
            self.stock[key] += units
            self._dirty.add(key)
            return [self._movement(key, when, units, "received")]
        if kind == "extra":
            reason, key, delta = data
            if delta < 0:
                delta = -min(-delta, self.stock[key])  # never below zero
            if delta == 0:
                return []
            self.stock[key] += delta
            self._dirty.add(key)
            return [self._movement(key, when, delta, reason)]
        events = [self._snapshot(key, when) for key in sorted(self._dirty)]  # grid: only pairs that changed
        self._dirty.clear()
        self._push(when + self._snapshot_every, "grid", None)
        return events
```

- [ ] **Step 4: Implement the scenario hooks (only active when the new arguments are given)**

`simulator/core.py`: change the signature to `def journey(rng, customer, catalog, campaign, start, hot=None):`, and replace the lines from `viewed = rng.sample(active_products, n_views)` and the cart block as follows.

```python
    viewed = rng.sample(active_products, n_views)
    if hot:  # inventory scenario: a hero product is the first thing the session looks at
        hero = rng.choice(hot)
        viewed = [hero] + [p for p in viewed if p is not hero][: n_views - 1]
```

```python
    if rng.random() < (0.6 if hot else 0.35):
        item = viewed[0] if hot else rng.choice(viewed)
```

`simulator/orders.py`: change the signature to `def make_order(rng, customer, catalog, campaign, t, order_id, first_item=None):` and replace `items = rng.sample(active, rng.randint(1, 3))` with

```python
    if first_item is not None:
        rest = [p for p in active if p is not first_item]
        items = [first_item] + rng.sample(rest, rng.randint(0, 2))
    else:
        items = rng.sample(active, rng.randint(1, 3))
```

`scripts/run_simulator.py`: add imports `from contracts.stores import store_for`, then change `iter_batch`:

```python
def iter_batch(rng, customers, catalog, start, sessions=None, max_events=None, mean_gap=None, starts=None,
               ledger=None, scenario=None):
```

Docstring addition: `ledger` (a StockLedger) makes the generator also yield ("inventory", event) items in time order (history mode); `scenario` (a HeroScenario) makes most sessions come from the hero store and look at the hero products. Neither argument changes the stream when it is None.

Before the `while` loop add

```python
    by_id = {p["product_id"]: p for p in catalog}
    hero_customers = None
    if scenario is not None:
        hero_customers = [c for c in customers if store_for(c["city"], c.get("district")) == scenario.store_id]
    if ledger is not None:
        for ev in ledger.opening():
            yield "inventory", ev
```

Inside the loop replace the two lines that choose `cust`, `camp` and call `journey` with

```python
        hot = None
        if hero_customers and rng.random() < scenario.share:
            cust, camp, hot = rng.choice(hero_customers), CAMPAIGNS[0], scenario.products
        else:
            cust = rng.choice(customers) if rng.random() < 0.85 else None
            camp = rng.choice(CAMPAIGNS) if rng.random() < 0.4 else None
        events = journey(rng, cust, catalog, camp, t, hot=hot)
        if ledger is not None:
            for ev in ledger.due(t):
                yield "inventory", ev
```

Replace the `make_order(...)` call with

```python
        first_item = None
        if hot:
            cart = next((e for e in events if e["event_type"] == "add_to_cart"), None)
            first_item = by_id.get(cart["payload"]["product_id"]) if cart else None
        order = make_order(rng, cust, catalog, camp, datetime.fromisoformat(purchase["event_time"]),
                           purchase["payload"]["order_id"], first_item=first_item)
```

and after the `for _, oe in order:` loop (still inside the `while`) add

```python
        if ledger is not None:
            created = next(oe for _, oe in order if oe["event_type"] == "order_created")
            store_id = store_for(cust["city"], cust.get("district"))
            if store_id:
                ledger.order(created, store_id)
```

After the `while` loop ends add

```python
    if ledger is not None:
        for ev in ledger.finish():
            yield "inventory", ev
```

- [ ] **Step 5: Run the tests**

Run: `.venv\Scripts\python -m pytest tests\test_stock_ledger.py -q` then the full host suite.
Expected: all PASS. If `test_the_hero_scenario_runs_a_product_out_and_restocks_it` fails, lower `DEFAULT_HERO_UNITS` in `simulator/stock.py` (do not weaken the test), and re-run. If `test_the_stream_without_inventory_is_unchanged` fails, the hooks changed the default stream: fix the hooks.

- [ ] **Step 6: Commit** (only when told to commit)

```bash
git add simulator scripts/run_simulator.py tests/test_stock_ledger.py
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Add the stock ledger and the hero stock-out scenario"
```

---

### Task 6: Runner flags, live mode and the demo script

**Files:**
- Modify: `scripts/run_simulator.py`, `scripts/watch_demo.ps1`
- Create: `tests/test_inventory_runner.py`

**Interfaces:**
- Consumes: Tasks 4 and 5.
- Produces: `run_simulator.py --inventory` and `--scenario hero-stockout` (the scenario implies inventory); `run_pipeline(..., inventory=False, scenario=None, hero_units=None)`; `add_inventory(batch, ledger) -> list` (live post-pass); `watch_demo.ps1 -Inventory`.

- [ ] **Step 1: Write the failing test**

`tests/test_inventory_runner.py`:

```python
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.run_simulator import build_parser, format_event, run_pipeline
from simulator.noise import NoiseConfig

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


class FakePub:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, event):
        self.sent.append((topic, key, event))

    def flush(self):
        pass


def run(tmp_path, **kw):
    pub = FakePub()
    counts = run_pipeline(seed=5, events=400, start=NOW - timedelta(days=3), now=NOW, live=kw.pop("live", False), pub=pub,
                          config=NoiseConfig.off(), manifest_path=str(tmp_path / "m.json"),
                          sleep_fn=lambda s: None, **kw)
    return counts, pub.sent


def test_the_flags_exist_and_default_to_off():
    args = build_parser().parse_args([])
    assert args.inventory is False and args.scenario is None
    args = build_parser().parse_args(["--inventory", "--scenario", "hero-stockout"])
    assert args.inventory is True and args.scenario == "hero-stockout"


def test_without_the_flag_no_inventory_topic_is_used(tmp_path):
    counts, sent = run(tmp_path)
    assert "inventory.events" not in counts and {t for t, _, _ in sent} <= {"app.events", "orders", "delivery.events"}


def test_history_mode_publishes_opening_snapshots_first_and_keys_by_store(tmp_path):
    counts, sent = run(tmp_path, inventory=True)
    stock = [(t, k, e) for t, k, e in sent if t == "inventory.events"]
    assert counts["inventory.events"] == len(stock) > 1000  # 8 stores x active products of opening snapshots
    assert all(k == e["store_id"] for _, k, e in stock)
    assert stock[0][2]["event_type"] == "stock_snapshot"
    assert any(e["event_type"] == "stock_movement" and e["payload"]["reason"] == "sold" for _, _, e in stock)


def test_live_mode_adds_the_same_kind_of_events_in_time_order(tmp_path):
    live_now = datetime.now(timezone.utc)
    pub = FakePub()
    run_pipeline(seed=5, events=300, start=live_now, now=live_now, live=True, pub=pub, config=NoiseConfig.off(),
                 manifest_path=str(tmp_path / "m.json"), rate=50.0, sleep_fn=lambda s: None,
                 now_fn=lambda: live_now + timedelta(hours=1), inventory=True, scenario="hero-stockout")
    stock = [e for t, _, e in pub.sent if t == "inventory.events"]
    assert stock and stock[0]["event_type"] == "stock_snapshot"
    times = [e["event_time"] for e in stock]
    assert times == sorted(times)
    assert any(e["event_type"] == "stock_movement" and e["payload"]["reason"] == "sold" for e in stock)


def test_show_prints_the_store_and_quantity_of_a_stock_event():
    line = format_event("inventory.events", {"event_type": "stock_movement", "event_time": "2026-10-10T11:00:05+00:00",
                                              "store_id": "store_cairo_maadi", "product_id": "prod_0001",
                                              "payload": {"qty_delta": -2, "reason": "sold"}})
    assert "store=" in line and "prod_0001" in line and "qty_delta=-2" in line


def test_the_demo_script_has_an_inventory_switch():
    text = (Path(__file__).parent.parent / "scripts" / "watch_demo.ps1").read_text(encoding="utf-8")
    assert "[switch]$Inventory" in text and "--inventory --scenario hero-stockout" in text
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_runner.py -q`
Expected: failures (unknown arguments, missing switch).

- [ ] **Step 3: Implement**

`scripts/run_simulator.py`:

Add imports `from simulator.stock import DEFAULT_HERO_UNITS, StockLedger`.

Add the parser arguments in `build_parser`:

```python
    ap.add_argument("--inventory", action="store_true",
                    help="also publish simulated dark-store stock events to inventory.events (opt-in)")
    ap.add_argument("--scenario", choices=["hero-stockout"], default=None,
                    help="with stock events: three hero products at the busiest store run out and are restocked "
                         "(implies --inventory)")
    ap.add_argument("--hero-units", type=int, default=None,
                    help="opening stock of each hero product (default %d)" % DEFAULT_HERO_UNITS)
```

Add `add_inventory` next to the other live helpers:

```python
def add_inventory(batch, ledger):
    """Live mode: stock events computed from the FINAL (compressed) timeline, merged into the batch in time order.

    Run after compress_sessions, compress_orders and cap_order_tail and before noise: the sales then sit at the real
    order times and every snapshot matches the stock at its own time."""
    out = [("inventory", ev) for ev in ledger.opening()]
    for hint, e in batch:
        t = datetime.fromisoformat(e["event_time"])
        out.extend(("inventory", ev) for ev in ledger.due(t))
        out.append((hint, e))
        if hint == "order" and e["event_type"] == "order_created":
            store_id = store_for(e.get("city"), e.get("district"))
            if store_id:
                ledger.order(e, store_id)
    out.extend(("inventory", ev) for ev in ledger.finish())
    out.sort(key=lambda he: datetime.fromisoformat(he[1]["event_time"]))
    return out
```

In `format_event` extend the key tuples: `("session_id", "order_id", "customer_id", "store_id")` for the first loop and add `"qty_delta", "on_hand"` to the tuple of payload names (the `store_id` loop prints `store=<last 8 characters>`, which is the abbreviated form the other ids use).

Change the `run_pipeline` signature to add `inventory=False, scenario=None, hero_units=None`, and inside it after `counter = NoiseCounter()`:

```python
    ledger = None
    if inventory or scenario:
        ledger = StockLedger(seed, catalog, start, scenario=scenario == "hero-stockout",
                             hero_units=hero_units or DEFAULT_HERO_UNITS)
    hero = ledger.scenario if ledger else None
```

Live branch: pass `scenario=hero` to `iter_batch(...)` and after `batch.sort(...)` add `if ledger is not None: batch = add_inventory(batch, ledger)`. History branch: `iter_batch(rng, customers, catalog, start, max_events=events, ledger=ledger, scenario=hero)`.

In `main`, pass `inventory=args.inventory, scenario=args.scenario, hero_units=args.hero_units` to `run_pipeline`.

`scripts/watch_demo.ps1`: add `[switch]$Inventory` to the `param(...)` block (next to the other switches, with a comment `# also publish simulated stock events and run the hero stock-out scenario`) and append the text to the generator command. Read the file first: find the line that builds the `run_simulator.py` command and add

```powershell
$inventoryArgs = if ($Inventory) { ' --inventory --scenario hero-stockout' } else { '' }
```

before it, then append `$inventoryArgs` to the argument string of that command. Also add `-Inventory` to the usage line in the header comment.

- [ ] **Step 4: Run the tests**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_runner.py tests\test_stock_ledger.py -q`, then the full host suite.
Expected: PASS. A failure in an existing runner test means the default path changed: fix the change, not the test.

- [ ] **Step 5: Manual check against real Kafka (optional but recommended)**

Run: `docker compose up -d kafka` then `.venv\Scripts\python kafka\create_topics.py` (expect `created inventory.events`), then `.venv\Scripts\python scripts\run_simulator.py --inventory --scenario hero-stockout --events 500 --seed 21 --bootstrap localhost:9092`. Expected: the last line lists `inventory.events` with a count above 1000.

- [ ] **Step 6: Commit** (only when told to commit)

```bash
git add scripts tests/test_inventory_runner.py
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Add the inventory flags to the simulator runner and the demo script"
```

---

### Task 7: Live Postgres tables and the sink recompute

**Files:**
- Create: `warehouse/live_stock.py`, `tests/test_live_stock_spark.py`, `tests/test_live_stock_sink.py`
- Modify: `warehouse/live_ddl.sql`, `warehouse/live_tables.py`, `warehouse/live_store.py`, `scripts/apply_live_ddl.py`, `streaming/jobs/silver_clean.py`

**Interfaces:**
- Consumes: Task 2 (Silver stock tables, `stats["stock_pairs"]`), Task 3 (`store_rows`, `district_rows`).
- Produces:
  - Postgres tables: `dim_store(store_id, store_name, city)`, `dim_store_district(city, district, store_id)`, `live_stock_now(store_id, product_id, on_hand, reserved_qty, available, reorder_point, updated_at)` key `(store_id, product_id)`, `live_oos_demand_minute(minute_start, store_id, product_id, utm_campaign, views, add_to_cart, landing_clicks)` key `(minute_start, store_id, product_id, utm_campaign)` where `utm_campaign` is `''` when the event has none.
  - `compute_stock_rows(spark, silver_root, pairs, now) -> list[dict]`, `compute_demand_rows(spark, silver_root, minutes) -> list[dict]`, `write_oos_demand(store, rows) -> int`.
  - `LiveSink.on_batch(spark, batch_id, event_minutes, arrival_minutes=(), quarantine_minutes=(), now=None, stock_pairs=())`.

- [ ] **Step 1: Write the failing Spark test for the two computes**

`tests/test_live_stock_spark.py`:

```python
"""Spark tests (Docker, RUN_SPARK_TESTS=1) for stock now and out-of-stock demand."""
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.spark

from streaming.silver.spark_schemas import SCHEMAS, coerce_row  # noqa: E402
from streaming.silver.writer import PARTITION_COLS, SILVER_KEYS, merge_insert, silver_path  # noqa: E402
from test_g1_live_spark import T0, ev, write_events  # noqa: E402
from warehouse import live_stock  # noqa: E402

NOW = datetime(2026, 10, 8, 13, 0, tzinfo=timezone.utc)
K = ("store_a", "prod_1")


def write(spark, lake, table, rows):
    df = spark.createDataFrame([tuple(coerce_row(table, r).values()) for r in rows], SCHEMAS[table])
    merge_insert(spark, silver_path(lake, table), df, SILVER_KEYS[table], PARTITION_COLS.get(table))


def snap(i, key, on_hand, minute, reserved=0, reorder=10):
    t = T0 + timedelta(minutes=minute)
    return {"event_id": "s%03d" % i, "store_id": key[0], "product_id": key[1], "on_hand": on_hand,
            "reserved_qty": reserved, "reorder_point": reorder, "event_time": t, "kafka_timestamp": t,
            "event_date": t.date()}


def move(i, key, delta, minute, reason="sold"):
    t = T0 + timedelta(minutes=minute)
    return {"event_id": "m%03d" % i, "store_id": key[0], "product_id": key[1], "qty_delta": delta, "reason": reason,
            "order_id": "ord_%d" % i if reason == "sold" else None, "requested_qty": -delta if reason == "sold" else None,
            "event_time": t, "kafka_timestamp": t, "event_date": t.date()}


def by_key(rows):
    return {(r["store_id"], r["product_id"]): r for r in rows}


def test_stock_now_is_the_latest_snapshot_plus_the_movements_after_it(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0), snap(2, K, 30, 10)])
    write(spark, lake, "silver_stock_movements", [move(1, K, -5, 3), move(2, K, -4, 12), move(3, K, 60, 14, "received")])
    [row] = live_stock.compute_stock_rows(spark, lake, [K], NOW)
    # latest snapshot (minute 10) says 30; only the movements after minute 10 count: -4 and +60
    assert (row["on_hand"], row["reserved_qty"], row["available"], row["reorder_point"]) == (86, 0, 86, 10)
    assert row["updated_at"] == NOW


def test_a_late_movement_older_than_the_snapshot_and_one_at_the_same_time_do_not_count(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 30, 10)])
    write(spark, lake, "silver_stock_movements", [move(1, K, -7, 5), move(2, K, -9, 10), move(3, K, -2, 11)])
    [row] = live_stock.compute_stock_rows(spark, lake, [K], NOW)
    assert row["on_hand"] == 28  # only the movement at minute 11


def test_two_snapshots_at_the_same_time_pick_the_greater_event_id(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 10), snap(2, K, 33, 10)])
    [row] = live_stock.compute_stock_rows(spark, lake, [K], NOW)
    assert row["on_hand"] == 33


def test_available_is_never_negative_and_reserved_is_subtracted(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 4, 0, reserved=1), snap(2, ("store_a", "prod_2"), 10, 0, reserved=3)])
    write(spark, lake, "silver_stock_movements", [move(1, K, -6, 1)])
    rows = by_key(live_stock.compute_stock_rows(spark, lake, [K, ("store_a", "prod_2")], NOW))
    assert (rows[K]["on_hand"], rows[K]["available"]) == (-2, 0)
    assert rows[("store_a", "prod_2")]["available"] == 7


def test_only_the_asked_pairs_are_computed_and_a_pair_without_a_snapshot_is_skipped(spark, tmp_path):
    lake = str(tmp_path / "lake")
    other = ("store_b", "prod_1")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0), snap(2, other, 50, 0)])
    assert list(by_key(live_stock.compute_stock_rows(spark, lake, [K], NOW))) == [K]
    assert live_stock.compute_stock_rows(spark, lake, [("store_c", "prod_9")], NOW) == []
    assert live_stock.compute_stock_rows(spark, lake, [], NOW) == []


def test_without_the_stock_tables_nothing_is_computed(spark, tmp_path):
    assert live_stock.compute_stock_rows(spark, str(tmp_path / "empty"), [K], NOW) == []


def test_demand_is_counted_per_minute_store_product_and_campaign(spark, tmp_path):
    lake = str(tmp_path / "lake")
    rows = [dict(ev(1, 0, "EG", "product_view"), product_id="prod_1", city="Cairo", district="Maadi", utm_campaign="eid_sale_1"),
            dict(ev(2, 0, "EG", "product_view"), product_id="prod_1", city="Cairo", district="Zamalek", utm_campaign=None),
            dict(ev(3, 0, "EG", "add_to_cart"), product_id="prod_1", city="Cairo", district="Maadi", utm_campaign=None),
            dict(ev(4, 0, "EG", "product_view"), product_id="prod_1", city="Assiut", district="Assiut First"),  # not served
            dict(ev(5, 0, "EG", "page_view"), product_id=None, city="Cairo", district="Maadi")]  # not a product event
    write_events(spark, lake, rows)
    out = live_stock.compute_demand_rows(spark, lake, [T0])
    got = {(r["store_id"], r["product_id"], r["utm_campaign"]): (r["views"], r["add_to_cart"], r["landing_clicks"]) for r in out}
    assert got == {("store_cairo_maadi", "prod_1", "eid_sale_1"): (1, 0, 1),
                   ("store_cairo_maadi", "prod_1", ""): (1, 1, 0)}
    assert all(r["minute_start"] == T0 for r in out)
```

- [ ] **Step 2: Run to verify it fails**

Run: `docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests/test_live_stock_spark.py -q -p no:cacheprovider`
Expected: collection error (`cannot import name 'live_stock'`).

- [ ] **Step 3: Implement `warehouse/live_stock.py`**

```python
"""Live stock and out-of-stock demand (inventory design, sections 7 and 8).

Two Spark computes (no database) and one Postgres writer. The computes run in the live sink's thread pool beside the
live-metrics and health computes; the writer runs on the sink's single connection.
"""
from datetime import datetime, timezone

from pyspark.sql import Window, functions as F

from contracts.stores import district_rows
from streaming.gold.live import MAX_MINUTE_LIST, minute_epochs
from warehouse.live_common import _read, _utc

DEMAND_KEYS = ["minute_start", "store_id", "product_id", "utm_campaign"]


def compute_stock_rows(spark, silver_root, pairs, now):
    """live_stock_now rows for the (store_id, product_id) pairs: the latest snapshot plus the movements strictly after it.

    A pair without any snapshot is skipped (it is repaired when its first snapshot arrives). available is never
    negative; on_hand keeps the raw value so a stock error stays visible."""
    pairs = sorted(set(pairs))
    if not pairs:
        return []
    snaps = _read(spark, silver_root, "silver_stock_snapshots")
    if snaps is None:
        return []
    moves = _read(spark, silver_root, "silver_stock_movements")
    keys = F.broadcast(spark.createDataFrame(pairs, "store_id string, product_id string"))
    cols = ["store_id", "product_id"]
    window = Window.partitionBy(*cols).orderBy(F.col("event_time").desc(), F.col("event_id").desc())
    latest = (snaps.join(keys, cols, "left_semi")
              .withColumn("_rn", F.row_number().over(window)).where("_rn = 1")
              .select(*cols, "on_hand", "reserved_qty", "reorder_point", F.col("event_time").alias("snap_time")))
    if moves is None:
        agg = latest.withColumn("delta", F.lit(0))
    else:
        m = moves.join(keys, cols, "left_semi").select(*cols, "qty_delta", "event_time")
        after = F.col("event_time") > F.col("snap_time")
        agg = (latest.join(m, cols, "left")
               .groupBy(*cols, "on_hand", "reserved_qty", "reorder_point")
               .agg(F.coalesce(F.sum(F.when(after, F.col("qty_delta"))), F.lit(0)).alias("delta")))
    out = []
    for r in agg.collect():
        on_hand = int(r["on_hand"]) + int(r["delta"])
        reserved = int(r["reserved_qty"] or 0)
        out.append(dict(store_id=r["store_id"], product_id=r["product_id"], on_hand=on_hand, reserved_qty=reserved,
                        available=max(on_hand - reserved, 0), reorder_point=int(r["reorder_point"] or 0),
                        updated_at=now))
    return out


def compute_demand_rows(spark, silver_root, minutes):
    """Product views and add-to-carts per (minute, store, product, campaign) for the event-time `minutes`.

    The store comes from the customer's city and district through the one store map. A landing click is a product
    view that carries a utm_campaign. Events of a place without a store are left out. Bots are not removed (v1)."""
    epochs = minute_epochs(minutes)
    if not epochs:
        return []
    events = _read(spark, silver_root, "silver_events")
    if events is None:
        return []
    mapping = spark.createDataFrame([(r["city"], r["district"], r["store_id"]) for r in district_rows()],
                                    "city string, district string, store_id string")
    sec = F.unix_timestamp("event_time")
    minute = sec - (sec % 60)
    if len(epochs) <= MAX_MINUTE_LIST:
        in_window = minute.isin(epochs)
    else:
        in_window = (sec >= epochs[0]) & (sec < epochs[-1] + 60)
    is_view, is_cart = F.col("event_type") == "product_view", F.col("event_type") == "add_to_cart"
    df = (events.where((is_view | is_cart) & F.col("product_id").isNotNull() & in_window)
          .join(F.broadcast(mapping), ["city", "district"], "inner")
          .groupBy(minute.alias("m"), "store_id", "product_id", F.coalesce(F.col("utm_campaign"), F.lit("")).alias("utm"))
          .agg(F.sum(is_view.cast("int")).alias("views"), F.sum(is_cart.cast("int")).alias("carts"),
               F.sum((is_view & F.col("utm_campaign").isNotNull()).cast("int")).alias("clicks")))
    return [dict(minute_start=_utc(r["m"]), store_id=r["store_id"], product_id=r["product_id"], utm_campaign=r["utm"],
                 views=int(r["views"] or 0), add_to_cart=int(r["carts"] or 0), landing_clicks=int(r["clicks"] or 0))
            for r in df.collect()]


def write_oos_demand(store, rows):
    """Upsert the demand rows of the products that are out of stock right now (point in time of the sink)."""
    if not rows:
        return 0
    out_of_stock = {(a, b) for a, b in store.query("SELECT store_id, product_id FROM live_stock_now WHERE available <= 0")}
    keep = [r for r in rows if (r["store_id"], r["product_id"]) in out_of_stock]
    return store.upsert("live_oos_demand_minute", keep, DEMAND_KEYS) if keep else 0
```

- [ ] **Step 4: Run the Spark test to verify it passes**

Run the same command as Step 2. Expected: 7 passed. (`warehouse/live_common.py` is created in Step 7; create it first if the import fails here.)

- [ ] **Step 5: Write the failing host test for the DDL, the specs, the seeding and the sink wiring**

`tests/test_live_stock_sink.py`:

```python
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from scripts import apply_live_ddl
from warehouse import live_store
from warehouse.live_tables import LIVE_TABLE_NAMES, LIVE_TABLES
from test_g1_live_store import MemStore  # records upserts by primary key, records every call

NOW = datetime(2026, 10, 10, 12, 0, 5, tzinfo=timezone.utc)
DDL = (Path(__file__).parent.parent / "warehouse" / "live_ddl.sql").read_text(encoding="utf-8")
NEW = ["dim_store", "dim_store_district", "live_stock_now", "live_oos_demand_minute"]


class StockStore(MemStore):
    """MemStore that also answers the out-of-stock query of write_oos_demand."""

    def __init__(self, oos=(), **kw):
        super().__init__(**kw)
        self.oos = list(oos)

    def query(self, sql, params=None, retry=True):
        if self.fail:
            raise self.fail
        if "live_stock_now" in sql:
            return list(self.oos)
        return []


def test_the_new_tables_are_specified_created_and_granted():
    assert set(NEW) <= set(LIVE_TABLE_NAMES) and len(LIVE_TABLES) == len(LIVE_TABLE_NAMES) == 8
    for name in NEW:
        assert re.search(r"CREATE TABLE IF NOT EXISTS %s \(" % name, DDL), name
        assert name in DDL.split("GRANT SELECT ON")[1].split("TO grafana_ro")[0], name
    spec = {t.name: t for t in LIVE_TABLES}
    assert spec["live_stock_now"].keys == ("store_id", "product_id")
    assert spec["live_oos_demand_minute"].keys == ("minute_start", "store_id", "product_id", "utm_campaign")


def test_apply_live_ddl_seeds_the_store_dimensions():
    store = StockStore()
    apply_live_ddl.main(["--role", "grafana_scratch"], env={"GRAFANA_DB_PASSWORD": "x"}, store=store)
    assert len(store.tables["dim_store"]) == 8
    assert len(store.tables["dim_store_district"]) == 37  # Cairo 15, Giza 8, Alexandria 10, Mansoura 2, Tanta 2
    again = {k: dict(v) for k, v in store.tables["dim_store"].items()}
    apply_live_ddl.main(["--role", "grafana_scratch"], env={"GRAFANA_DB_PASSWORD": "x"}, store=store)
    assert store.tables["dim_store"] == again  # running it twice changes nothing


def stub(monkeypatch, stock_rows=None, demand_rows=None, fail_stock=None):
    calls = {"stock": [], "demand": []}

    def fake_metrics(spark, root, minutes, bot_lookback_hours=6):
        return []

    def fake_health(spark, root, minutes):
        return {m: dict(events=0, late_events=0, quarantined=0) for m in live_store.minute_epochs(minutes)}

    def fake_stock(spark, root, pairs, now):
        calls["stock"].append(sorted(pairs))
        if fail_stock:
            raise fail_stock
        return stock_rows or []

    def fake_demand(spark, root, minutes):
        calls["demand"].append(sorted(minutes))
        return demand_rows or []

    monkeypatch.setattr(live_store, "compute_live_metric_rows", fake_metrics)
    monkeypatch.setattr(live_store, "compute_health_counts", fake_health)
    monkeypatch.setattr(live_store, "compute_stock_rows", fake_stock)
    monkeypatch.setattr(live_store, "compute_demand_rows", fake_demand)
    return calls


K = ("store_a", "prod_1")
STOCK = [dict(store_id="store_a", product_id="prod_1", on_hand=0, reserved_qty=0, available=0, reorder_point=10,
              updated_at=NOW)]
DEMAND = [dict(minute_start=NOW, store_id="store_a", product_id="prod_1", utm_campaign="eid_sale_1", views=3, add_to_cart=2,
               landing_clicks=1),
          dict(minute_start=NOW, store_id="store_a", product_id="prod_2", utm_campaign="", views=9, add_to_cart=1,
               landing_clicks=0)]


def test_stock_rows_are_written_before_the_demand_rows_and_only_out_of_stock_demand_is_kept(monkeypatch):
    stub(monkeypatch, STOCK, DEMAND)
    store = StockStore(oos=[K])
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1")
    assert sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW, stock_pairs=[K]) is not None
    tables = [c[0] for c in store.calls]
    assert tables.index("live_stock_now") < tables.index("live_oos_demand_minute")
    [(key, row)] = store.tables["live_oos_demand_minute"].items()
    assert key[2] == "prod_1" and (row["views"], row["add_to_cart"], row["landing_clicks"]) == (3, 2, 1)  # prod_2 is in stock


def test_without_stock_events_the_inventory_work_is_skipped(monkeypatch):
    calls = stub(monkeypatch, STOCK, DEMAND)
    store = StockStore()
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1")
    assert sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW) is not None
    assert calls == {"stock": [], "demand": []}
    assert "live_stock_now" not in store.tables


def test_a_failing_stock_compute_returns_none_and_keeps_the_pairs_pending(monkeypatch, caplog):
    calls = stub(monkeypatch, STOCK, DEMAND, fail_stock=RuntimeError("read failed"))
    store = StockStore(oos=[K])
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1", retry_after=0.0)
    with caplog.at_level(logging.WARNING, logger="funnelstream.live_store"):
        assert sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW, stock_pairs=[K]) is None  # never raises
    assert "not written to Postgres" in caplog.text
    first_pairs = calls["stock"][0]
    first_calls = len(calls["stock"])
    stub(monkeypatch, STOCK, DEMAND)  # the problem is gone
    assert sink.on_batch(None, 2, [], [], [], now=NOW) is not None
    assert "live_stock_now" in store.tables and first_pairs == [K] and first_calls == 1
```

- [ ] **Step 6: Run to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_live_stock_sink.py -q`
Expected: failures (specs, DDL and sink arguments missing).

- [ ] **Step 7: Implement the tables, seeding and sink wiring**

`warehouse/live_tables.py`: add

```python
_DIM_STORE = "store_id:string store_name:string city:string"
_DIM_STORE_DISTRICT = "city:string district:string store_id:string"
_STOCK_NOW = ("store_id:string product_id:string on_hand:int reserved_qty:int available:int reorder_point:int "
              "updated_at:timestamp")
_OOS_DEMAND = ("minute_start:timestamp store_id:string product_id:string utm_campaign:string views:bigint "
               "add_to_cart:bigint landing_clicks:bigint")
```

and append to `LIVE_TABLES`:

```python
    Table("dim_store", "gold", _parse(_DIM_STORE, ["store_id"]), ("store_id",)),
    Table("dim_store_district", "gold", _parse(_DIM_STORE_DISTRICT, ["city", "district"]), ("city", "district")),
    Table("live_stock_now", "gold", _parse(_STOCK_NOW, ["store_id", "product_id"]), ("store_id", "product_id")),
    Table("live_oos_demand_minute", "gold",
          _parse(_OOS_DEMAND, ["minute_start", "store_id", "product_id", "utm_campaign"]),
          ("minute_start", "store_id", "product_id", "utm_campaign")),
```

`warehouse/live_ddl.sql`: add before the `-- BEGIN grafana_ro` line

```sql
CREATE TABLE IF NOT EXISTS dim_store (
    store_id    text PRIMARY KEY,
    store_name  text NOT NULL,
    city        text NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_store_district (
    city      text NOT NULL,
    district  text NOT NULL,
    store_id  text NOT NULL,
    PRIMARY KEY (city, district)
);

CREATE TABLE IF NOT EXISTS live_stock_now (          -- one row per store and product, recomputed from Silver (simulated stock)
    store_id       text        NOT NULL,
    product_id     text        NOT NULL,
    on_hand        integer     NOT NULL,
    reserved_qty   integer     NOT NULL,
    available      integer     NOT NULL,             -- greatest(on_hand - reserved_qty, 0)
    reorder_point  integer     NOT NULL,
    updated_at     timestamptz NOT NULL,
    PRIMARY KEY (store_id, product_id)
);

CREATE TABLE IF NOT EXISTS live_oos_demand_minute (  -- demand on products that were out of stock when the sink wrote it
    minute_start   timestamptz NOT NULL,
    store_id       text        NOT NULL,
    product_id     text        NOT NULL,
    utm_campaign   text        NOT NULL DEFAULT '',  -- '' when the event has no campaign
    views          bigint      NOT NULL DEFAULT 0,
    add_to_cart    bigint      NOT NULL DEFAULT 0,
    landing_clicks bigint      NOT NULL DEFAULT 0,   -- product views that carry a utm_campaign
    PRIMARY KEY (minute_start, store_id, product_id, utm_campaign)
);
```

and replace the GRANT line with

```sql
GRANT SELECT ON live_metrics_minute, pipeline_health_minute, pipeline_batch_log, pipeline_heartbeat,
    dim_store, dim_store_district, live_stock_now, live_oos_demand_minute TO grafana_ro;
```

`scripts/apply_live_ddl.py`: import `from contracts.stores import district_rows, store_rows`, add

```python
def seed_store_dims(store):
    """The store map is data: written from the one Python source every time, so the SQL joins and the simulator agree."""
    store.upsert("dim_store", store_rows(), ["store_id"])
    store.upsert("dim_store_district", district_rows(), ["city", "district"])
```

and call `seed_store_dims(store)` in `main` right after `store.execute(sql)`.

First create `warehouse/live_common.py` and MOVE `_utc` and `_read` into it unchanged (with their imports: `datetime`, `timezone`, `DeltaTable`, `silver_path`). `warehouse/live_store.py` imports them back (`from warehouse.live_common import _read, _utc`), so the names `live_store._read` and `live_store._utc` still exist and the existing tests that monkeypatch `live_store._read` keep working; `warehouse/live_stock.py` imports from `live_common`, which avoids a circular import. Then add to `live_store.py`: `from warehouse.live_stock import compute_demand_rows, compute_stock_rows, write_oos_demand`.

In `LiveSink.__init__` add `self._pending_stock, self._stock_seen = set(), False`. Change the `on_batch` signature to add `stock_pairs=()` and, after the two `_pending_*` updates, add

```python
        self._pending_stock |= {tuple(p) for p in stock_pairs}
        self._stock_seen = self._stock_seen or bool(self._pending_stock)
```

In the `try` block, extend the thread pool: after `health_job = ...` add (inside the same `with`)

```python
                stock_job = (pool.submit(in_job_group(compute_stock_rows), spark, self.silver_root,
                                         sorted(stock_pairs_now), now) if stock_pairs_now else None)
                demand_job = (pool.submit(in_job_group(compute_demand_rows), spark, self.silver_root, metric_minutes)
                              if self._stock_seen else None)
```

with `stock_pairs_now = set(self._pending_stock)` copied next to `metric_minutes`. After `metric_rows, health_counts = ...` add

```python
            stock_rows = stock_job.result() if stock_job else []
            demand_rows = demand_job.result() if demand_job else []
```

and after `write_health(...)` and before the heartbeat:

```python
            if stock_rows:
                store.upsert("live_stock_now", stock_rows, ["store_id", "product_id"])
            write_oos_demand(store, demand_rows)
```

After a successful batch clear `self._pending_stock = set()` together with the other pending sets. The returned `(n_metrics, n_health)` and the log line stay unchanged.

`streaming/jobs/silver_clean.py`: in `handle`, pass the pairs to the sink:

```python
            sink.on_batch(sess, batch_id, stats.get("event_minutes", ()), stats.get("arrival_minutes", ()),
                          stats.get("quarantine_minutes", ()), stock_pairs=stats.get("stock_pairs", ()))
```

- [ ] **Step 8: Run all affected tests**

Host: `.venv\Scripts\python -m pytest -q` (expected: the new file passes; `tests/test_g1_live_store.py` may pin the number of live tables or compare the DDL file with the specs: update the expected counts, and if the DDL comparison fails, align DDL column names and types with the specs).
Spark, one file at a time: `tests/test_live_stock_spark.py` (7), `tests/test_g1_live_spark.py` (10), `tests/test_live_sink_parallel_spark.py` (4), `tests/test_silver_parallel_spark.py` (4).
Expected: all PASS.

- [ ] **Step 9: Apply the DDL to the running Postgres**

Run: `docker compose run --rm spark python scripts/apply_live_ddl.py`
Expected: 8 `table ... ready` lines and exit 0. Then `"SELECT count(*) FROM dim_store;" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'` prints 8.

- [ ] **Step 10: Commit** (only when told to commit)

```bash
git add warehouse scripts streaming tests
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Add live stock and out-of-stock demand tables and the sink recompute"
```

---

### Task 8: Grafana operations dashboard and the out-of-stock alert

**Files:**
- Create: `monitoring/dashboards.py`, `scripts/gen_dashboards.py`, `monitoring/grafana/dashboards/inventory-ops.json` (generated), `tests/test_inventory_dashboards.py`
- Modify: `monitoring/grafana/provisioning/alerting/alerting.yaml`, `tests/test_grafana_provisioning.py`

**Interfaces:**
- Consumes: Task 7 tables.
- Produces: `monitoring.dashboards` helpers `stat(...)`, `timeseries(...)`, `table(...)`, `text(...)`, `dashboard(...)`, `build_inventory_ops() -> dict`; the dashboard uid `fs-inventory-ops`; the alert rule `fs-oos-demand`.

- [ ] **Step 1: Write the failing test**

`tests/test_inventory_dashboards.py`:

```python
import json
import re
from pathlib import Path

from monitoring.dashboards import build_inventory_ops

ROOT = Path(__file__).parent.parent
DASH = ROOT / "monitoring" / "grafana" / "dashboards" / "inventory-ops.json"
ALLOWED = {"live_stock_now", "live_oos_demand_minute", "live_metrics_minute", "dim_store", "dim_store_district"}


def sqls(d):
    return [t["rawSql"] for p in d["panels"] for t in p.get("targets", [])]


def test_the_file_is_what_the_builder_produces():
    assert json.loads(DASH.read_text(encoding="utf-8")) == build_inventory_ops()


def test_seven_data_panels_one_note_and_unique_ids():
    d = build_inventory_ops()
    ids = [p["id"] for p in d["panels"]]
    assert len(ids) == len(set(ids)) and d["uid"] == "fs-inventory-ops"
    assert sorted(p["type"] for p in d["panels"] if p["type"] != "text") == \
        ["stat", "stat", "stat", "table", "table", "timeseries", "timeseries"]
    note = next(p for p in d["panels"] if p["type"] == "text")
    assert "simulated" in note["options"]["content"].lower()


def test_every_panel_uses_the_postgres_datasource_and_only_the_live_tables():
    d = build_inventory_ops()
    for p in d["panels"]:
        if p["type"] == "text":
            continue
        assert p["datasource"]["uid"] == "fs-postgres"
    for sql in sqls(d):
        tables = {m.lower() for m in re.findall(r"(?:from|join)\s+([a-z_]+)", sql, re.I)}
        assert tables and tables <= ALLOWED, (sql, tables)


def test_the_stat_thresholds_follow_the_design():
    d = build_inventory_ops()
    steps = {p["title"]: [s["value"] for s in p["fieldConfig"]["defaults"]["thresholds"]["steps"]]
             for p in d["panels"] if p["type"] == "stat"}
    assert steps["Out-of-stock products now"] == [None, 1, 5]
    assert steps["Seconds since the last stock change"] == [None, 30, 60]


def test_time_series_queries_use_the_grafana_time_macros():
    for p in build_inventory_ops()["panels"]:
        if p["type"] == "timeseries":
            assert "$__timeFilter(" in p["targets"][0]["rawSql"] and "$__timeGroupAlias(" in p["targets"][0]["rawSql"]
```

Append to `tests/test_grafana_provisioning.py` `test_alert_rules`: extend `CONTRACT_TABLES` with `"live_stock_now", "live_oos_demand_minute", "dim_store", "dim_store_district"`, change the set of expected rule uids to include `fs-oos-demand`, and the count to 5, and add

```python
    oos = rules["fs-oos-demand"]
    assert threshold(oos) == ("gt", 0) and oos["noDataState"] == "OK"
    assert "interval '3 minutes'" in sql(oos) and "add_to_cart >= 5" in sql(oos) and "count(*) >= 2" in sql(oos)
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_dashboards.py tests\test_grafana_provisioning.py -q`
Expected: collection error (`monitoring.dashboards` missing) and the alert assertions failing.

- [ ] **Step 3: Implement the builder**

`monitoring/dashboards.py` (also create `monitoring/__init__.py` if the package import fails):

```python
"""Builders for the provisioned Grafana dashboards. Plain functions returning dicts, so the JSON files are generated
(scripts/gen_dashboards.py) and a test fails when a file drifts from its builder."""

DS = {"type": "grafana-postgresql-datasource", "uid": "fs-postgres"}


def _target(sql, fmt):
    return [{"refId": "A", "datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True, "rawSql": sql}]


def _pos(x, y, w, h):
    return {"x": x, "y": y, "w": w, "h": h}


def stat(pid, title, desc, sql, pos, steps, unit="short", decimals=0):
    return {"id": pid, "type": "stat", "title": title, "description": desc, "datasource": DS, "gridPos": _pos(*pos),
            "targets": _target(sql, "table"),
            "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "thresholds"},
                                         "thresholds": {"mode": "absolute", "steps": steps},
                                         "decimals": decimals, "noValue": "no data"}, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "background", "graphMode": "none"}}


def timeseries(pid, title, desc, sql, pos, unit="short"):
    return {"id": pid, "type": "timeseries", "title": title, "description": desc, "datasource": DS,
            "gridPos": _pos(*pos), "targets": _target(sql, "time_series"),
            "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "palette-classic"},
                                         "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10,
                                                    "showPoints": "never", "spanNulls": False}, "min": 0},
                            "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}}


def table(pid, title, desc, sql, pos):
    return {"id": pid, "type": "table", "title": title, "description": desc, "datasource": DS, "gridPos": _pos(*pos),
            "targets": _target(sql, "table"), "fieldConfig": {"defaults": {}, "overrides": []},
            "options": {"showHeader": True}}


def text(pid, title, markdown, pos):
    return {"id": pid, "type": "text", "title": title, "gridPos": _pos(*pos),
            "options": {"mode": "markdown", "content": markdown}}


def dashboard(uid, title, panels):
    return {"uid": uid, "title": title, "tags": ["funnelstream"], "timezone": "Africa/Cairo", "refresh": "5s",
            "time": {"from": "now-30m", "to": "now"}, "schemaVersion": 39, "editable": True, "graphTooltip": 1,
            "templating": {"list": []}, "annotations": {"list": []}, "links": [], "panels": panels}


def _steps(*pairs):
    colors = ["green", "orange", "red"]
    return [{"color": colors[i], "value": v} for i, v in enumerate(pairs)]


def build_inventory_ops():
    panels = [
        stat(1, "Out-of-stock products now",
             "Store and product pairs with available stock of zero (simulated stock, latest sink write).",
             "SELECT count(*) AS \"out of stock\" FROM live_stock_now WHERE available <= 0",
             (0, 0, 6, 5), _steps(None, 1, 5)),
        stat(2, "Low-stock products now",
             "Pairs with stock above zero but below their reorder point.",
             "SELECT count(*) AS \"low stock\" FROM live_stock_now WHERE available > 0 AND available < reorder_point",
             (6, 0, 6, 5), _steps(None, 10, 30)),
        stat(3, "Seconds since the last stock change",
             "Age of the newest stock row. Stock is recomputed after each Silver batch, so it is about one batch behind.",
             "SELECT coalesce(round(extract(epoch from (now() - max(updated_at))))::int, 0) AS \"seconds\" FROM live_stock_now",
             (12, 0, 6, 5), _steps(None, 30, 60), unit="s"),
        text(4, "About this data",
             "All stock numbers are **simulated**: the simulator sells and restocks eight dark stores. "
             "Available stock is on-hand stock; order reservations are out of scope.", (18, 0, 6, 5)),
        timeseries(5, "Demand on out-of-stock products per minute",
                   "Product views and add-to-carts that arrived for a store and product that was out of stock. "
                   "This is demand the shelf could not serve.",
                   "SELECT $__timeGroupAlias(minute_start, '1m', 0), sum(views) AS \"views\", "
                   "sum(add_to_cart) AS \"add to cart\" FROM live_oos_demand_minute WHERE $__timeFilter(minute_start) "
                   "GROUP BY 1 ORDER BY 1", (0, 5, 12, 8)),
        timeseries(6, "App traffic: events per minute",
                   "Non-late, non-bot events per minute from live_metrics_minute (all countries).",
                   "SELECT $__timeGroupAlias(minute_start, '1m', 0), sum(events) AS \"events\" FROM live_metrics_minute "
                   "WHERE $__timeFilter(minute_start) GROUP BY 1 ORDER BY 1", (12, 5, 12, 8)),
        table(7, "Lowest stock now (20 products)",
              "The twenty store and product pairs with the least available stock.",
              "SELECT s.store_name AS \"store\", l.product_id AS \"product\", l.available AS \"available\", "
              "l.reorder_point AS \"reorder point\" FROM live_stock_now l JOIN dim_store s ON s.store_id = l.store_id "
              "ORDER BY l.available ASC, l.store_id, l.product_id LIMIT 20", (0, 13, 12, 9)),
        table(8, "Lost demand now (last 15 minutes)",
              "Products with the most add-to-carts while out of stock in the last 15 minutes.",
              "SELECT s.store_name AS \"store\", d.product_id AS \"product\", sum(d.add_to_cart) AS \"add to cart\", "
              "sum(d.views) AS \"views\" FROM live_oos_demand_minute d JOIN dim_store s ON s.store_id = d.store_id "
              "WHERE d.minute_start >= now() - interval '15 minutes' GROUP BY 1, 2 ORDER BY 3 DESC, 4 DESC LIMIT 10",
              (12, 13, 12, 9)),
    ]
    return dashboard("fs-inventory-ops", "FunnelStream: Operations, live inventory", panels)
```

`scripts/gen_dashboards.py`:

```python
"""Write the provisioned dashboard files from monitoring/dashboards.py. Run: python scripts/gen_dashboards.py"""
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from monitoring import dashboards

OUT = os.path.join(_ROOT, "monitoring", "grafana", "dashboards")
FILES = {"inventory-ops.json": dashboards.build_inventory_ops}


def main():
    for name, build in FILES.items():
        path = os.path.join(OUT, name)
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(build(), fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        print("wrote", path)


if __name__ == "__main__":
    main()
```

Run `.venv\Scripts\python scripts\gen_dashboards.py` to create `inventory-ops.json`.

- [ ] **Step 4: Add the alert rule**

In `monitoring/grafana/provisioning/alerting/alerting.yaml` add a fifth rule to the `rules:` list (same shape as `fs-silver-silent`, after it):

```yaml
      - uid: fs-oos-demand
        title: Demand on out-of-stock products
        condition: C
        for: 0s
        noDataState: OK
        execErrState: Error
        labels:
          severity: warning
        annotations:
          summary: "At least 5 add-to-carts a minute on an out-of-stock product, two minutes in a row"
          runbook: "Open the dashboard 'Operations, live inventory', table 'Lost demand now': restock the listed store and product, or ask marketing to pause the campaign that sends traffic to it."
        data:
          - refId: A
            relativeTimeRange: {from: 600, to: 0}
            datasourceUid: fs-postgres
            model:
              refId: A
              datasource: {type: postgres, uid: fs-postgres}
              editorMode: code
              format: time_series
              rawQuery: true
              rawSql: >-
                SELECT now() AS time, count(*) AS products
                FROM (
                  SELECT store_id, product_id
                  FROM live_oos_demand_minute
                  WHERE minute_start >= now() - interval '3 minutes' AND add_to_cart >= 5
                  GROUP BY store_id, product_id
                  HAVING count(*) >= 2
                ) t
          - refId: B
            datasourceUid: __expr__
            relativeTimeRange: {from: 600, to: 0}
            model:
              refId: B
              type: reduce
              expression: A
              reducer: last
              datasource: {type: __expr__, uid: __expr__}
          - refId: C
            datasourceUid: __expr__
            relativeTimeRange: {from: 600, to: 0}
            model:
              refId: C
              type: threshold
              expression: B
              conditions:
                - evaluator: {type: gt, params: [0]}
              datasource: {type: __expr__, uid: __expr__}
```

Also update the header comment of the file: the list of rules now has five.

- [ ] **Step 5: Run the tests**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_dashboards.py tests\test_grafana_provisioning.py -q`, then the full host suite.
Expected: PASS.

- [ ] **Step 6: Load it into Grafana and check by eye**

Run: `docker compose up -d --force-recreate grafana`, wait 25 s, then `curl -s -u admin:admin http://localhost:3000/api/search?query=FunnelStream` (expect both dashboards) and `curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules` (expect five titles). Open `http://localhost:3000` and confirm the dashboard "FunnelStream: Operations, live inventory" has no red error triangle on any panel (they show "no data" until events flow).

- [ ] **Step 7: Commit** (only when told to commit)

```bash
git add monitoring scripts/gen_dashboards.py tests
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Add the live inventory dashboard and the out-of-stock demand alert"
```

---

### Task 9: Marketing D-1 mart and its Grafana page (stretch: cut first)

**Files:**
- Create: `warehouse/d1_marketing.py`, `scripts/build_d1_marketing.py`, `tests/test_d1_marketing.py`, `tests/test_d1_marketing_spark.py`, `monitoring/grafana/dashboards/marketing-d1.json` (generated)
- Modify: `warehouse/live_ddl.sql`, `warehouse/live_tables.py`, `monitoring/dashboards.py`, `scripts/gen_dashboards.py`, `tests/test_live_stock_sink.py` (table count), `tests/test_inventory_dashboards.py`

**Interfaces:**
- Consumes: Task 7 (`live_oos_demand_minute` with `utm_campaign` and `landing_clicks`), the Databricks `agg_campaign_daily`, the local Silver `silver_events` and `silver_products`.
- Produces:
  - `exposed_spend(spend, oos_clicks, clicks) -> Decimal | None`.
  - `assemble_campaign_rows(report_date, today, campaigns, spend_rows, clicks_by_utm, oos_by_utm) -> list[dict]`.
  - `assemble_product_rows(report_date, oos_carts_by_product, price_by_product, limit=5) -> list[dict]`.
  - Postgres tables `marketing_d1_campaign` (key `report_date, campaign_id`) and `marketing_d1_product` (key `report_date, product_id`).
  - `scripts/build_d1_marketing.py [--date YYYY-MM-DD]` (default: yesterday in Cairo).

- [ ] **Step 1: Write the failing host test**

`tests/test_d1_marketing.py`:

```python
from datetime import date
from decimal import Decimal

from warehouse.d1_marketing import assemble_campaign_rows, assemble_product_rows, exposed_spend

D = date(2026, 10, 9)
CAMPAIGNS = [{"campaign_id": "camp_201", "utm_campaign": "eid_sale_1"}, {"campaign_id": "camp_202", "utm_campaign": "ramadan_offer_1"}]
SPEND = [dict(campaign_id="camp_201", spend=Decimal("1000.0000"), clicks=500, orders_placed=20,
              revenue_placed=Decimal("5000.00"), revenue_delivered=Decimal("4000.00"), cpa=Decimal("50.00"),
              roas_placed=Decimal("5.00"), roas_delivered=Decimal("4.00"))]


def test_exposed_spend_is_spend_times_the_out_of_stock_click_share():
    assert exposed_spend(Decimal("1000"), 30, 120) == Decimal("250.00")
    assert exposed_spend(Decimal("1000.0000"), 1, 3) == Decimal("333.33")
    assert exposed_spend(Decimal("1000"), 0, 120) == Decimal("0.00")


def test_exposed_spend_is_none_without_clicks_or_spend():
    assert exposed_spend(Decimal("1000"), 5, 0) is None
    assert exposed_spend(None, 5, 10) is None


def test_the_campaign_row_joins_spend_with_the_click_counts_through_the_utm_name():
    rows = assemble_campaign_rows(D, date(2026, 10, 10), CAMPAIGNS, SPEND, {"eid_sale_1": 120}, {"eid_sale_1": 30})
    [row] = rows  # camp_202 has no spend row: it is not reported
    assert row["campaign_id"] == "camp_201" and row["utm_campaign"] == "eid_sale_1"
    assert (row["landing_clicks"], row["oos_clicks"]) == (120, 30)
    assert row["oos_click_share"] == Decimal("0.2500")
    assert row["exposed_spend"] == Decimal("250.00")
    assert row["spend"] == Decimal("1000.00") and row["roas_delivered"] == Decimal("4.00")
    assert row["report_date"] == D


def test_a_recent_day_is_provisional_and_an_old_one_is_not():
    recent = assemble_campaign_rows(D, date(2026, 10, 10), CAMPAIGNS, SPEND, {"eid_sale_1": 10}, {})[0]
    old = assemble_campaign_rows(D, date(2026, 10, 14), CAMPAIGNS, SPEND, {"eid_sale_1": 10}, {})[0]
    assert recent["provisional"] is True and old["provisional"] is False


def test_a_campaign_without_clicks_has_no_share_and_no_exposed_spend():
    [row] = assemble_campaign_rows(D, date(2026, 10, 10), CAMPAIGNS, SPEND, {}, {})
    assert row["landing_clicks"] == 0 and row["oos_click_share"] is None and row["exposed_spend"] is None


def test_the_top_products_by_revenue_at_risk():
    rows = assemble_product_rows(D, {"p1": 10, "p2": 3, "p3": 8, "p4": 1, "p5": 2, "p6": 4},
                                 {"p1": Decimal("100.00"), "p2": Decimal("900.00"), "p3": Decimal("50.00"),
                                  "p4": Decimal("10.00"), "p5": Decimal("5.00")})  # p6 has no price: skipped
    assert [r["product_id"] for r in rows] == ["p2", "p1", "p3", "p4", "p5"]
    assert rows[0]["oos_add_to_carts"] == 3 and rows[0]["revenue_at_risk"] == Decimal("2700.00")
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_d1_marketing.py -q`
Expected: collection error (`warehouse.d1_marketing` missing).

- [ ] **Step 3: Implement the pure part**

`warehouse/d1_marketing.py`:

```python
"""Marketing D-1 mart (inventory design, section 9).

The pure functions turn yesterday's numbers into the rows of two Postgres tables. The three readers (Databricks spend,
local Silver clicks, Postgres out-of-stock demand) are thin and live below. The metric is an ESTIMATE: spend is only
known per campaign and day, so it is spread over clicks, and a customer who met an empty shelf may have bought a
substitute. It is titled "estimated spend exposed to out-of-stock" everywhere.
"""
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

PROVISIONAL_DAYS = 3  # ROAS delivered matures one to three days after the order day
_CENT, _SHARE = Decimal("0.01"), Decimal("0.0001")


def exposed_spend(spend, oos_clicks, clicks):
    """spend * oos_clicks / clicks to two decimals; None when there are no clicks or no spend."""
    if spend is None or not clicks:
        return None
    return (Decimal(str(spend)) * oos_clicks / clicks).quantize(_CENT, rounding=ROUND_HALF_UP)


def assemble_campaign_rows(report_date, today, campaigns, spend_rows, clicks_by_utm, oos_by_utm):
    """One row per campaign that has a spend row for the day (campaigns: dicts with campaign_id and utm_campaign)."""
    utm_of = {c["campaign_id"]: c["utm_campaign"] for c in campaigns}
    provisional = (today - report_date) < timedelta(days=PROVISIONAL_DAYS)
    out = []
    for s in spend_rows:
        utm = utm_of.get(s["campaign_id"], "")
        clicks, oos = int(clicks_by_utm.get(utm, 0)), int(oos_by_utm.get(utm, 0))
        share = (Decimal(oos) / Decimal(clicks)).quantize(_SHARE, rounding=ROUND_HALF_UP) if clicks else None
        out.append(dict(report_date=report_date, campaign_id=s["campaign_id"], utm_campaign=utm,
                        spend=None if s["spend"] is None else Decimal(str(s["spend"])).quantize(_CENT),
                        ad_clicks=s["clicks"], orders_placed=s["orders_placed"], revenue_placed=s["revenue_placed"],
                        revenue_delivered=s["revenue_delivered"], cpa=s["cpa"], roas_placed=s["roas_placed"],
                        roas_delivered=s["roas_delivered"], landing_clicks=clicks, oos_clicks=oos,
                        oos_click_share=share, exposed_spend=exposed_spend(s["spend"], oos, clicks),
                        provisional=provisional))
    return out


def assemble_product_rows(report_date, oos_carts_by_product, price_by_product, limit=5):
    """The `limit` products with the most revenue at risk (add-to-carts on empty stock times price)."""
    rows = [dict(report_date=report_date, product_id=pid, oos_add_to_carts=int(n),
                 revenue_at_risk=(Decimal(str(price_by_product[pid])) * int(n)).quantize(_CENT))
            for pid, n in oos_carts_by_product.items() if pid in price_by_product]
    rows.sort(key=lambda r: (-r["revenue_at_risk"], r["product_id"]))
    return rows[:limit]
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv\Scripts\python -m pytest tests\test_d1_marketing.py -q`
Expected: 7 passed.

- [ ] **Step 5: Tables, readers and the script**

`warehouse/live_tables.py`: add

```python
_D1_CAMPAIGN = ("report_date:date campaign_id:string utm_campaign:string spend:decimal(12,2)? ad_clicks:bigint? "
                "orders_placed:bigint? revenue_placed:decimal(12,2)? revenue_delivered:decimal(12,2)? cpa:decimal(12,2)? "
                "roas_placed:decimal(12,2)? roas_delivered:decimal(12,2)? landing_clicks:bigint oos_clicks:bigint "
                "oos_click_share:decimal(8,4)? exposed_spend:decimal(12,2)? provisional:boolean")
_D1_PRODUCT = "report_date:date product_id:string oos_add_to_carts:bigint revenue_at_risk:decimal(12,2)"
```

and the two `Table(...)` entries (keys `("report_date", "campaign_id")` and `("report_date", "product_id")`). In `warehouse/live_ddl.sql` add the matching `CREATE TABLE IF NOT EXISTS marketing_d1_campaign (...)` and `marketing_d1_product (...)` with the same column names, `numeric(12,2)` / `numeric(8,4)` for the decimals, `date` for `report_date`, and primary keys as above, and add both names to the GRANT list. Update the table count (`10`) in `tests/test_live_stock_sink.py`.

Append the readers to `warehouse/d1_marketing.py`:

```python
def read_spend_rows(dbx_store, report_date):
    """D-1 rows of agg_campaign_daily (last touch) from Databricks."""
    sql = ("SELECT campaign_id, spend, clicks, orders_placed, revenue_placed, revenue_delivered, cpa, roas_placed, "
           "roas_delivered FROM %s WHERE report_date = DATE'%s' AND attribution_model = 'last_touch'"
           % (dbx_store.qualified("agg_campaign_daily"), report_date.isoformat()))
    cols = ["campaign_id", "spend", "clicks", "orders_placed", "revenue_placed", "revenue_delivered", "cpa", "roas_placed",
            "roas_delivered"]
    return [dict(zip(cols, r)) for r in dbx_store.query(sql)]


def read_landing_clicks(spark, silver_root, report_date):
    """{utm_campaign: landing clicks} of the Cairo day, from local Silver (product views that carry a campaign)."""
    from pyspark.sql import functions as F
    from warehouse.live_common import _read
    events = _read(spark, silver_root, "silver_events")
    if events is None:
        return {}
    rows = (events.where((F.col("event_date") == F.lit(report_date.isoformat()).cast("date"))
                         & (F.col("event_type") == "product_view") & F.col("utm_campaign").isNotNull())
            .groupBy("utm_campaign").count().collect())
    return {r["utm_campaign"]: int(r["count"]) for r in rows}


def read_oos_demand(pg_store, report_date):
    """(oos landing clicks by utm_campaign, oos add-to-carts by product) of the Cairo day from live_oos_demand_minute."""
    day = "(minute_start AT TIME ZONE 'Africa/Cairo')::date = %s"
    clicks = {u: int(n) for u, n in pg_store.query(
        "SELECT utm_campaign, sum(landing_clicks) FROM live_oos_demand_minute WHERE " + day + " AND utm_campaign <> '' "
        "GROUP BY utm_campaign", (report_date,))}
    carts = {p: int(n) for p, n in pg_store.query(
        "SELECT product_id, sum(add_to_cart) FROM live_oos_demand_minute WHERE " + day + " GROUP BY product_id",
        (report_date,))}
    return clicks, carts


def read_prices(spark, silver_root):
    """{product_id: price} from local silver_products (latest version per product)."""
    from pyspark.sql import Window, functions as F
    from warehouse.live_common import _read
    products = _read(spark, silver_root, "silver_products")
    if products is None:
        return {}
    w = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc())
    rows = products.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").select("product_id", "price").collect()
    return {r["product_id"]: Decimal(str(r["price"])) for r in rows}
```

`scripts/build_d1_marketing.py`:

```python
"""Build the marketing D-1 mart in Postgres for one Cairo day (default: yesterday).

Needs Databricks settings (the spend) and PG_DSN: run it in the spark container:
  docker compose run --rm spark python scripts/build_d1_marketing.py [--date 2026-10-09]
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from simulator.campaigns import CAMPAIGNS
from streaming.common.spark import get_spark
from warehouse import d1_marketing as d1
from warehouse.store import get_store


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", help="Cairo day to report, YYYY-MM-DD (default yesterday)")
    ap.add_argument("--lake", default="data/lake")
    args = ap.parse_args(argv)
    today = datetime.now(ZoneInfo("Africa/Cairo")).date()
    report_date = date.fromisoformat(args.date) if args.date else today - timedelta(days=1)
    lake = os.path.abspath(args.lake)
    dbx, pg = get_store("databricks"), get_store("postgres")
    try:
        spark = get_spark("d1_marketing")
        spend = d1.read_spend_rows(dbx, report_date)
        clicks = d1.read_landing_clicks(spark, lake, report_date)
        oos_clicks, oos_carts = d1.read_oos_demand(pg, report_date)
        campaign_rows = d1.assemble_campaign_rows(report_date, today, CAMPAIGNS, spend, clicks, oos_clicks)
        product_rows = d1.assemble_product_rows(report_date, oos_carts, d1.read_prices(spark, lake))
        pg.upsert("marketing_d1_campaign", campaign_rows, ["report_date", "campaign_id"])
        pg.upsert("marketing_d1_product", product_rows, ["report_date", "product_id"])
    finally:
        dbx.close()
        pg.close()
    print("D-1 %s: %d campaign rows, %d product rows" % (report_date, len(campaign_rows), len(product_rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Spark test for the Silver reader**

`tests/test_d1_marketing_spark.py`:

```python
from datetime import date

import pytest

pytestmark = pytest.mark.spark

from test_g1_live_spark import ev, write_events  # noqa: E402
from warehouse import d1_marketing as d1  # noqa: E402


def test_landing_clicks_count_product_views_with_a_campaign_on_the_cairo_day(spark, tmp_path):
    lake = str(tmp_path / "lake")
    rows = [dict(ev(1, 0, "EG", "product_view"), product_id="prod_1", utm_campaign="eid_sale_1"),
            dict(ev(2, 1, "EG", "product_view"), product_id="prod_2", utm_campaign="eid_sale_1"),
            dict(ev(3, 1, "EG", "product_view"), product_id="prod_2", utm_campaign=None),
            dict(ev(4, 2, "EG", "add_to_cart"), product_id="prod_2", utm_campaign="eid_sale_1"),
            dict(ev(5, 2, "EG", "product_view"), product_id="prod_3", utm_campaign="ramadan_offer_1")]
    write_events(spark, lake, rows)
    day = rows[0]["event_date"]
    assert d1.read_landing_clicks(spark, lake, day) == {"eid_sale_1": 2, "ramadan_offer_1": 1}
    assert d1.read_landing_clicks(spark, lake, date(2020, 1, 1)) == {}
    assert d1.read_landing_clicks(spark, str(tmp_path / "empty"), day) == {}
```

Run: `docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests/test_d1_marketing_spark.py -q -p no:cacheprovider`
Expected: PASS.

- [ ] **Step 7: The Grafana page**

In `monitoring/dashboards.py` add `build_marketing_d1()` returning `dashboard("fs-marketing-d1", "FunnelStream: Marketing, D-1", panels)` with these panels (all read the latest `report_date` in `marketing_d1_campaign`):
1. stat "Spend (EGP)": `SELECT coalesce(sum(spend), 0) AS "spend" FROM marketing_d1_campaign WHERE report_date = (SELECT max(report_date) FROM marketing_d1_campaign)`
2. stat "Orders placed": same with `sum(orders_placed)`.
3. stat "ROAS placed": `SELECT coalesce(sum(revenue_placed) / nullif(sum(spend), 0), 0) AS "roas placed" ...`
4. stat "ROAS delivered (provisional for recent days)": same with `revenue_delivered`.
5. stat "Estimated spend exposed to out-of-stock (EGP)": `sum(exposed_spend)`.
6. stat "Out-of-stock click share": `coalesce(sum(oos_clicks)::numeric / nullif(sum(landing_clicks), 0), 0)` with `unit="percentunit"`.
7. table "Campaigns, sorted by exposed spend": `SELECT campaign_id, utm_campaign, spend, orders_placed, cpa, roas_placed, roas_delivered, landing_clicks, oos_clicks, oos_click_share, exposed_spend, provisional FROM marketing_d1_campaign WHERE report_date = (SELECT max(report_date) FROM marketing_d1_campaign) ORDER BY exposed_spend DESC NULLS LAST`
8. table "Top 5 products by revenue at risk": `SELECT product_id, oos_add_to_carts, revenue_at_risk FROM marketing_d1_product WHERE report_date = (SELECT max(report_date) FROM marketing_d1_product) ORDER BY revenue_at_risk DESC`
9. text note: "Simulated data. Exposed spend = spend x share of landing clicks that met an out-of-stock product; it is an estimate and an upper bound (a customer may buy a substitute). Recent days are provisional: ROAS delivered matures after 1 to 3 days."

Add `"marketing-d1.json": dashboards.build_marketing_d1` to `FILES` in `scripts/gen_dashboards.py`, run it, and extend `tests/test_inventory_dashboards.py` with the same drift, datasource and table-allow-list checks for the marketing dashboard (allowed tables: `marketing_d1_campaign`, `marketing_d1_product`; the sub-select `FROM marketing_d1_campaign` inside the query is found by the same regex).

- [ ] **Step 8: Run everything and try it on the real data**

Host suite and the two new Spark files. Then, with the real stack and a day of data: `docker compose run --rm spark python scripts/apply_live_ddl.py`, `docker compose run --rm spark python scripts/build_d1_marketing.py --date <a day that has data>`. Expected: `D-1 <date>: N campaign rows, M product rows` and rows visible in the Grafana page. (The Databricks read needs the real workspace; if it is unreachable, report that, do not fake rows.)

- [ ] **Step 9: Commit** (only when told to commit)

```bash
git add warehouse scripts monitoring tests
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Add the marketing D-1 mart and its Grafana page"
```

---

### Task 10: Documentation and the rehearsal

**Files:**
- Modify: `docs/RUNNING.md`, `README.md`, `docs/HANDOFF.md`, `docs/FunnelStream-Sprint-Plan-v3.xlsx` (statuses of INV rows)

**Interfaces:**
- Consumes: everything above.
- Produces: the written procedure of the inventory demo and a recorded rehearsal result.

- [ ] **Step 1: Document**

Add to `docs/RUNNING.md` a section "Live inventory demo" with: what it shows (simulated stock, two users), the commands (`.\scripts\watch_demo.ps1 -Rate 10 -SyncEvery 0 -Inventory`, then open `http://localhost:3000` dashboards "Operations, live inventory" and "Marketing, D-1"), the expected timeline of the hero scenario (opening snapshots in the first batch, a hero product runs out after roughly one to two minutes at 10 events per second, the alert `Demand on out-of-stock products` fires about two to three minutes later, a `received` movement follows 90 seconds after the stock-out and the panels recover), the D-1 command, and the plain statement that the inventory is simulated and reservations are out of scope. Add the same two paragraphs (what it is, how to start) to `README.md`. Update `docs/HANDOFF.md`: mark the inventory work as done or not done per task, with the real numbers measured in Step 2.

- [ ] **Step 2: Rehearse once, end to end, and record real numbers**

Start the stack as in the failure demo procedure of `docs/RUNNING.md` with `-Inventory`. Wait until `.\scripts\failure_demo.ps1 status` is healthy. Record, with timestamps from the windows: when the first `live_stock_now` rows appeared, the minute a hero product reached `available = 0`, when the dashboard panels moved, when `fs-oos-demand` fired and resolved (alert sink lines), and the Silver batch time during the run (`pipeline_batch_log` gaps). Write the measured values into `docs/HANDOFF.md`. If any step did not happen as described, stop and report; do not edit the document to match the plan.

- [ ] **Step 3: Check the failure demo still holds with inventory on**

Run the G3 steps from `docs/RUNNING.md` ("Failure demo (G3)") once with `-Inventory` on: kill, wait for the heartbeat alert, restart, then `.\scripts\failure_demo.ps1 verify`. Extend `scripts/check_duplicates.py` first if it lists its tables in a hard-coded list: add `silver_stock_movements` (key `event_id`) and `silver_stock_snapshots` (key `event_id`), with a test in `tests/test_check_duplicates.py` that the new tables are checked. Expected: `NO DUPLICATES` for all six tables after the restart.

- [ ] **Step 4: Update the sprint sheet and commit** (only when told to commit)

Set the status of INV0 to Done and of INV1 to INV5 (and INV6 if built) to Done with `data/update_sprint_v3.py`-style edits, then:

```bash
git add docs README.md scripts tests
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Document the inventory demo and record the rehearsal"
```

---

## Self-review

1. **Spec coverage.** Purpose and success criteria: Tasks 7, 8, 9, 10. Contracts (section 4): Tasks 1, 2. Store map (section 5): Task 3. Simulator model and scenario (section 6): Tasks 5, 6. Silver and live serving (section 7): Tasks 2, 4, 7. Operations dashboard and alert (section 8): Task 8. Marketing D-1 (section 9): Task 9. Testing (section 10): every task is test first; Spark tests per file. Order and risks (section 11): the header order and Task 9 as the cut candidate. Assumptions (section 12): written into Global Constraints and the Task 7 definitions. Not covered on purpose: the Databricks mirror and `agg_oos_demand_daily` (spec says only if time remains).
2. **One deliberate change against the spec.** The spec computes the D-1 out-of-stock clicks from "hourly stock state". This plan reuses the out-of-stock demand the live sink already records minute by minute (`landing_clicks` and `utm_campaign` columns on `live_oos_demand_minute`), which needs no extra Spark logic and keeps the live and D-1 numbers identical. The cost: the D-1 number is point in time at sink writing time, accurate to about one Silver batch, which the dashboards state. Spec section 12 already allows an approximate D-1.
3. **Placeholders.** None: every step has code or an exact command. The only value that depends on running code is `DEFAULT_HERO_UNITS`, with a test that fails if it is wrong and a rule for fixing it (lower the constant, never the test).
4. **Type and name consistency.** `StockLedger` methods `opening/order/due/finish` are used identically in `iter_batch` (Task 5) and `add_inventory` (Task 6). `compute_stock_rows(spark, root, pairs, now)` and `compute_demand_rows(spark, root, minutes)` match between `live_stock.py`, the stubs in `test_live_stock_sink.py` and `LiveSink`. `DEMAND_KEYS` equals the table key in `live_tables.py` and the DDL. `stats["stock_pairs"]` (Task 2) is what `silver_clean.handle` passes to `on_batch(stock_pairs=...)` (Task 7).
5. **Review Focus.** All five lines have a test: replay (Task 2 Spark, Task 7), late movement (Task 7), unserved customers (Tasks 3 and 5), oversold clamp (Task 5), failing compute (Task 7).
