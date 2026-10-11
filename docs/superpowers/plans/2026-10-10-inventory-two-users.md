# Live inventory for operations and D-1 marketing: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add simulated dark-store inventory to FunnelStream so one platform serves two users: an operations user who watches live traffic and real stock in Grafana, and a marketing user who reads yesterday's campaign numbers including the estimated ad spend exposed to out-of-stock products.

**Architecture:** Two new event types (`stock_snapshot`, `stock_movement`) travel on a new Kafka topic `inventory.events` through the existing Bronze and Silver path (two new Silver tables). The live sink recomputes `live_stock_now` for the touched store and product pairs and upserts `live_oos_demand_minute` (all served demand per minute, flagged `oos_at_write` with the stock when the minute was written) in Postgres. Grafana reads those tables for an operations dashboard and one new alert. A daily job rolls the Silver stock tables and the minute demand table into two Postgres marts (`inventory_daily`, `oos_demand_daily`) that a second Grafana dashboard and Power BI both read. A daily job joins the Databricks `agg_campaign_daily` spend with the out-of-stock and served landing clicks of `live_oos_demand_minute` into a Postgres mart that a second Grafana dashboard shows. `dim_product_current` gives Grafana tables product names.

**Tech Stack:** Python 3.11, PySpark 3.5 with Delta (in Docker), Kafka (confluent-kafka), Postgres 16 (psycopg 3), Grafana 11.3 provisioning, Power BI Desktop (reads the same Postgres marts), pytest.

**Spec:** `docs/superpowers/specs/2026-10-10-inventory-two-users-design.md` (read it first; this plan implements it).

## Global Constraints

- Defense date is 13 October 2026. The marketing D-1 mart (Task 9B) is the first thing to cut if time runs out; Tasks 1 to 8, 9A and 10 are the core. Task 9A (the daily inventory mart, the Grafana inventory-insights page and the tables Power BI reads) is core because the product owner asked for inventory insights in both Grafana and Power BI.
- The inventory is simulated and is labeled as simulated wherever a number is shown (dashboard descriptions, README).
- Eight dark stores exactly: Cairo 3 (Nasr City, Maadi, New Cairo), Giza 1, Alexandria 2, Mansoura 1, Tanta 1. Assiut and "Other" are "not served" and excluded from out-of-stock metrics.
- Stock event envelope: `event_id, event_type, event_version (1), event_time, store_id, product_id, payload`. A flat event without `payload` is quarantined.
- `available = on_hand - reserved_qty`; `reserved_qty` is 0 by assumption. Out of stock means `available <= 0`; low stock means `0 < available < reorder_point`.
- No noise injection on inventory events. Inventory is opt-in (`--inventory`, `--scenario hero-stockout`); with the flags off the simulator stream and every existing count must stay byte for byte identical.
- Stock now is a full recompute per touched `(store_id, product_id)`: the latest snapshot (by `event_time`, ties by `event_id`) plus the sum of `qty_delta` of movements with `event_time` strictly after that snapshot. A replay or a late event never double counts.
- The marketing metric is titled "estimated spend exposed to out-of-stock". `exposed_spend = spend x oos_clicks / served_clicks`, both counts from the same table (`live_oos_demand_minute` / `oos_demand_daily`), served districts only; it is an estimate that assumes spend is spread evenly over served landing clicks (never called an upper bound).
- Day rule: ad spend and ad clicks are the ad API's UTC day; orders and demand are the Cairo day; `agg_campaign_daily.report_date` is the ad day; the D-1 job takes yesterday in Cairo for the demand and the ad row with the same date value; the mismatch (at most 3 hours) is accepted and written down.
- `live_oos_demand_minute` keeps ALL served demand per minute and a flag `oos_at_write` ("stock when the minute was written"); the flag is not replay-safe and every dashboard and document says so. `campaign_id` (resolved from `utm_campaign`; `'direct'` when none) is the campaign key; `utm_campaign` is only an attribute.
- `LIVE_TABLES` (Postgres, `warehouse/live_tables.py`) has 9 entries after Task 7 (the four health tables, `dim_store`, `dim_store_district`, `dim_product_current`, `live_stock_now`, `live_oos_demand_minute`), 10 after Task 8B, 13 after Task 9A and 15 after Task 9B. This counts `dim_product_current` (Ruling 3 of the ledger said 8, 9, 12 before that table was added).
- Known limitations, written in the spec and in MODEL.md: express KPIs exist only in `express_daily` and `live_express_orders` and the Gold delivery metrics include express orders; there is no stock reversal on cancellation (reserve-on-order); Store is conformed across the Postgres marts only (Gold has City).
- The live-inventory section is an **express** service with a hard **20-minute** delivery promise (`promised_minutes`, default 20, carried by every express order). Normal express orders take 8 to 17.5 minutes and never more than 19.5 in the simulator; the platform monitors the promise live and alerts on ANY breach (Task 8B). Express timing is never time-compressed in live mode.
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
| INV6 D-1 mart and page | 9B |
| INV7 daily inventory mart, Grafana insights page, Power BI tables (added 10 Oct evening) | 9A |
| INV8 express delivery and the 20-minute promise (added 10 Oct night) | 6B, 8B, and the express table in 9A |
| SMK end-to-end smoke test of the whole project (added 10 Oct night) | 11 |
| documentation and rehearsal | 10 |

Order: 1, 3 (independent), 2, 4, 5, 6, 6B, 7, 8, 8B, 9A, 9B, 10, then 11 (the end-to-end smoke test of the whole project). Tasks 1 and 3 can run in parallel.

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

### Task 6B: Express orders and the 20-minute promise (contract, Silver table, simulator)

The live-inventory section of the app is an **express** service: an order placed from it carries a promise of delivery within **20 minutes**, and the promise must never be exceeded. The platform cannot enforce delivery, so it does three things: the simulator makes a normal express order take 8 to 17.5 minutes (never more than 19.5); the pipeline records every express order with its own promise; and Task 8B watches the promise live and alerts on any breach.

**Files:**
- Modify: `contracts/validate.py`, `streaming/silver/schemas.py`, `streaming/silver/spark_schemas.py`, `streaming/silver/writer.py`, `streaming/silver/transform.py`, `simulator/orders.py`, `scripts/run_simulator.py`
- Create: `tests/test_express_contract.py`, `tests/test_express_orders.py`, `tests/test_express_silver_spark.py`

**Interfaces:**
- Consumes: Task 1 (validator), Task 2 (transform and Silver table pattern), Task 3 (`store_for`), Task 5 (`StockLedger`, `iter_batch`), Task 6 (runner).
- Produces:
  - `order_created.payload.service` (`"express"` or `"standard"`, default standard) and `order_created.payload.promised_minutes` (integer 1 to 60, required when the service is express).
  - Silver table `silver_express_orders(order_id, store_id, promised_minutes, created_at, event_date)`, key `order_id`, partition `event_date`. One row per express order, written by the transform of its `order_created` event; `store_id` comes from `store_for(city, district)`.
  - `make_express_order(rng, customer, catalog, campaign, t, order_id, first_item=None, promised_minutes=20, late_minutes=None) -> list[(topic, event)]`: the same event kinds and topics as `make_order`, in minutes.
  - Runner flags `--express-promise-minutes N` (default 20) and `--late-express-every K` (default 0: never).

Behaviour to implement, test first (write each test, watch it fail for the right reason, then implement):

- **Contract.** `service` outside `express` or `standard` is `BAD_VALUE`; an express order without an integer `promised_minutes` between 1 and 60 is `BAD_VALUE` (a boolean is not an integer); a standard order may omit both. An order event without these fields (every existing event) still validates exactly as before.
- **Silver.** An express `order_created` produces the usual rows (`silver_orders`, `silver_order_status`, `silver_order_items`) plus one `silver_express_orders` row; a standard one produces no express row. An express order whose city and district have no store (`store_for` is None) is quarantined as `BAD_VALUE` with the detail "express order outside a served district". Replaying the batch changes nothing (key `order_id`). Add the table to `TABLE_COLUMNS`, the Spark schemas, `SILVER_KEYS` and `PARTITION_COLS`, and update the count-pinned tests as in Task 2.
- **Simulator, normal case.** With the ledger on (`--inventory`), an order of a customer whose district has a store is an express order made by `make_express_order`; every other order stays a standard `make_order` order. Timeline in minutes after `order_created`: payment authorised (non-COD only) after 10 to 40 seconds, packed after 2 to 4 minutes, shipped about 1 minute later, out for delivery 30 to 60 seconds after that, **delivered after 8 to 17.5 minutes in total, never above `promised_minutes - 0.5`**, completed 30 seconds after delivery. No cancellations, no failed attempts, no returns. The sale lines still reach the stock ledger (Task 5/6).
- **Simulator, late case.** With `late_minutes` set (the runner passes it for every K-th express order when `--late-express-every K` is given) the order is delivered after `late_minutes` (default `promised_minutes + 1` to `promised_minutes + 8`) instead, so a breach exists to be detected. Without the flag no order is ever late: a test generates 5,000 events with the ledger on and asserts that the maximum delivered-minus-created time of every express order is below 19.5 minutes.
- **Live mode keeps real minutes.** `compress_orders`, `compress_sessions`' order shift and `cap_order_tail` must not rescale express orders (their timeline is already in minutes); standard orders compress as before. A rehearsal that needs a faster clock passes `--express-promise-minutes 2`: the generator then scales the express timeline by `promised_minutes / 20` and carries the shorter promise in the event, so the SLA logic (which always reads the order's own promise) behaves the same. Test: with promise 2 the maximum express duration is below 1.9 minutes and the event carries `promised_minutes == 2`.
- **Nothing changes when the flags are off.** The digest test of Task 5 (`test_the_stream_without_inventory_is_unchanged`) still passes, and a stream with `--inventory` but no express-capable customer is identical to the Task 5 stream.

Run the host suite, the new Spark file and the existing Silver Spark files one at a time. Commit only when told to commit: `Add express orders and the 20-minute promise to the contract, Silver and the simulator`.

---

### Task 7: Live Postgres tables and the sink recompute

**Files:**
- Create: `warehouse/live_stock.py`, `tests/test_live_stock_spark.py`, `tests/test_live_stock_sink.py`
- Modify: `warehouse/live_ddl.sql`, `warehouse/live_tables.py`, `warehouse/live_store.py`, `scripts/apply_live_ddl.py`, `streaming/jobs/silver_clean.py`

**Interfaces:**
- Consumes: Task 2 (Silver stock tables, `stats["stock_pairs"]`), Task 3 (`store_rows`, `district_rows`).
- Produces:
  - Postgres tables: `dim_store(store_id, store_name, city)`, `dim_store_district(city, district, store_id)`, `dim_product_current(product_id, name, category, price)` (the latest version of every product, so Grafana tables show names), `live_stock_now(store_id, product_id, on_hand, reserved_qty, available, reorder_point, updated_at)` key `(store_id, product_id)`, `live_oos_demand_minute(minute_start, store_id, product_id, campaign_id, utm_campaign, views, add_to_cart, landing_clicks, oos_at_write)` key `(minute_start, store_id, product_id, campaign_id)`.
  - `live_oos_demand_minute` stores ALL served demand of a minute: there is no write-time out-of-stock filter. `campaign_id` is resolved from `utm_campaign` through the static campaign list (`simulator.campaigns.CAMPAIGNS`): `'direct'` when the event has no campaign, `'unknown'` for a utm_campaign that is not in the list; `utm_campaign` is kept only as an attribute (`''` when none). `oos_at_write` is true when the pair's available stock was zero when the sink wrote the minute ("stock when the minute was written"). The sink upserts the touched minutes. **Limitation, stated on every dashboard and doc:** this flag is not replay-safe; a replay, a late event or a rewrite after a restock can flag a minute differently, and a stock-out that starts mid-minute credits the whole minute.
  - `compute_stock_rows(spark, silver_root, pairs, now) -> list[dict]`, `compute_demand_rows(spark, silver_root, minutes) -> list[dict]` (all served demand, without the flag), `write_oos_demand(store, rows) -> int` (sets `oos_at_write` from `live_stock_now`, upserts every row), `compute_product_rows(spark, silver_root) -> list[dict]` (latest `silver_products` version per product).
  - `LiveSink.on_batch(spark, batch_id, event_minutes, arrival_minutes=(), quarantine_minutes=(), now=None, stock_pairs=())`.

- [ ] **Step 1: Write the failing Spark test for the two computes**

`tests/test_live_stock_spark.py`:

```python
"""Spark tests (Docker, RUN_SPARK_TESTS=1) for stock now, served demand and the product dimension."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

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


def test_demand_is_counted_per_minute_store_product_and_campaign_for_every_served_event(spark, tmp_path):
    lake = str(tmp_path / "lake")
    rows = [dict(ev(1, 0, "EG", "product_view"), product_id="prod_1", city="Cairo", district="Maadi", utm_campaign="eid_sale_1"),
            dict(ev(2, 0, "EG", "product_view"), product_id="prod_1", city="Cairo", district="Zamalek", utm_campaign=None),
            dict(ev(3, 0, "EG", "add_to_cart"), product_id="prod_1", city="Cairo", district="Maadi", utm_campaign=None),
            dict(ev(4, 0, "EG", "product_view"), product_id="prod_1", city="Assiut", district="Assiut First"),  # not served
            dict(ev(5, 0, "EG", "page_view"), product_id=None, city="Cairo", district="Maadi"),  # not a product event
            dict(ev(6, 0, "EG", "product_view"), product_id="prod_2", city="Cairo", district="Maadi",
                 utm_campaign="mystery_1")]  # a utm_campaign that is not in the campaign list
    write_events(spark, lake, rows)
    out = live_stock.compute_demand_rows(spark, lake, [T0])
    got = {(r["store_id"], r["product_id"], r["campaign_id"]): (r["views"], r["add_to_cart"], r["landing_clicks"]) for r in out}
    assert got == {("store_cairo_maadi", "prod_1", "camp_201"): (1, 0, 1),
                   ("store_cairo_maadi", "prod_1", "direct"): (1, 1, 0),
                   ("store_cairo_maadi", "prod_2", "unknown"): (1, 0, 1)}
    utm = {r["campaign_id"]: r["utm_campaign"] for r in out if r["product_id"] == "prod_1"}
    assert utm == {"camp_201": "eid_sale_1", "direct": ""}  # the utm_campaign is only an attribute
    assert all(r["minute_start"] == T0 and "oos_at_write" not in r for r in out)  # the writer sets the flag, not the compute


def test_the_current_product_dimension_keeps_the_latest_version_of_each_product(spark, tmp_path):
    lake = str(tmp_path / "lake")

    def prod(pid, name, price, at, category="dairy"):
        return {"product_id": pid, "updated_at": at, "name": name, "category": category, "price": Decimal(price),
                "is_active": True}

    write(spark, lake, "silver_products", [prod("prod_1", "Milk 1L", "30.00", T0),
                                           prod("prod_1", "Milk 1L Full Fat", "32.50", T0 + timedelta(hours=1)),
                                           prod("prod_2", "Rice 1kg", "40.00", T0, "grocery")])
    rows = {r["product_id"]: r for r in live_stock.compute_product_rows(spark, lake)}
    assert set(rows) == {"prod_1", "prod_2"}
    assert (rows["prod_1"]["name"], rows["prod_1"]["price"], rows["prod_1"]["category"]) == ("Milk 1L Full Fat", Decimal("32.50"), "dairy")
    assert live_stock.compute_product_rows(spark, str(tmp_path / "empty")) == []
```

- [ ] **Step 2: Run to verify it fails**

Run: `docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests/test_live_stock_spark.py -q -p no:cacheprovider`
Expected: collection error (`cannot import name 'live_stock'`).

- [ ] **Step 3: Implement `warehouse/live_stock.py`**

```python
"""Live stock and out-of-stock demand (inventory design, sections 7 and 8).

Three Spark computes (no database: stock now, served demand, current products) and one Postgres writer. The stock and
demand computes run in the live sink's thread pool beside the live-metrics and health computes; the writer runs on the
sink's single connection.
"""
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import Window, functions as F

from contracts.stores import district_rows
from simulator.campaigns import CAMPAIGNS
from streaming.gold.live import MAX_MINUTE_LIST, minute_epochs
from warehouse.live_common import _read, _utc

DEMAND_KEYS = ["minute_start", "store_id", "product_id", "campaign_id"]


def _stock_levels(spark, silver_root, pairs=None, as_of=None):
    """DataFrame (store_id, product_id, on_hand, reserved_qty, reorder_point): the latest snapshot plus the movements
    strictly after it, both taken before `as_of` (exclusive; None means no limit).

    pairs None means every store and product pair. Returns None when no snapshot table exists. A pair without any
    snapshot has no row. Used by the live sink (no as_of) and by the daily inventory mart (as_of = end of the day)."""
    snaps = _read(spark, silver_root, "silver_stock_snapshots")
    if snaps is None:
        return None
    moves = _read(spark, silver_root, "silver_stock_movements")
    cols = ["store_id", "product_id"]
    keys = (F.broadcast(spark.createDataFrame(sorted(set(pairs)), "store_id string, product_id string"))
            if pairs is not None else None)

    def narrow(df):
        if keys is not None:
            df = df.join(keys, cols, "left_semi")
        if as_of is not None:
            df = df.where(F.col("event_time") < F.lit(as_of))
        return df

    window = Window.partitionBy(*cols).orderBy(F.col("event_time").desc(), F.col("event_id").desc())
    latest = (narrow(snaps).withColumn("_rn", F.row_number().over(window)).where("_rn = 1")
              .select(*cols, "on_hand", "reserved_qty", "reorder_point", F.col("event_time").alias("snap_time")))
    if moves is None:
        return latest.select(*cols, "on_hand", "reserved_qty", "reorder_point")
    m = narrow(moves).select(*cols, "qty_delta", "event_time")
    after = F.col("event_time") > F.col("snap_time")
    agg = (latest.join(m, cols, "left")
           .groupBy(*cols, "on_hand", "reserved_qty", "reorder_point")
           .agg(F.coalesce(F.sum(F.when(after, F.col("qty_delta"))), F.lit(0)).alias("delta")))
    return agg.select(*cols, (F.col("on_hand") + F.col("delta")).alias("on_hand"), "reserved_qty", "reorder_point")


def compute_stock_rows(spark, silver_root, pairs, now):
    """live_stock_now rows for the (store_id, product_id) pairs: the latest snapshot plus the movements strictly after it.

    A pair without any snapshot is skipped (it is repaired when its first snapshot arrives). available is never
    negative; on_hand keeps the raw value so a stock error stays visible."""
    pairs = sorted(set(pairs))
    if not pairs:
        return []
    levels = _stock_levels(spark, silver_root, pairs=pairs)
    if levels is None:
        return []
    out = []
    for r in levels.collect():
        on_hand, reserved = int(r["on_hand"]), int(r["reserved_qty"] or 0)
        out.append(dict(store_id=r["store_id"], product_id=r["product_id"], on_hand=on_hand, reserved_qty=reserved,
                        available=max(on_hand - reserved, 0), reorder_point=int(r["reorder_point"] or 0),
                        updated_at=now))
    return out


def compute_demand_rows(spark, silver_root, minutes):
    """Product views and add-to-carts per (minute, store, product, campaign) for the event-time `minutes`.

    ALL demand of served districts is returned, whatever the stock was: there is no out-of-stock filter here, and no
    flag (write_oos_demand sets `oos_at_write`). The store comes from the customer's city and district through the one
    store map. `campaign_id` comes from the utm_campaign through the static campaign list ('direct' when the event has
    none, 'unknown' when the utm_campaign is not listed); `utm_campaign` stays as an attribute. A landing click is a
    product view that carries a utm_campaign. Events of a place without a store are left out. Bots are not removed (v1)."""
    epochs = minute_epochs(minutes)
    if not epochs:
        return []
    events = _read(spark, silver_root, "silver_events")
    if events is None:
        return []
    mapping = spark.createDataFrame([(r["city"], r["district"], r["store_id"]) for r in district_rows()],
                                    "city string, district string, store_id string")
    campaigns = spark.createDataFrame([(c["utm_campaign"], c["campaign_id"]) for c in CAMPAIGNS],
                                      "utm_campaign string, _cid string")
    sec = F.unix_timestamp("event_time")
    minute = sec - (sec % 60)
    if len(epochs) <= MAX_MINUTE_LIST:
        in_window = minute.isin(epochs)
    else:
        in_window = (sec >= epochs[0]) & (sec < epochs[-1] + 60)
    is_view, is_cart = F.col("event_type") == "product_view", F.col("event_type") == "add_to_cart"
    has_utm = F.col("utm_campaign").isNotNull() & (F.col("utm_campaign") != "")
    campaign_id = F.when(~has_utm, F.lit("direct")).otherwise(F.coalesce(F.col("_cid"), F.lit("unknown")))
    df = (events.where((is_view | is_cart) & F.col("product_id").isNotNull() & in_window)
          .join(F.broadcast(mapping), ["city", "district"], "inner")
          .join(F.broadcast(campaigns), ["utm_campaign"], "left")
          .groupBy(minute.alias("m"), "store_id", "product_id", campaign_id.alias("cid"))
          .agg(F.sum(is_view.cast("int")).alias("views"), F.sum(is_cart.cast("int")).alias("carts"),
               F.sum((is_view & has_utm).cast("int")).alias("clicks"),
               F.min(F.coalesce(F.col("utm_campaign"), F.lit(""))).alias("utm")))
    return [dict(minute_start=_utc(r["m"]), store_id=r["store_id"], product_id=r["product_id"], campaign_id=r["cid"],
                 utm_campaign=r["utm"], views=int(r["views"] or 0), add_to_cart=int(r["carts"] or 0),
                 landing_clicks=int(r["clicks"] or 0))
            for r in df.collect()]


def write_oos_demand(store, rows):
    """Upsert the demand rows of the touched minutes, every one of them, each flagged `oos_at_write`.

    The flag is true when the pair had no available stock in live_stock_now at the moment the sink writes: it is the
    stock when the minute was written, not the stock at event time. Nothing is filtered out. Known limitation: the flag
    is not replay-safe; a replay, a late event or a rewrite of the minute after a restock can flag it differently, and a
    stock-out that starts in the middle of a minute credits the whole minute."""
    if not rows:
        return 0
    empty = {(a, b) for a, b in store.query("SELECT store_id, product_id FROM live_stock_now WHERE available <= 0")}
    flagged = [dict(r, oos_at_write=(r["store_id"], r["product_id"]) in empty) for r in rows]
    return store.upsert("live_oos_demand_minute", flagged, DEMAND_KEYS)


def compute_product_rows(spark, silver_root):
    """dim_product_current rows: the latest silver_products version of every product ([] without the table).

    Written by scripts/apply_live_ddl.py next to dim_store so Grafana tables can show names instead of ids."""
    products = _read(spark, silver_root, "silver_products")
    if products is None:
        return []
    latest = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc())
    rows = (products.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1")
            .select("product_id", "name", "category", "price").collect())
    return [dict(product_id=r["product_id"], name=r["name"], category=r["category"],
                 price=None if r["price"] is None else Decimal(str(r["price"]))) for r in rows]
```

- [ ] **Step 4: Run the Spark test to verify it passes**

Run the same command as Step 2. Expected: 8 passed. (`warehouse/live_common.py` is created in Step 7; create it first if the import fails here.)

- [ ] **Step 5: Write the failing host test for the DDL, the specs, the seeding and the sink wiring**

`tests/test_live_stock_sink.py`:

```python
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from scripts import apply_live_ddl
from warehouse import live_store
from warehouse.live_tables import LIVE_TABLE_NAMES, LIVE_TABLES
from test_g1_live_store import MemStore  # records upserts by primary key, records every call

NOW = datetime(2026, 10, 10, 12, 0, 5, tzinfo=timezone.utc)
DDL = (Path(__file__).parent.parent / "warehouse" / "live_ddl.sql").read_text(encoding="utf-8")
NEW = ["dim_store", "dim_store_district", "dim_product_current", "live_stock_now", "live_oos_demand_minute"]


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
    assert set(NEW) <= set(LIVE_TABLE_NAMES) and len(LIVE_TABLES) == len(LIVE_TABLE_NAMES) == 9
    for name in NEW:
        assert re.search(r"CREATE TABLE IF NOT EXISTS %s \(" % name, DDL), name
        assert name in DDL.split("GRANT SELECT ON")[1].split("TO grafana_ro")[0], name
    spec = {t.name: t for t in LIVE_TABLES}
    assert spec["live_stock_now"].keys == ("store_id", "product_id")
    assert spec["live_oos_demand_minute"].keys == ("minute_start", "store_id", "product_id", "campaign_id")
    assert spec["dim_product_current"].keys == ("product_id",)
    flag = [c for c in spec["live_oos_demand_minute"].columns if c.name == "oos_at_write"]
    assert flag and flag[0].type == "boolean" and re.search(r"oos_at_write\s+boolean", DDL)


def test_apply_live_ddl_seeds_the_store_dimensions():
    store = StockStore()
    apply_live_ddl.main(["--role", "grafana_scratch"], env={"GRAFANA_DB_PASSWORD": "x"}, store=store)
    assert len(store.tables["dim_store"]) == 8
    assert len(store.tables["dim_store_district"]) == 37  # Cairo 15, Giza 8, Alexandria 10, Mansoura 2, Tanta 2
    again = {k: dict(v) for k, v in store.tables["dim_store"].items()}
    apply_live_ddl.main(["--role", "grafana_scratch"], env={"GRAFANA_DB_PASSWORD": "x"}, store=store)
    assert store.tables["dim_store"] == again  # running it twice changes nothing


def test_apply_live_ddl_fills_dim_product_current_from_the_given_rows_and_is_idempotent():
    store = StockStore()
    rows = [dict(product_id="p1", name="Milk 1L", category="dairy", price=Decimal("32.50")),
            dict(product_id="p2", name="Rice 1kg", category="grocery", price=None)]
    env = {"GRAFANA_DB_PASSWORD": "x"}
    apply_live_ddl.main(["--role", "grafana_scratch"], env=env, store=store, product_rows=rows)
    assert sorted(r["name"] for r in store.tables["dim_product_current"].values()) == ["Milk 1L", "Rice 1kg"]
    rows[0] = dict(rows[0], name="Milk 1L Full Fat")  # the catalog changed: the same call updates the row
    apply_live_ddl.main(["--role", "grafana_scratch"], env=env, store=store, product_rows=rows)
    assert sorted(r["name"] for r in store.tables["dim_product_current"].values()) == ["Milk 1L Full Fat", "Rice 1kg"]


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
DEMAND = [dict(minute_start=NOW, store_id="store_a", product_id="prod_1", campaign_id="camp_201", utm_campaign="eid_sale_1",
               views=3, add_to_cart=2, landing_clicks=1),
          dict(minute_start=NOW, store_id="store_a", product_id="prod_2", campaign_id="direct", utm_campaign="", views=9,
               add_to_cart=1, landing_clicks=0)]


def test_stock_rows_are_written_before_the_demand_rows_and_all_served_demand_is_kept_with_the_flag(monkeypatch):
    stub(monkeypatch, STOCK, DEMAND)
    store = StockStore(oos=[K])
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1")
    assert sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW, stock_pairs=[K]) is not None
    tables = [c[0] for c in store.calls]
    assert tables.index("live_stock_now") < tables.index("live_oos_demand_minute")
    rows = {key[2]: row for key, row in store.tables["live_oos_demand_minute"].items()}  # key[2] is the product
    assert set(rows) == {"prod_1", "prod_2"}  # nothing is filtered out at write time
    assert rows["prod_1"]["oos_at_write"] is True and rows["prod_2"]["oos_at_write"] is False
    assert (rows["prod_1"]["views"], rows["prod_1"]["add_to_cart"], rows["prod_1"]["landing_clicks"]) == (3, 2, 1)
    assert rows["prod_1"]["campaign_id"] == "camp_201" and rows["prod_2"]["campaign_id"] == "direct"


def test_a_rewritten_minute_takes_its_flag_from_the_stock_of_that_moment(monkeypatch):
    stub(monkeypatch, STOCK, DEMAND)
    store = StockStore(oos=[K])
    sink = live_store.LiveSink("lake", store=store, job_run_id="run-1")
    sink.on_batch(None, 1, [1_000_000_020], [], [], now=NOW, stock_pairs=[K])

    def flags():
        return {key[2]: row["oos_at_write"] for key, row in store.tables["live_oos_demand_minute"].items()}

    assert flags() == {"prod_1": True, "prod_2": False}
    store.oos = []  # restocked, then the same minute is written again (a late event or a replay)
    sink.on_batch(None, 2, [1_000_000_020], [], [], now=NOW, stock_pairs=[K])
    assert flags() == {"prod_1": False, "prod_2": False}  # known limitation: the flag is the stock when written


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
_DIM_PRODUCT_CURRENT = "product_id:string name:string? category:string? price:decimal(10,2)?"
_OOS_DEMAND = ("minute_start:timestamp store_id:string product_id:string campaign_id:string utm_campaign:string "
               "views:bigint add_to_cart:bigint landing_clicks:bigint oos_at_write:boolean")
```

and append to `LIVE_TABLES`:

```python
    Table("dim_store", "gold", _parse(_DIM_STORE, ["store_id"]), ("store_id",)),
    Table("dim_store_district", "gold", _parse(_DIM_STORE_DISTRICT, ["city", "district"]), ("city", "district")),
    Table("dim_product_current", "gold", _parse(_DIM_PRODUCT_CURRENT, ["product_id"]), ("product_id",)),
    Table("live_stock_now", "gold", _parse(_STOCK_NOW, ["store_id", "product_id"]), ("store_id", "product_id")),
    Table("live_oos_demand_minute", "gold",
          _parse(_OOS_DEMAND, ["minute_start", "store_id", "product_id", "campaign_id"]),
          ("minute_start", "store_id", "product_id", "campaign_id")),
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

CREATE TABLE IF NOT EXISTS dim_product_current (     -- latest version of each product (from silver_products), so Grafana shows names
    product_id  text PRIMARY KEY,
    name        text,
    category    text,
    price       numeric(10,2)
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

CREATE TABLE IF NOT EXISTS live_oos_demand_minute (  -- ALL served demand per minute, store, product and campaign
    minute_start   timestamptz NOT NULL,
    store_id       text        NOT NULL,
    product_id     text        NOT NULL,
    campaign_id    text        NOT NULL,             -- from utm_campaign via the campaign list; 'direct' when none, 'unknown' when not listed
    utm_campaign   text        NOT NULL DEFAULT '',  -- attribute only; '' when the event has no campaign
    views          bigint      NOT NULL DEFAULT 0,
    add_to_cart    bigint      NOT NULL DEFAULT 0,
    landing_clicks bigint      NOT NULL DEFAULT 0,   -- product views that carry a utm_campaign
    oos_at_write   boolean     NOT NULL DEFAULT false, -- stock was zero when the sink wrote this minute (not replay-safe)
    PRIMARY KEY (minute_start, store_id, product_id, campaign_id)
);
```

and replace the GRANT line with

```sql
GRANT SELECT ON live_metrics_minute, pipeline_health_minute, pipeline_batch_log, pipeline_heartbeat,
    dim_store, dim_store_district, dim_product_current, live_stock_now, live_oos_demand_minute TO grafana_ro;
```

`scripts/apply_live_ddl.py`: import `from contracts.stores import district_rows, store_rows`, add

```python
def seed_store_dims(store):
    """The store map is data: written from the one Python source every time, so the SQL joins and the simulator agree."""
    store.upsert("dim_store", store_rows(), ["store_id"])
    store.upsert("dim_store_district", district_rows(), ["city", "district"])


def seed_product_dim(store, rows):
    """dim_product_current: the latest silver_products version of each product, filled by the same job as dim_store so
    Grafana tables show names. Run the script again after the catalog changes (seed_silver_reference.py)."""
    if rows:
        store.upsert("dim_product_current", rows, ["product_id"])
    return len(rows)


def read_product_rows(lake):
    """Rows from local Silver through Spark; [] and a warning on stderr when Spark or the table is not available."""
    try:
        from streaming.common.spark import get_spark
        from warehouse.live_stock import compute_product_rows
        return compute_product_rows(get_spark("apply_live_ddl"), lake)
    except Exception as e:  # the DDL must still be applied; the dimension is filled on the next run
        print("warning: dim_product_current not filled (%s)" % type(e).__name__, file=sys.stderr)
        return []
```

Give `main` the signature `main(argv=None, env=None, store=None, product_rows=None)`, add `ap.add_argument("--lake", default="data/lake")`, and call `seed_store_dims(store)` right after `store.execute(sql)`, then
`rows = product_rows if product_rows is not None else (read_product_rows(os.path.abspath(args.lake)) if own else [])` and `seed_product_dim(store, rows)` (tests inject a store, so they never start Spark). Print `dim_product_current: N products` after the `table ... ready` lines.

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
Spark, one file at a time: `tests/test_live_stock_spark.py` (8), `tests/test_g1_live_spark.py` (10), `tests/test_live_sink_parallel_spark.py` (4), `tests/test_silver_parallel_spark.py` (4).
Expected: all PASS.

- [ ] **Step 9: Apply the DDL to the running Postgres**

Run: `docker compose run --rm spark python scripts/apply_live_ddl.py`
Expected: 9 `table ... ready` lines, a line `dim_product_current: N products` (N is about 200 once `scripts/seed_silver_reference.py` has run; with no Silver products yet it prints a warning and the table stays empty) and exit 0. Then `"SELECT count(*) FROM dim_store;" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'` prints 8.

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
ALLOWED = {"live_stock_now", "live_oos_demand_minute", "live_metrics_minute", "dim_store", "dim_store_district",
           "dim_product_current"}


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

Append to `tests/test_grafana_provisioning.py` `test_alert_rules`: extend `CONTRACT_TABLES` with `"live_stock_now", "live_oos_demand_minute", "dim_store", "dim_store_district", "dim_product_current"`, change the set of expected rule uids to include `fs-oos-demand`, and the count to 5, and add

```python
    oos = rules["fs-oos-demand"]
    assert threshold(oos) == ("gt", 0) and oos["noDataState"] == "OK"
    assert "interval '3 minutes'" in sql(oos) and "count(*) >= 2" in sql(oos)
    assert "oos_at_write" in sql(oos) and "sum(add_to_cart) AS carts" in sql(oos) and "carts >= 5" in sql(oos)  # per minute, flagged
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
             "Available stock is on-hand stock; order reservations are out of scope. Demand on empty shelves is flagged "
             "with the stock when the minute was written: an estimate that a replay can change.", (18, 0, 6, 5)),
        timeseries(5, "Demand on out-of-stock products per minute",
                   "Product views and add-to-carts that arrived for a store and product with zero stock when the minute "
                   "was written (the stock when the minute was written, not at event time). Demand the shelf could not serve.",
                   "SELECT $__timeGroupAlias(minute_start, '1m', 0), sum(views) AS \"views\", "
                   "sum(add_to_cart) AS \"add to cart\" FROM live_oos_demand_minute "
                   "WHERE oos_at_write AND $__timeFilter(minute_start) GROUP BY 1 ORDER BY 1", (0, 5, 12, 8)),
        timeseries(6, "App traffic: events per minute",
                   "Non-late, non-bot events per minute from live_metrics_minute (all countries).",
                   "SELECT $__timeGroupAlias(minute_start, '1m', 0), sum(events) AS \"events\" FROM live_metrics_minute "
                   "WHERE $__timeFilter(minute_start) GROUP BY 1 ORDER BY 1", (12, 5, 12, 8)),
        table(7, "Lowest stock now (20 products)",
              "The twenty store and product pairs with the least available stock.",
              "SELECT s.store_name AS \"store\", coalesce(p.name, l.product_id) AS \"product\", "
              "l.available AS \"available\", l.reorder_point AS \"reorder point\" FROM live_stock_now l "
              "JOIN dim_store s ON s.store_id = l.store_id "
              "LEFT JOIN dim_product_current p ON p.product_id = l.product_id "
              "ORDER BY l.available ASC, l.store_id, l.product_id LIMIT 20", (0, 13, 12, 9)),
        table(8, "Lost demand now (last 15 minutes)",
              "Products with the most add-to-carts while out of stock in the last 15 minutes (stock when the minute was written).",
              "SELECT s.store_name AS \"store\", coalesce(p.name, d.product_id) AS \"product\", "
              "sum(d.add_to_cart) AS \"add to cart\", sum(d.views) AS \"views\" FROM live_oos_demand_minute d "
              "JOIN dim_store s ON s.store_id = d.store_id "
              "LEFT JOIN dim_product_current p ON p.product_id = d.product_id "
              "WHERE d.oos_at_write AND d.minute_start >= now() - interval '15 minutes' "
              "GROUP BY d.store_id, s.store_name, d.product_id, p.name ORDER BY 3 DESC, 4 DESC LIMIT 10",
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
                  FROM (
                    SELECT minute_start, store_id, product_id, sum(add_to_cart) AS carts
                    FROM live_oos_demand_minute
                    WHERE minute_start >= now() - interval '3 minutes' AND oos_at_write
                    GROUP BY minute_start, store_id, product_id
                  ) m
                  WHERE carts >= 5
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

The inner query sums the campaigns of one minute first (the table has one row per campaign) and only minutes flagged `oos_at_write` count. Also update the header comment of the file: the list of rules now has five.

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

### Task 8B: Live express SLA table, panels and the "any breach" alert

**Files:**
- Create: `warehouse/live_express.py`, `tests/test_live_express_spark.py`, `tests/test_live_express_sink.py`
- Modify: `warehouse/live_tables.py`, `warehouse/live_ddl.sql`, `warehouse/live_store.py`, `monitoring/dashboards.py` (extend `build_inventory_ops`), `monitoring/grafana/dashboards/inventory-ops.json` (regenerate), `monitoring/grafana/provisioning/alerting/alerting.yaml`, `tests/test_inventory_dashboards.py`, `tests/test_grafana_provisioning.py`, `tests/test_live_stock_sink.py` (table counts)

**Interfaces:**
- Consumes: Task 6B (`silver_express_orders`), Task 7 (the sink and its thread pool), Task 8 (the dashboard builder and the alert file).
- Produces:
  - Postgres table `live_express_orders(order_id, store_id, promised_minutes, created_at, delivered_at, closed_without_delivery)`, key `order_id`; `delivered_at` is NULL until the order's first `delivered` status; `closed_without_delivery` is true when the order's latest status is `order_cancelled` or `order_returned` and it has no `delivered_at`. Grafana computes elapsed time and state from these columns with `now()`, so the state moves even between Silver batches.
  - `compute_express_rows(spark, silver_root, since) -> list[dict]`: every express order created at or after `since`, joined to its first `delivered` status time and to its latest status (`silver_order_status`: greatest `status_time`, ties by `status_rank`), as `live_express_orders` rows. The sink calls it each batch with `since = now - 3 hours` (so an order that is still open is refreshed until it is delivered or older than three hours).
  - Grafana: a row "Express delivery, 20-minute promise" on the Operations dashboard and the alert rule `fs-express-sla`.

Definitions used everywhere (the dashboard prints them): an express order is **on time** if `delivered_at - created_at <= promised_minutes`; it is **breached** if it was delivered later than its promise, or if it is still undelivered, is not `closed_without_delivery` and `now() - created_at > promised_minutes + 1 minute` (a one-minute grace, so a `delivered` event that reaches the sink a batch late does not raise a false alarm); it is **at risk** if undelivered, not closed and `now() - created_at >= 0.75 x promised_minutes` and not yet breached (15 minutes for the standard promise). An order whose latest status is cancelled or returned is never "undelivered past its promise": it has no delivery to be late. The promise is always the order's own `promised_minutes`. The SLA clock compares the `order_created` time with the `delivered` event time; both are simulator event times, so there is no clock skew between the customer side and the courier side.

Behaviour to implement, test first:

- **Table and DDL.** `live_express_orders` in `live_tables.py`, `live_ddl.sql` (primary key `order_id`, `delivered_at` nullable, `closed_without_delivery boolean NOT NULL DEFAULT false`) and the `GRANT SELECT` list; `LIVE_TABLES` goes from 9 to 10 entries in the pinned tests of `tests/test_live_stock_sink.py`.
- **Compute (Spark test).** Orders created before `since` are excluded; an order with a `delivered` status gets the earliest delivered time (a replayed or duplicated status row does not change it); an order without one has `delivered_at` NULL; an undelivered order whose latest status is `order_cancelled` or `order_returned` has `closed_without_delivery` true, while an order that was delivered and later returned keeps its `delivered_at` and has the flag false; two stores and a promise of 2 minutes are carried unchanged. Without the `silver_express_orders` table the result is `[]`.
- **Sink (host test, same stub style as Task 7).** With stock or express activity seen, the express compute runs in the sink's thread pool with the others and its rows are upserted; a failing express compute makes `on_batch` return None without raising and the work stays pending (the Task 7 failure test pattern); when no express order was ever seen the express compute is skipped (no extra Spark job per batch).
- **Panels** (extend `build_inventory_ops`; every query reads only `live_express_orders` and `dim_store`):
  - stat "Express orders at risk now": `SELECT count(*) AS "orders" FROM live_express_orders WHERE delivered_at IS NULL AND NOT closed_without_delivery AND now() - created_at >= promised_minutes * interval '45 seconds' AND now() - created_at <= promised_minutes * interval '1 minute' + interval '1 minute'`, thresholds orange at 1, red at 3.
  - stat "Express breaches, last hour": delivered later than the promise, or undelivered, not closed and older than `promised_minutes + 1 minute`, among orders created in the last hour; thresholds red at 1 (any breach is red).
  - stat "On-time rate, last hour": on-time orders divided by orders that are either delivered or breached (cancelled and returned undelivered orders are in neither count), as a percentage, unit percent, red below 100.
  - stat "Slowest express delivery, last hour (minutes)": the maximum of elapsed minutes (delivery time for delivered orders, age for open ones that are not closed), thresholds orange at 15, red at 20.
  - table "Slowest express orders, last hour": store, order id, promise, elapsed minutes, state (on time, at risk, breached, closed without delivery), ten rows ordered by elapsed minutes descending.
  The dashboard test of Task 8 extends: the new panels exist, use the allowed tables, and the stat thresholds are as above.
- **Alert `fs-express-sla`** (severity critical, evaluated every minute, `for: 0s`, `noDataState: OK`): fires when at least one express order created in the last 60 minutes is breached (the breach test of the definitions above: a delivery later than the promise, or `now() - created_at > promised_minutes + 1 minute` for an undelivered order that is not `closed_without_delivery`). Summary "An express order missed its delivery promise"; runbook "Open 'Operations, live inventory', table 'Slowest express orders': find the store, check its stock and riders, call the customer, and record the cause." The provisioning test pins the rule's uid, threshold (`gt 0`), severity and that its SQL contains `promised_minutes`, `live_express_orders`, `closed_without_delivery` and `interval '1 minute'` (the grace).
- **Rehearsal.** `.\scripts\watch_demo.ps1 -Inventory -ExpressPromiseMinutes 2 -LateExpressEvery 15` runs the whole path with a 2-minute promise and every 15th express order late, so the alert fires within about 3 minutes; the same command without the two options shows no breach in the normal case. Add both switches to the demo script (they pass `--express-promise-minutes` and `--late-express-every` to the runner) and extend the script test of Task 6.

Run the host suite and the Spark files one at a time. Commit only when told to commit: `Add the live express delivery SLA table, panels and the any-breach alert`.

---

### Task 9A: Daily inventory mart and the Grafana inventory-insights page (core)

**Files:**
- Create: `warehouse/inventory_daily.py`, `scripts/build_inventory_daily.py`, `tests/test_inventory_daily.py`, `tests/test_inventory_daily_spark.py`, `monitoring/grafana/dashboards/inventory-daily.json` (generated)
- Modify: `warehouse/live_tables.py`, `warehouse/live_ddl.sql`, `monitoring/dashboards.py`, `scripts/gen_dashboards.py`, `tests/test_inventory_dashboards.py`, `tests/test_live_stock_sink.py` (table counts)

**Interfaces:**
- Consumes: Task 7 (`_stock_levels(spark, silver_root, pairs=None, as_of=None)`, the Silver stock tables, `live_oos_demand_minute`, `dim_store`).
- Produces:
  - Postgres tables `inventory_daily(report_date, store_id, product_id, closing_on_hand, units_sold, units_received, units_shrunk, units_adjusted, oversold_units, ended_out_of_stock)`, key `(report_date, store_id, product_id)`, and `oos_demand_daily(report_date, store_id, product_id, campaign_id, utm_campaign, served_views, served_add_to_cart, served_clicks, oos_views, oos_add_to_cart, oos_clicks)`, key `(report_date, store_id, product_id, campaign_id)`. Power BI reads both (`Inventory Daily` and `OOS Demand Daily`). `oos_demand_daily` is rolled up from `live_oos_demand_minute` (Task 7): `served_*` sum ALL served demand of the Cairo day, `oos_*` sum only the minutes flagged `oos_at_write`; `served_clicks` (all landing clicks in served districts) is the denominator of the exposed-spend share and `oos_clicks` its numerator, both from this one table. `campaign_id` is the campaign of the click (`'direct'` when none); `utm_campaign` is an attribute.
  - `cairo_day_bounds(report_date) -> (start, end)` aware UTC datetimes, end exclusive.
  - `compute_inventory_daily(spark, silver_root, report_date) -> list[dict]`.
  - `rollup_oos_demand_daily(pg_store, report_date) -> None`.
  - `scripts/build_inventory_daily.py [--date today|yesterday|YYYY-MM-DD] [--days N]` (needs Spark and Postgres only, no Databricks). `--date` defaults to yesterday and `--days` to **2**: the last two Cairo days are rebuilt on every run, so a late event or an express order that was still open at midnight is corrected by the next run. This script is the one writer of `inventory_daily`, `oos_demand_daily` and `express_daily`.
  - `monitoring.dashboards.build_inventory_daily()` and the dashboard uid `fs-inventory-daily`.

**Express addition (the 20-minute promise).** Task 9A also builds a third Postgres table, `express_daily(report_date, store_id, orders, delivered, on_time, breached, avg_delivery_minutes, max_delivery_minutes)`, key `(report_date, store_id)`, with `compute_express_daily(spark, silver_root, report_date, now=None) -> list[dict]` in `warehouse/inventory_daily.py` (`now` defaults to the current UTC time; tests pass a fixed one) and a call in `scripts/build_inventory_daily.py`. Per Cairo day of `silver_express_orders.created_at` and store: `orders` counts express orders; `delivered` those with a `delivered` status (earliest per order); `on_time` the delivered ones with `delivered_at - created_at <= promised_minutes`; `breached` the delivered-late ones plus the undelivered ones whose latest status is NOT `order_cancelled` or `order_returned` and whose age at `now` (the build time) exceeds `promised_minutes + 1 minute`; an undelivered order that was cancelled or returned is counted in `orders` only; the two minute columns are the average and maximum of `delivered_at - created_at` in minutes over delivered orders (NULL when none). The SLA clock compares the `order_created` time with the `delivered` event time, both simulator event times. Spark tests (write them first): a mix of on-time, late and open orders over two stores and a Cairo midnight split; the maximum is the slowest delivered order; a replayed status row changes nothing; a day without express orders gives `[]`; a cancelled undelivered order past its promise is not breached; an order created at 23:55 and still open is not breached with `now` at 00:00 and is breached when the same day is built again with a later `now` (this is why the script rebuilds two days). Spec string `_EXPRESS_DAILY = "report_date:date store_id:string orders:int delivered:int on_time:int breached:int avg_delivery_minutes:decimal(8,2)? max_delivery_minutes:decimal(8,2)?"`, key `(report_date, store_id)`; the DDL, the `LIVE_TABLES` entry and the `GRANT` go in with the other two tables (`LIVE_TABLES` has 13 entries after Task 9A: 10 after Task 8B plus these three). The Grafana daily page gets two more panels: stat "Express on-time rate, latest day" (`SELECT coalesce(round(100.0 * sum(on_time) / nullif(sum(on_time) + sum(breached), 0), 1), 100) AS "percent" FROM express_daily WHERE report_date = (SELECT max(report_date) FROM express_daily)`, unit percent, red below 100) and timeseries "Slowest express delivery per day (minutes)" (`SELECT report_date::timestamp AS "time", max(max_delivery_minutes) AS "slowest", 20 AS "promise" FROM express_daily WHERE $__timeFilter(report_date::timestamp) GROUP BY 1 ORDER BY 1`). Power BI reads the table as `Express Daily`.

Definitions (also printed on the dashboard): `closing_on_hand` is the stock at the end of the Cairo day (latest snapshot plus the movements after it, before midnight, never below zero); `units_sold`, `units_received`, `units_shrunk` and `units_adjusted` sum the movements of that day by reason; `oversold_units` is the sum over sold movements of `requested_qty` minus the units taken (a sale of zero units emits no movement, so fully oversold lines are not counted: a lower bound); `ended_out_of_stock` is `closing_on_hand - reserved <= 0`. The model is reserve-on-order: a cancelled or returned order does not put stock back (known limitation, spec section 14), so stock drifts down with every cancellation. The out-of-stock flag of the demand columns is the stock when the minute was written; it is not replay-safe (known limitation).

- [ ] **Step 1: Write the failing host test**

`tests/test_inventory_daily.py`:

```python
import re
from datetime import date, datetime, timezone
from pathlib import Path

from warehouse.inventory_daily import cairo_day_bounds, rollup_oos_demand_daily
from warehouse.live_tables import LIVE_TABLE_NAMES, LIVE_TABLES

DDL = (Path(__file__).parent.parent / "warehouse" / "live_ddl.sql").read_text(encoding="utf-8")


def test_the_cairo_day_in_utc_follows_the_clock_of_egypt():
    start, end = cairo_day_bounds(date(2026, 10, 10))  # Egypt is on summer time (UTC+3) until the end of October
    assert start == datetime(2026, 10, 9, 21, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 10, 10, 21, 0, tzinfo=timezone.utc)
    start, end = cairo_day_bounds(date(2026, 12, 1))  # winter time, UTC+2
    assert start == datetime(2026, 11, 30, 22, 0, tzinfo=timezone.utc)
    assert (end - start).total_seconds() == 24 * 3600


def test_the_new_tables_are_specified_created_and_granted():
    spec = {t.name: t for t in LIVE_TABLES}
    assert {"inventory_daily", "oos_demand_daily", "express_daily"} <= set(LIVE_TABLE_NAMES)
    assert spec["inventory_daily"].keys == ("report_date", "store_id", "product_id")
    assert spec["oos_demand_daily"].keys == ("report_date", "store_id", "product_id", "campaign_id")
    assert spec["express_daily"].keys == ("report_date", "store_id")
    cols = {c.name for c in spec["oos_demand_daily"].columns}
    assert {"campaign_id", "utm_campaign", "served_clicks", "oos_clicks", "oos_views", "oos_add_to_cart"} <= cols
    for name in ("inventory_daily", "oos_demand_daily", "express_daily"):
        assert re.search(r"CREATE TABLE IF NOT EXISTS %s \(" % name, DDL), name
        assert name in DDL.split("GRANT SELECT ON")[1].split("TO grafana_ro")[0], name


class RecordingStore:
    def __init__(self):
        self.executed = []

    def execute(self, sql, retry=True):
        self.executed.append(sql)


def test_the_rollup_sums_the_minutes_of_one_cairo_day_and_is_idempotent():
    store = RecordingStore()
    rollup_oos_demand_daily(store, date(2026, 10, 10))
    [sql] = store.executed
    assert "FROM live_oos_demand_minute" in sql and "ON CONFLICT (report_date, store_id, product_id, campaign_id)" in sql
    assert "AT TIME ZONE 'Africa/Cairo'" in sql and "DATE'2026-10-10'" in sql
    assert "FILTER (WHERE oos_at_write)" in sql  # the oos_* columns are the flagged minutes, the served_* columns all of them
    assert "DO UPDATE SET utm_campaign = EXCLUDED.utm_campaign" in sql
    assert "served_clicks = EXCLUDED.served_clicks" in sql and "oos_clicks = EXCLUDED.oos_clicks"  in sql  # a total is replaced, never incremented
    rollup_oos_demand_daily(store, date(2026, 10, 10))
    assert store.executed[0] == store.executed[1]
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_daily.py -q`
Expected: collection error (`warehouse.inventory_daily` missing).

- [ ] **Step 3: Implement the module, the tables and the DDL**

`warehouse/live_tables.py`: add

```python
_INV_DAILY = ("report_date:date store_id:string product_id:string closing_on_hand:int units_sold:int "
              "units_received:int units_shrunk:int units_adjusted:int oversold_units:int ended_out_of_stock:boolean")
_OOS_DAILY = ("report_date:date store_id:string product_id:string campaign_id:string utm_campaign:string "
              "served_views:bigint served_add_to_cart:bigint served_clicks:bigint oos_views:bigint "
              "oos_add_to_cart:bigint oos_clicks:bigint")
_EXPRESS_DAILY = ("report_date:date store_id:string orders:int delivered:int on_time:int breached:int "
                  "avg_delivery_minutes:decimal(8,2)? max_delivery_minutes:decimal(8,2)?")
```

and append to `LIVE_TABLES`:

```python
    Table("inventory_daily", "gold", _parse(_INV_DAILY, ["report_date", "store_id", "product_id"]),
          ("report_date", "store_id", "product_id")),
    Table("oos_demand_daily", "gold",
          _parse(_OOS_DAILY, ["report_date", "store_id", "product_id", "campaign_id"]),
          ("report_date", "store_id", "product_id", "campaign_id")),
    Table("express_daily", "gold", _parse(_EXPRESS_DAILY, ["report_date", "store_id"]), ("report_date", "store_id")),
```

`warehouse/live_ddl.sql`: add before the `-- BEGIN grafana_ro` line

```sql
CREATE TABLE IF NOT EXISTS inventory_daily (          -- one row per store, product and Cairo day (simulated stock)
    report_date        date    NOT NULL,
    store_id           text    NOT NULL,
    product_id         text    NOT NULL,
    closing_on_hand    integer NOT NULL,              -- stock at the end of the day, never below zero
    units_sold         integer NOT NULL DEFAULT 0,
    units_received     integer NOT NULL DEFAULT 0,
    units_shrunk       integer NOT NULL DEFAULT 0,
    units_adjusted     integer NOT NULL DEFAULT 0,
    oversold_units     integer NOT NULL DEFAULT 0,    -- asked for beyond the shelf: a lower bound
    ended_out_of_stock boolean NOT NULL,
    PRIMARY KEY (report_date, store_id, product_id)
);

CREATE TABLE IF NOT EXISTS oos_demand_daily (         -- live_oos_demand_minute summed per Cairo day
    report_date        date   NOT NULL,
    store_id           text   NOT NULL,
    product_id         text   NOT NULL,
    campaign_id        text   NOT NULL,                -- 'direct' when the click had no campaign
    utm_campaign       text   NOT NULL DEFAULT '',     -- attribute only
    served_views       bigint NOT NULL DEFAULT 0,      -- all demand in served districts
    served_add_to_cart bigint NOT NULL DEFAULT 0,
    served_clicks      bigint NOT NULL DEFAULT 0,      -- all landing clicks in served districts (denominator of the share)
    oos_views          bigint NOT NULL DEFAULT 0,      -- the part of the minutes flagged oos_at_write (stock when written)
    oos_add_to_cart    bigint NOT NULL DEFAULT 0,
    oos_clicks         bigint NOT NULL DEFAULT 0,      -- numerator of the share
    PRIMARY KEY (report_date, store_id, product_id, campaign_id)
);

CREATE TABLE IF NOT EXISTS express_daily (            -- one row per store and Cairo day, express orders and the promise
    report_date          date    NOT NULL,
    store_id             text    NOT NULL,
    orders               integer NOT NULL DEFAULT 0,
    delivered            integer NOT NULL DEFAULT 0,
    on_time              integer NOT NULL DEFAULT 0,
    breached             integer NOT NULL DEFAULT 0,
    avg_delivery_minutes numeric(8,2),
    max_delivery_minutes numeric(8,2),
    PRIMARY KEY (report_date, store_id)
);
```

and add `inventory_daily, oos_demand_daily, express_daily` to the GRANT list (the line already lists the Task 7 and Task 8B tables).

`warehouse/inventory_daily.py`:

```python
"""Daily inventory mart (inventory design, section 9, and the Power BI and Grafana inventory pages).

inventory_daily: one row per store, product and Cairo day, computed from the Silver stock tables.
oos_demand_daily: live_oos_demand_minute summed per Cairo day (plain SQL in Postgres): all served demand, and the part
flagged out of stock when the minute was written.
"""
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

from warehouse.live_common import _read
from warehouse.live_stock import _stock_levels

CAIRO, UTC = ZoneInfo("Africa/Cairo"), ZoneInfo("UTC")


def cairo_day_bounds(report_date):
    """(start, end) of the Cairo calendar day as aware UTC datetimes; end is exclusive."""
    start = datetime.combine(report_date, time(0), tzinfo=CAIRO).astimezone(UTC)
    end = datetime.combine(report_date + timedelta(days=1), time(0), tzinfo=CAIRO).astimezone(UTC)
    return start, end


def compute_inventory_daily(spark, silver_root, report_date):
    """inventory_daily rows for the Cairo day: end-of-day stock plus the day's flows, for every store and product pair
    that has a snapshot before the end of the day. [] when the Silver stock tables do not exist."""
    _, end = cairo_day_bounds(report_date)
    levels = _stock_levels(spark, silver_root, as_of=end)
    if levels is None:
        return []
    keys = ["store_id", "product_id"]
    moves = _read(spark, silver_root, "silver_stock_movements")
    flows = None
    if moves is not None:
        day = moves.where(F.col("event_date") == F.lit(report_date.isoformat()).cast("date"))

        def total(reason, value):
            return F.coalesce(F.sum(F.when(F.col("reason") == reason, value)), F.lit(0))

        flows = day.groupBy(*keys).agg(
            total("sold", -F.col("qty_delta")).alias("units_sold"),
            total("received", F.col("qty_delta")).alias("units_received"),
            total("shrinkage", -F.col("qty_delta")).alias("units_shrunk"),
            total("adjustment", F.col("qty_delta")).alias("units_adjusted"),
            total("sold", F.col("requested_qty") + F.col("qty_delta")).alias("oversold_units"))
    joined = levels.join(flows, keys, "left") if flows is not None else levels
    rows = []
    for r in joined.collect():
        on_hand, reserved = int(r["on_hand"]), int(r["reserved_qty"] or 0)

        def flow(name):
            return int(r[name] or 0) if flows is not None else 0

        rows.append(dict(report_date=report_date, store_id=r["store_id"], product_id=r["product_id"],
                         closing_on_hand=max(on_hand, 0), units_sold=flow("units_sold"),
                         units_received=flow("units_received"), units_shrunk=flow("units_shrunk"),
                         units_adjusted=flow("units_adjusted"), oversold_units=flow("oversold_units"),
                         ended_out_of_stock=(on_hand - reserved) <= 0))
    return rows


def rollup_oos_demand_daily(pg_store, report_date):
    """Replace the oos_demand_daily rows of one Cairo day with the sums of live_oos_demand_minute (idempotent)."""
    day = report_date.isoformat()  # a date object: nothing else can reach the SQL text
    pg_store.execute(
        "INSERT INTO oos_demand_daily (report_date, store_id, product_id, campaign_id, utm_campaign, served_views, "
        "served_add_to_cart, served_clicks, oos_views, oos_add_to_cart, oos_clicks) "
        "SELECT (minute_start AT TIME ZONE 'Africa/Cairo')::date, store_id, product_id, campaign_id, min(utm_campaign), "
        "sum(views), sum(add_to_cart), sum(landing_clicks), "
        "coalesce(sum(views) FILTER (WHERE oos_at_write), 0), "
        "coalesce(sum(add_to_cart) FILTER (WHERE oos_at_write), 0), "
        "coalesce(sum(landing_clicks) FILTER (WHERE oos_at_write), 0) FROM live_oos_demand_minute "
        "WHERE (minute_start AT TIME ZONE 'Africa/Cairo')::date = DATE'%s' GROUP BY 1, 2, 3, 4 "
        "ON CONFLICT (report_date, store_id, product_id, campaign_id) DO UPDATE SET "
        "utm_campaign = EXCLUDED.utm_campaign, served_views = EXCLUDED.served_views, "
        "served_add_to_cart = EXCLUDED.served_add_to_cart, served_clicks = EXCLUDED.served_clicks, "
        "oos_views = EXCLUDED.oos_views, oos_add_to_cart = EXCLUDED.oos_add_to_cart, "
        "oos_clicks = EXCLUDED.oos_clicks" % day)
```

`scripts/build_inventory_daily.py`:

```python
"""Build inventory_daily, oos_demand_daily and express_daily in Postgres for one or more Cairo days.

Needs Spark (the Silver tables) and PG_DSN, not Databricks: run it in the spark container:
  docker compose run --rm spark python scripts/build_inventory_daily.py [--date today] [--days 2]
By default it rebuilds the last 2 Cairo days (yesterday and the day before), so late events and express orders that
were still open at midnight are corrected by the next run.
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from streaming.common.spark import get_spark
from warehouse.inventory_daily import compute_express_daily, compute_inventory_daily, rollup_oos_demand_daily
from warehouse.store import get_store


def parse_day(text, today):
    if text == "today":
        return today
    if text == "yesterday":
        return today - timedelta(days=1)
    return date.fromisoformat(text)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", default="yesterday", help="last Cairo day to build: today, yesterday or YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=2, help="how many days to build, ending at --date (default 2)")
    ap.add_argument("--lake", default="data/lake")
    args = ap.parse_args(argv)
    if args.days < 1:
        print("error: --days must be at least 1", file=sys.stderr)
        return 2
    today = datetime.now(ZoneInfo("Africa/Cairo")).date()
    last = parse_day(args.date, today)
    lake = os.path.abspath(args.lake)
    pg = get_store("postgres")
    try:
        spark = get_spark("inventory_daily")
        for back in range(args.days - 1, -1, -1):
            day = last - timedelta(days=back)
            rows = compute_inventory_daily(spark, lake, day)
            if rows:
                pg.upsert("inventory_daily", rows, ["report_date", "store_id", "product_id"])
            express = compute_express_daily(spark, lake, day)
            if express:
                pg.upsert("express_daily", express, ["report_date", "store_id"])
            rollup_oos_demand_daily(pg, day)
            print("%s: %d inventory rows, %d express rows" % (day, len(rows), len(express)))
    finally:
        pg.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run the host tests**

Run: `.venv\Scripts\python -m pytest tests\test_inventory_daily.py -q`, then the full host suite (update the table counts in `tests/test_live_stock_sink.py` from 10 to 13: the three tables of this task, express_daily included). Expected: PASS.

- [ ] **Step 5: Write the failing Spark test and run it**

`tests/test_inventory_daily_spark.py`:

```python
"""Spark tests (Docker, RUN_SPARK_TESTS=1) for the daily inventory mart."""
from datetime import date

import pytest

pytestmark = pytest.mark.spark

from test_live_stock_spark import K, move, snap, write  # noqa: E402
from test_g1_live_spark import T0  # noqa: E402
from warehouse import inventory_daily  # noqa: E402

DAY = date(2026, 10, 8)  # T0 is 2026-10-08 12:00 UTC, which is 15:00 on the 8th in Cairo


def row_of(rows, key=K):
    [r] = [r for r in rows if (r["store_id"], r["product_id"]) == key]
    return r


def test_flows_and_closing_stock_of_one_day(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0)])
    sold = move(1, K, -5, 3)
    sold["requested_qty"] = 7  # asked for 7, only 5 were there: 2 oversold
    write(spark, lake, "silver_stock_movements",
          [sold, move(2, K, -3, 4), move(3, K, 60, 5, "received"), move(4, K, -2, 6, "shrinkage"),
           move(5, K, 4, 7, "adjustment")])
    r = row_of(inventory_daily.compute_inventory_daily(spark, lake, DAY))
    assert (r["units_sold"], r["units_received"], r["units_shrunk"], r["units_adjusted"]) == (8, 60, 2, 4)
    assert r["oversold_units"] == 2
    assert r["closing_on_hand"] == 40 - 5 - 3 + 60 - 2 + 4 and r["ended_out_of_stock"] is False


def test_a_product_that_runs_out_ends_the_day_out_of_stock_and_never_negative(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 4, 0)])
    write(spark, lake, "silver_stock_movements", [move(1, K, -4, 3)])
    r = row_of(inventory_daily.compute_inventory_daily(spark, lake, DAY))
    assert r["closing_on_hand"] == 0 and r["ended_out_of_stock"] is True
    write(spark, lake, "silver_stock_movements", [move(2, K, -6, 4)])  # a stock error: the raw level would be -6
    r = row_of(inventory_daily.compute_inventory_daily(spark, lake, DAY))
    assert r["closing_on_hand"] == 0


def test_cairo_midnight_splits_the_days_and_closing_stock_stops_at_the_cut(spark, tmp_path):
    lake = str(tmp_path / "lake")
    late = T0.replace(hour=22, minute=30)  # 01:30 on the 9th in Cairo
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0)])
    write(spark, lake, "silver_stock_movements", [move(1, K, -5, 3)])
    after_midnight = dict(move(2, K, -7, 0), event_time=late, kafka_timestamp=late, event_date=date(2026, 10, 9))
    write(spark, lake, "silver_stock_movements", [after_midnight])
    day8 = row_of(inventory_daily.compute_inventory_daily(spark, lake, DAY))
    day9 = row_of(inventory_daily.compute_inventory_daily(spark, lake, date(2026, 10, 9)))
    assert (day8["units_sold"], day8["closing_on_hand"]) == (5, 35)
    assert (day9["units_sold"], day9["closing_on_hand"]) == (7, 28)


def test_a_day_without_movements_still_reports_the_closing_stock(spark, tmp_path):
    lake = str(tmp_path / "lake")
    write(spark, lake, "silver_stock_snapshots", [snap(1, K, 40, 0)])
    r = row_of(inventory_daily.compute_inventory_daily(spark, lake, date(2026, 10, 9)))
    assert (r["closing_on_hand"], r["units_sold"], r["oversold_units"]) == (40, 0, 0)


def test_without_the_stock_tables_nothing_is_computed(spark, tmp_path):
    assert inventory_daily.compute_inventory_daily(spark, str(tmp_path / "empty"), DAY) == []
```

Run: `docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest tests/test_inventory_daily_spark.py tests/test_live_stock_spark.py -q -p no:cacheprovider` (one file after the other if memory is short).
Expected before Step 3 is complete: FAIL (`compute_inventory_daily` missing); after: 5 plus 8 passed (the express Spark tests of the addition above are in `tests/test_inventory_daily_spark.py` too and raise the first number). If `test_live_stock_spark.py` fails after the `_stock_levels` refactor, the refactor changed the live behaviour: fix the refactor, not the test.

- [ ] **Step 6: The Grafana inventory-insights page**

In `monitoring/dashboards.py` add `build_inventory_daily()` returning `dashboard("fs-inventory-daily", "FunnelStream: Inventory insights, daily", panels)` with these panels (grid 24 wide; stats on the top row at height 5):

1. stat "Closing stock, latest day (units)": `SELECT coalesce(sum(closing_on_hand), 0) AS "units" FROM inventory_daily WHERE report_date = (SELECT max(report_date) FROM inventory_daily)`
2. stat "Products out of stock at day end": `SELECT count(*) AS "products" FROM inventory_daily WHERE ended_out_of_stock AND report_date = (SELECT max(report_date) FROM inventory_daily)` with thresholds `_steps(None, 1, 5)`.
3. stat "Oversold units, latest day": `SELECT coalesce(sum(oversold_units), 0) AS "units" FROM inventory_daily WHERE report_date = (SELECT max(report_date) FROM inventory_daily)` with thresholds `_steps(None, 1, 20)`.
4. stat "Shrinkage share of units out, latest day": `SELECT coalesce(round(100.0 * sum(units_shrunk) / nullif(sum(units_sold) + sum(units_shrunk), 0), 1), 0) AS "percent" FROM inventory_daily WHERE report_date = (SELECT max(report_date) FROM inventory_daily)`, unit `"percent"`, thresholds `_steps(None, 3, 6)`.
5. timeseries "Closing stock by store (units)": `SELECT i.report_date::timestamp AS "time", s.store_name AS "metric", sum(i.closing_on_hand) AS "units" FROM inventory_daily i JOIN dim_store s ON s.store_id = i.store_id WHERE $__timeFilter(i.report_date::timestamp) GROUP BY 1, 2 ORDER BY 1`
6. timeseries "Units sold, received and shrunk per day": `SELECT report_date::timestamp AS "time", sum(units_sold) AS "sold", sum(units_received) AS "received", sum(units_shrunk) AS "shrunk" FROM inventory_daily WHERE $__timeFilter(report_date::timestamp) GROUP BY 1 ORDER BY 1`
7. table "Lowest days of cover, latest day (20 products)": `SELECT s.store_name AS "store", coalesce(p.name, i.product_id) AS "product", i.closing_on_hand AS "closing stock", round(a.per_day, 1) AS "sold per day", round(i.closing_on_hand / nullif(a.per_day, 0), 1) AS "days of cover" FROM inventory_daily i JOIN dim_store s ON s.store_id = i.store_id LEFT JOIN dim_product_current p ON p.product_id = i.product_id JOIN (SELECT store_id, product_id, sum(units_sold)::numeric / 7 AS per_day FROM inventory_daily WHERE report_date > (SELECT max(report_date) FROM inventory_daily) - 7 GROUP BY 1, 2) a ON a.store_id = i.store_id AND a.product_id = i.product_id WHERE i.report_date = (SELECT max(report_date) FROM inventory_daily) AND a.per_day > 0 ORDER BY 5 ASC NULLS LAST LIMIT 20`
8. timeseries "Demand on empty shelves per day": `SELECT report_date::timestamp AS "time", sum(oos_views) AS "views", sum(oos_add_to_cart) AS "add to cart" FROM oos_demand_daily WHERE $__timeFilter(report_date::timestamp) GROUP BY 1 ORDER BY 1`
9. text note: "Simulated stock. Closing stock is the end of the Cairo day; oversold units are a lower bound; cancelled orders do not return stock (reserve-on-order); demand on empty shelves is flagged with the stock when the minute was written, which a replay can change."

Add `"inventory-daily.json": dashboards.build_inventory_daily` to `FILES` in `scripts/gen_dashboards.py`, run it, and extend `tests/test_inventory_dashboards.py`: the file equals the builder; unique panel ids; every panel on `fs-postgres`; the tables found after `from` or `join` are within `{inventory_daily, oos_demand_daily, express_daily, dim_store, dim_product_current}`; the four stat titles above exist with the threshold lists above; each time series query contains `$__timeFilter(`.

- [ ] **Step 7: Run everything and check by eye**

Host suite; the Spark files of Step 5; then `.venv\Scripts\python scripts\gen_dashboards.py`, `docker compose up -d --force-recreate grafana`, `docker compose run --rm spark python scripts/apply_live_ddl.py` (expect 13 tables ready), and after a demo run with `-Inventory`: `docker compose run --rm spark python scripts/build_inventory_daily.py --date today` (expect two lines, today and yesterday: `<date>: N inventory rows, M express rows`). Open the dashboard "FunnelStream: Inventory insights, daily": no red triangle on any panel.

- [ ] **Step 8: Commit** (only when told to commit)

```bash
git add warehouse scripts monitoring tests
git -c user.name=FunnelStream -c user.email=finalprojectnti@gmail.com commit -m "Add the daily inventory mart and the Grafana inventory-insights page"
```

---

### Task 9B: Marketing D-1 mart and its Grafana page (stretch: cut first)

**Files:**
- Create: `warehouse/d1_marketing.py`, `scripts/build_d1_marketing.py`, `tests/test_d1_marketing.py`, `monitoring/grafana/dashboards/marketing-d1.json` (generated)
- Modify: `warehouse/live_ddl.sql`, `warehouse/live_tables.py`, `monitoring/dashboards.py`, `scripts/gen_dashboards.py`, `tests/test_live_stock_sink.py` (table count), `tests/test_inventory_dashboards.py`

**Interfaces:**
- Consumes: Task 7 (`live_oos_demand_minute` with `campaign_id`, `landing_clicks` and `oos_at_write`; `dim_product_current` for prices) and the Databricks `agg_campaign_daily`. There is no Silver read and no Spark in this task: both sides of the exposed-spend ratio come from the same Postgres table.
- Produces:
  - `exposed_spend(spend, oos_clicks, served_clicks) -> Decimal | None`.
  - `assemble_campaign_rows(report_date, today, campaigns, spend_rows, served_by_campaign, oos_by_campaign) -> list[dict]` (both click dicts are keyed by `campaign_id`).
  - `assemble_product_rows(report_date, oos_demand_by_pair, price_by_product) -> list[dict]`: EVERY (store, product) pair with demand flagged out of stock on the day, no limit; the Grafana and Power BI visuals do Top N.
  - `cairo_yesterday(now) -> date`, `read_spend_rows(dbx_store, report_date)`, `read_oos_demand(pg_store, report_date)`, `read_prices(pg_store)`.
  - Postgres tables `marketing_d1_campaign` (key `report_date, campaign_id`) and `marketing_d1_product` (key `report_date, store_id, product_id`).
  - `scripts/build_d1_marketing.py [--date YYYY-MM-DD]` (default: yesterday in Cairo).

**Day rule (the same sentence is in spec section 9, `dashboards/powerbi/MODEL.md` and `docs/metrics.md`).** Ad spend and ad clicks are the ad API's UTC day. Orders and demand are the Cairo day. `agg_campaign_daily.report_date` is the ad day. The D-1 job takes yesterday in Cairo for the demand (`live_oos_demand_minute`, Cairo day) and reads the ad row whose `report_date` has the same date value. The two days differ by at most 3 hours (Cairo is UTC+3 in summer and UTC+2 in winter); the mismatch is accepted and the page says so.

**Exposed spend (one definition).** `exposed_spend = spend x oos_clicks / served_clicks`. `served_clicks` is the number of landing clicks (product views that carry a campaign) in served districts, `oos_clicks` the part of them in minutes flagged `oos_at_write` (stock when the minute was written). Both come from `live_oos_demand_minute` (equal to the sums of `oos_demand_daily`), so numerator and denominator have the same population and click definition. It is an estimate, assuming spend is spread evenly over served landing clicks. It is not an upper bound and the pages never call it one.

- [ ] **Step 1: Write the failing host test**

`tests/test_d1_marketing.py`:

```python
from datetime import date, datetime, timezone
from decimal import Decimal

from warehouse.d1_marketing import (assemble_campaign_rows, assemble_product_rows, cairo_yesterday, exposed_spend,
                                    read_oos_demand, read_prices)

D = date(2026, 10, 9)
CAMPAIGNS = [{"campaign_id": "camp_201", "utm_campaign": "eid_sale_1"}, {"campaign_id": "camp_202", "utm_campaign": "ramadan_offer_1"}]
SPEND = [dict(campaign_id="camp_201", spend=Decimal("1000.0000"), clicks=500, orders_placed=20,
              revenue_placed=Decimal("5000.00"), revenue_delivered=Decimal("4000.00"), cpa=Decimal("50.00"),
              roas_placed=Decimal("5.00"), roas_delivered=Decimal("4.00"))]
SERVED, OOS = {"camp_201": 120}, {"camp_201": 30}


class FakePg:
    """Answers the three queries of the readers and records their text."""

    def __init__(self):
        self.sql = []

    def query(self, sql, params=None):
        self.sql.append(sql)
        if "dim_product_current" in sql:
            return [("p1", Decimal("100.00"))]
        if "GROUP BY campaign_id" in sql:
            return [("camp_201", 120, 30), ("direct", 0, 0)]
        return [("store_a", "p1", 7, 4)]


def test_exposed_spend_is_spend_times_the_out_of_stock_click_share():
    assert exposed_spend(Decimal("1000"), 30, 120) == Decimal("250.00")
    assert exposed_spend(Decimal("1000.0000"), 1, 3) == Decimal("333.33")
    assert exposed_spend(Decimal("1000"), 0, 120) == Decimal("0.00")


def test_exposed_spend_is_none_without_served_clicks_or_spend():
    assert exposed_spend(Decimal("1000"), 5, 0) is None
    assert exposed_spend(None, 5, 10) is None


def test_the_campaign_row_joins_spend_with_the_click_counts_through_the_campaign_id():
    rows = assemble_campaign_rows(D, date(2026, 10, 10), CAMPAIGNS, SPEND, SERVED, OOS)
    [row] = rows  # camp_202 has no spend row: it is not reported
    assert row["campaign_id"] == "camp_201" and row["utm_campaign"] == "eid_sale_1"
    assert (row["served_clicks"], row["oos_clicks"]) == (120, 30)
    assert row["oos_click_share"] == Decimal("0.2500")
    assert row["exposed_spend"] == Decimal("250.00")
    assert row["spend"] == Decimal("1000.00") and row["roas_delivered"] == Decimal("4.00")
    assert row["report_date"] == D


def test_a_recent_day_is_provisional_and_an_old_one_is_not():
    recent = assemble_campaign_rows(D, date(2026, 10, 10), CAMPAIGNS, SPEND, {"camp_201": 10}, {})[0]
    old = assemble_campaign_rows(D, date(2026, 10, 14), CAMPAIGNS, SPEND, {"camp_201": 10}, {})[0]
    assert recent["provisional"] is True and old["provisional"] is False


def test_a_campaign_without_served_clicks_has_no_share_and_no_exposed_spend():
    [row] = assemble_campaign_rows(D, date(2026, 10, 10), CAMPAIGNS, SPEND, {}, {})
    assert row["served_clicks"] == 0 and row["oos_click_share"] is None and row["exposed_spend"] is None


def test_every_out_of_stock_product_of_the_day_is_kept_per_store():
    demand = {("s1", "p1"): (4, 10), ("s2", "p1"): (1, 2), ("s1", "p2"): (6, 3), ("s1", "p6"): (2, 4), ("s1", "p7"): (5, 0)}
    prices = {"p1": Decimal("100.00"), "p2": Decimal("900.00"), "p7": Decimal("5.00")}  # p6 has no price
    rows = assemble_product_rows(D, demand, prices)
    assert len(rows) == 5  # no Top N here: the visual does it
    assert [(r["store_id"], r["product_id"]) for r in rows] == [("s1", "p2"), ("s1", "p1"), ("s2", "p1"), ("s1", "p7"), ("s1", "p6")]
    assert rows[0]["oos_add_to_carts"] == 3 and rows[0]["revenue_at_risk"] == Decimal("2700.00")
    assert rows[3]["revenue_at_risk"] == Decimal("0.00") and rows[4]["revenue_at_risk"] is None  # kept, but unpriced


def test_the_default_day_is_yesterday_on_the_cairo_calendar():
    assert cairo_yesterday(datetime(2026, 10, 9, 22, 30, tzinfo=timezone.utc)) == date(2026, 10, 9)  # 01:30 on the 10th in Cairo
    assert cairo_yesterday(datetime(2026, 10, 9, 20, 30, tzinfo=timezone.utc)) == date(2026, 10, 8)  # 23:30 on the 9th in Cairo


def test_both_sides_of_the_share_and_the_prices_come_from_postgres_not_from_silver():
    pg = FakePg()
    served, oos, pairs = read_oos_demand(pg, D)
    assert served == {"camp_201": 120, "direct": 0} and oos == {"camp_201": 30, "direct": 0}
    assert pairs == {("store_a", "p1"): (7, 4)}
    assert read_prices(pg) == {"p1": Decimal("100.00")}
    assert all("live_oos_demand_minute" in s or "dim_product_current" in s for s in pg.sql)
    assert not any("silver" in s.lower() for s in pg.sql)
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv\Scripts\python -m pytest tests\test_d1_marketing.py -q`
Expected: collection error (`warehouse.d1_marketing` missing).

- [ ] **Step 3: Implement the pure part**

`warehouse/d1_marketing.py`:

```python
"""Marketing D-1 mart (inventory design, section 9).

The pure functions turn yesterday's numbers into the rows of two Postgres tables. The readers (Databricks spend,
Postgres demand and prices) are thin and live below.

Day rule: ad spend and ad clicks are the ad API's UTC day; orders and demand are the Cairo day;
agg_campaign_daily.report_date is the ad day. The job takes yesterday in Cairo for the demand and the ad row with the
same date value; the mismatch is at most 3 hours and is accepted.

The metric is an ESTIMATE: exposed_spend = spend x oos_clicks / served_clicks, both counts from live_oos_demand_minute
(served districts only, the same landing-click definition), assuming spend is spread evenly over served landing clicks.
The out-of-stock flag is the stock when the minute was written (not replay-safe). It is titled "estimated spend exposed
to out-of-stock" everywhere.
"""
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

PROVISIONAL_DAYS = 3  # ROAS delivered matures one to three days after the order day
CAIRO = ZoneInfo("Africa/Cairo")
_CENT, _SHARE = Decimal("0.01"), Decimal("0.0001")


def cairo_yesterday(now):
    """The default D-1 date: yesterday on the Cairo calendar (`now` is an aware datetime)."""
    return now.astimezone(CAIRO).date() - timedelta(days=1)


def exposed_spend(spend, oos_clicks, served_clicks):
    """spend * oos_clicks / served_clicks to two decimals; None when there are no served clicks or no spend."""
    if spend is None or not served_clicks:
        return None
    return (Decimal(str(spend)) * oos_clicks / served_clicks).quantize(_CENT, rounding=ROUND_HALF_UP)


def assemble_campaign_rows(report_date, today, campaigns, spend_rows, served_by_campaign, oos_by_campaign):
    """One row per campaign that has a spend row for the day (campaigns: dicts with campaign_id and utm_campaign)."""
    utm_of = {c["campaign_id"]: c["utm_campaign"] for c in campaigns}
    provisional = (today - report_date) < timedelta(days=PROVISIONAL_DAYS)
    out = []
    for s in spend_rows:
        cid = s["campaign_id"]
        served, oos = int(served_by_campaign.get(cid, 0)), int(oos_by_campaign.get(cid, 0))
        share = (Decimal(oos) / Decimal(served)).quantize(_SHARE, rounding=ROUND_HALF_UP) if served else None
        out.append(dict(report_date=report_date, campaign_id=cid, utm_campaign=utm_of.get(cid, ""),
                        spend=None if s["spend"] is None else Decimal(str(s["spend"])).quantize(_CENT),
                        ad_clicks=s["clicks"], orders_placed=s["orders_placed"], revenue_placed=s["revenue_placed"],
                        revenue_delivered=s["revenue_delivered"], cpa=s["cpa"], roas_placed=s["roas_placed"],
                        roas_delivered=s["roas_delivered"], served_clicks=served, oos_clicks=oos,
                        oos_click_share=share, exposed_spend=exposed_spend(s["spend"], oos, served),
                        provisional=provisional))
    return out


def assemble_product_rows(report_date, oos_demand_by_pair, price_by_product):
    """One row per (store, product) with demand flagged out of stock that day; every pair is kept, the visual does Top N.

    oos_demand_by_pair: {(store_id, product_id): (oos_views, oos_add_to_carts)}. revenue_at_risk is add-to-carts times
    price, NULL when the product has no price. Sorted by revenue at risk, then store and product."""
    rows = []
    for (store_id, product_id), (views, carts) in oos_demand_by_pair.items():
        price = price_by_product.get(product_id)
        rows.append(dict(report_date=report_date, store_id=store_id, product_id=product_id, oos_views=int(views),
                         oos_add_to_carts=int(carts),
                         revenue_at_risk=None if price is None else (Decimal(str(price)) * int(carts)).quantize(_CENT)))
    rows.sort(key=lambda r: (r["revenue_at_risk"] is None, -(r["revenue_at_risk"] or 0), r["store_id"], r["product_id"]))
    return rows
```

- [ ] **Step 4: Run the pure tests**

Run: `.venv\Scripts\python -m pytest tests	est_d1_marketing.py -q`
Expected: collection error `cannot import name 'read_oos_demand'`: the readers are added in Step 5, and the whole file passes in Step 6. (To see the pure functions pass first, temporarily comment out the last test and its two imports.)

- [ ] **Step 5: Tables, readers and the script**

`warehouse/live_tables.py`: add

```python
_D1_CAMPAIGN = ("report_date:date campaign_id:string utm_campaign:string spend:decimal(12,2)? ad_clicks:bigint? "
                "orders_placed:bigint? revenue_placed:decimal(12,2)? revenue_delivered:decimal(12,2)? cpa:decimal(12,2)? "
                "roas_placed:decimal(12,2)? roas_delivered:decimal(12,2)? served_clicks:bigint oos_clicks:bigint "
                "oos_click_share:decimal(8,4)? exposed_spend:decimal(12,2)? provisional:boolean")
_D1_PRODUCT = ("report_date:date store_id:string product_id:string oos_views:bigint oos_add_to_carts:bigint "
               "revenue_at_risk:decimal(12,2)?")
```

and the two `Table(...)` entries (keys `("report_date", "campaign_id")` and `("report_date", "store_id", "product_id")`). In `warehouse/live_ddl.sql` add the matching `CREATE TABLE IF NOT EXISTS marketing_d1_campaign (...)` and `marketing_d1_product (...)` with the same column names, `numeric(12,2)` / `numeric(8,4)` for the decimals, `date` for `report_date`, and primary keys as above, and add both names to the GRANT list. Update the table count in `tests/test_live_stock_sink.py` from 13 to 15.

Append the readers to `warehouse/d1_marketing.py`:

```python
_CAIRO_DAY = "(minute_start AT TIME ZONE 'Africa/Cairo')::date = %s"


def read_spend_rows(dbx_store, report_date):
    """D-1 rows of agg_campaign_daily (last touch) from Databricks: the ad row whose report_date has the same date value."""
    sql = ("SELECT campaign_id, spend, clicks, orders_placed, revenue_placed, revenue_delivered, cpa, roas_placed, "
           "roas_delivered FROM %s WHERE report_date = DATE'%s' AND attribution_model = 'last_touch'"
           % (dbx_store.qualified("agg_campaign_daily"), report_date.isoformat()))
    cols = ["campaign_id", "spend", "clicks", "orders_placed", "revenue_placed", "revenue_delivered", "cpa", "roas_placed",
            "roas_delivered"]
    return [dict(zip(cols, r)) for r in dbx_store.query(sql)]


def read_oos_demand(pg_store, report_date):
    """(served clicks by campaign_id, out-of-stock clicks by campaign_id, {(store, product): (oos views, oos add-to-carts)}).

    All three come from live_oos_demand_minute for the Cairo day, so numerator and denominator of the share have the same
    population: landing clicks in served districts. 'Out of stock' is the oos_at_write flag."""
    served, oos = {}, {}
    for cid, n_served, n_oos in pg_store.query(
            "SELECT campaign_id, sum(landing_clicks), coalesce(sum(landing_clicks) FILTER (WHERE oos_at_write), 0) "
            "FROM live_oos_demand_minute WHERE " + _CAIRO_DAY + " GROUP BY campaign_id", (report_date,)):
        served[cid], oos[cid] = int(n_served), int(n_oos)
    pairs = {(store_id, product_id): (int(views), int(carts)) for store_id, product_id, views, carts in pg_store.query(
        "SELECT store_id, product_id, sum(views), sum(add_to_cart) FROM live_oos_demand_minute WHERE " + _CAIRO_DAY
        + " AND oos_at_write GROUP BY store_id, product_id", (report_date,))}
    return served, oos, pairs


def read_prices(pg_store):
    """{product_id: price} from dim_product_current (filled by scripts/apply_live_ddl.py from silver_products)."""
    return {pid: Decimal(str(price)) for pid, price in
            pg_store.query("SELECT product_id, price FROM dim_product_current WHERE price IS NOT NULL")}
```

`scripts/build_d1_marketing.py`:

```python
"""Build the marketing D-1 mart in Postgres for one Cairo day (default: yesterday in Cairo).

Day rule: the demand is the Cairo day, the ad row is the row whose report_date (the ad API's UTC day) has the same date
value; the mismatch of at most 3 hours is accepted. Needs Databricks settings (the spend) and PG_DSN, not Spark:
  docker compose run --rm spark python scripts/build_d1_marketing.py [--date 2026-10-09]
"""
import argparse
import os
import sys
from datetime import date, datetime, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from simulator.campaigns import CAMPAIGNS
from warehouse import d1_marketing as d1
from warehouse.store import get_store


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", help="Cairo day to report, YYYY-MM-DD (default yesterday in Cairo)")
    args = ap.parse_args(argv)
    now = datetime.now(timezone.utc)
    today = now.astimezone(d1.CAIRO).date()
    report_date = date.fromisoformat(args.date) if args.date else d1.cairo_yesterday(now)
    dbx, pg = get_store("databricks"), get_store("postgres")
    try:
        spend = d1.read_spend_rows(dbx, report_date)
        served, oos, pairs = d1.read_oos_demand(pg, report_date)
        campaign_rows = d1.assemble_campaign_rows(report_date, today, CAMPAIGNS, spend, served, oos)
        product_rows = d1.assemble_product_rows(report_date, pairs, d1.read_prices(pg))
        if campaign_rows:
            pg.upsert("marketing_d1_campaign", campaign_rows, ["report_date", "campaign_id"])
        if product_rows:
            pg.upsert("marketing_d1_product", product_rows, ["report_date", "store_id", "product_id"])
    finally:
        dbx.close()
        pg.close()
    print("D-1 %s: %d campaign rows, %d product rows" % (report_date, len(campaign_rows), len(product_rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Run the host test again**

Run: `.venv\Scripts\python -m pytest tests\test_d1_marketing.py -q`
Expected: 8 passed. There is no Spark test in this task: the Silver reader of the first draft is gone, so no Silver denominator can disagree with the Postgres numerator.

- [ ] **Step 7: The Grafana page**

In `monitoring/dashboards.py` add `build_marketing_d1()` returning `dashboard("fs-marketing-d1", "FunnelStream: Marketing, D-1", panels)` with these panels (all read the latest `report_date` in `marketing_d1_campaign`):
1. stat "Spend (EGP)": `SELECT coalesce(sum(spend), 0) AS "spend" FROM marketing_d1_campaign WHERE report_date = (SELECT max(report_date) FROM marketing_d1_campaign)`
2. stat "Orders placed": same with `sum(orders_placed)`.
3. stat "ROAS placed": `SELECT coalesce(sum(revenue_placed) / nullif(sum(spend), 0), 0) AS "roas placed" ...`
4. stat "ROAS delivered (provisional for recent days)": same with `revenue_delivered`.
5. stat "Estimated spend exposed to out-of-stock (EGP)": `sum(exposed_spend)`.
6. stat "Out-of-stock click share": `coalesce(sum(oos_clicks)::numeric / nullif(sum(served_clicks), 0), 0)` with `unit="percentunit"`.
7. table "Campaigns, sorted by exposed spend": `SELECT campaign_id, utm_campaign, spend, orders_placed, cpa, roas_placed, roas_delivered, served_clicks, oos_clicks, oos_click_share, exposed_spend, provisional FROM marketing_d1_campaign WHERE report_date = (SELECT max(report_date) FROM marketing_d1_campaign) ORDER BY exposed_spend DESC NULLS LAST`
8. table "Top 5 products by revenue at risk": `SELECT s.store_name AS "store", coalesce(p.name, m.product_id) AS "product", m.oos_add_to_carts, m.revenue_at_risk FROM marketing_d1_product m JOIN dim_store s ON s.store_id = m.store_id LEFT JOIN dim_product_current p ON p.product_id = m.product_id WHERE m.report_date = (SELECT max(report_date) FROM marketing_d1_product) ORDER BY m.revenue_at_risk DESC NULLS LAST LIMIT 5` (the mart keeps every out-of-stock product; the limit is the panel's, so the card sums stay whole).
9. text note: "Simulated data. Exposed spend = spend x out-of-stock landing clicks / served landing clicks, both counted in served districts; it is an estimate that assumes spend is spread evenly over served landing clicks. The out-of-stock flag is the stock when the minute was written, which a replay can change. Spend is the ad platform's UTC day, orders and demand are the Cairo day: they differ by at most 3 hours. Recent days are provisional: ROAS delivered matures after 1 to 3 days."

Add `"marketing-d1.json": dashboards.build_marketing_d1` to `FILES` in `scripts/gen_dashboards.py`, run it, and extend `tests/test_inventory_dashboards.py` with the same drift, datasource and table-allow-list checks for the marketing dashboard (allowed tables: `marketing_d1_campaign`, `marketing_d1_product`, `dim_store`, `dim_product_current`; the sub-select `FROM marketing_d1_campaign` inside the query is found by the same regex), plus: no panel text or description contains the words "upper bound".

- [ ] **Step 8: Run everything and try it on the real data**

Host suite. Then, with the real stack and a day of data: `docker compose run --rm spark python scripts/apply_live_ddl.py` (expect 15 tables ready), `docker compose run --rm spark python scripts/build_d1_marketing.py --date <a day that has data>`. Expected: `D-1 <date>: N campaign rows, M product rows` and rows visible in the Grafana page. (The Databricks read needs the real workspace; if it is unreachable, report that, do not fake rows.)

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

---

### Task 11: Complete end-to-end smoke test of the whole project

The product owner wants one command that assesses the full project, not only the inventory work: every layer from the simulator to Grafana, Power BI's source tables and Databricks. The result is a report, `docs/smoke-test.md`, with one row per stage (pass, fail or skipped, the evidence number, the seconds) and an overall verdict. A smoke test finds out what is broken; it never hides a failure, never retries a stage until it passes, and never edits data to make a check green.

**Files:**
- Create: `scripts/smoke_test.py`, `tests/test_smoke_test.py`, `docs/smoke-test.md` (written by a real run only)
- Modify: `tests/test_checks.py` (the two known failures), `docs/RUNNING.md` (a "Smoke test" section)

**Interfaces:**
- Consumes: everything above and the existing scripts: `scripts/run_simulator.py`, `streaming.jobs.bronze_ingest --once`, `scripts/seed_silver_reference.py`, `streaming.jobs.silver_clean --once`, `scripts/apply_live_ddl.py`, `scripts/build_inventory_daily.py`, `scripts/check_duplicates.py`, `scripts/run_batch.py`, `scripts/apply_ddl.py`, `scripts/publish_silver.py`, `monitoring/grafana/**`, `scripts/benchmark.py` (its reset and isolation approach).
- Produces: `python scripts/smoke_test.py [--events 20000] [--catalog workspace] [--skip-spark-tests] [--skip-gold] [--dry-run] [--yes]` with the same safety model as `scripts/benchmark.py`: `--dry-run` prints the numbered plan and exits 0; without `--yes` it exits 2 (it recreates the Kafka container, so all messages in Kafka are lost); it refuses (exit 3) when a demo container is running, when a path is outside `data/smoke`, or when the catalog is `funnelstream`. It writes only below `data/smoke`, into a separate Postgres database `funnelstream_smoke` (created if missing) and into the scratch catalog; the demo lake, the demo Postgres tables and the real catalog are never touched.

First make the host suite fully green: the two failures `tests/test_checks.py::test_expected_quarantine_is_the_sum_of_the_nine_kinds_and_ignores_the_rest` and `::test_the_repo_manifest_loads` read the git-ignored `noise_manifest.json`, which any simulator run rewrites. Change both to read a fixed fixture (a small manifest dict or file inside `tests/` with the numbers they expect, `duplicate=25`), so they no longer depend on the last run. Test first: show them failing when `noise_manifest.json` says 31, then passing with the fixture.

Stages (each is a function returning `Result(name, status, evidence, seconds)`; the runner continues after a failure and marks the stages that depend on it "skipped: <prerequisite> failed"):

1. **Preflight.** Docker engine answers; host UTC clock and `docker info` SystemTime agree within 2 seconds; `w32tm`-style NTP status is reported (informational); host free memory and the Docker memory limit are reported; a real Kafka client handshake from the host works (`kafka/create_topics.py` style, not just a TCP connect); Grafana `/api/health` answers 200 and the data source `fs-postgres` reports OK; Postgres answers; no `fs-` demo container is running.
2. **Host tests.** `pytest -q` on the host: zero failures and zero errors (skips allowed). Evidence: counts.
3. **Spark tests** (skippable). Every `tests/test_*spark*.py` file, one container at a time (`docker compose run --rm -e RUN_SPARK_TESTS=1 spark python -m pytest <file> -q -p no:cacheprovider`), each its own result row with passed and failed counts.
4. **Static checks.** `scripts/gen_ddl.py --check` exit 0; every provisioned dashboard JSON is valid JSON with unique panel ids; every alert rule in `alerting.yaml` has `condition: C` and a runbook; `monitoring/dashboards.py` builders equal the files on disk (the drift test).
5. **Reset and generate.** Delete `data/smoke`; recreate Kafka; create the topics (all four); create database `funnelstream_smoke`; apply the live DDL to it (`PG_DSN` pointed at it); run the simulator in history mode with `--inventory`, noise on, `--events N`, a unique seed. Evidence: messages per topic; the `inventory.events` count is above zero.
6. **Bronze and Silver.** `bronze_ingest --once`, `seed_silver_reference.py`, `silver_clean --once` against the isolated lake with `PG_DSN` pointing at the smoke database. Evidence: Bronze rows equal the simulator total; every Silver table exists and is non-empty, including `silver_stock_movements`, `silver_stock_snapshots` and `silver_express_orders`.
7. **Data invariants** (SQL and Spark reads on the smoke lake and database):
   - no duplicate key in any Silver table (`scripts/check_duplicates.py` exit 0);
   - quarantine equals the manifest's expected count for the kinds the validator can see (the rule of `docs/RUNNING.md` step 5);
   - stock: replaying all events never goes below zero; `live_stock_now.available >= 0`; for every pair the live rule (latest snapshot plus later movements) equals the Python ledger replay of the same events;
   - express: every express order has `promised_minutes`; the maximum delivered-minus-created time is below `promised_minutes`; zero breaches in the clean run;
   - live tables populated: `live_metrics_minute`, `pipeline_health_minute`, `live_stock_now`, `live_express_orders`; `pipeline_heartbeat` has `silver_job`.
8. **Daily marts.** `build_inventory_daily.py --date <the Cairo day of the data> --days 2` fills `inventory_daily`, `oos_demand_daily` and `express_daily`; evidence: row counts; the closing stock of the day equals the sum of `live_stock_now.on_hand` for the same pairs when the day is today.
9. **Dashboards run.** For every panel of every provisioned dashboard, take its `rawSql`, expand the Grafana macros (`$__timeFilter(col)` becomes `col >= now() - interval '1 day'`, `$__timeGroupAlias(col, '1m', 0)` becomes `date_trunc('minute', col) AS "time"`, `$__timeGroup` likewise) and execute it against the smoke database with the read-only role; every query must run without an error (zero rows is fine). Evidence: panels run, panels failed with the panel title and the error.
10. **Alert logic.** For every rule of `alerting.yaml`, take query A, run it against the smoke database and apply the rule's threshold: the clean run must leave `fs-express-sla` and `fs-oos-demand` not firing (or firing only when the data truly holds a hero stock-out, which the test states); then a second tiny simulation with `--express-promise-minutes 2 --late-express-every 1` and `--scenario hero-stockout` into the same smoke lake makes `fs-express-sla` fire on the breach and `fs-oos-demand` fire on the hero stock-out. Evidence: each rule, its value, firing or not.
11. **Replay safety.** Re-run `silver_clean --once` after deleting its checkpoint (a full replay of Bronze): table counts and the duplicate check are unchanged. This is the batch-level cousin of the failure demo.
12. **Databricks batch** (skippable with `--skip-gold`). `apply_ddl.py --backend databricks --catalog <scratch>`, `publish_silver.py`, `run_batch.py --skip-publish --full --manifest` against the scratch catalog: `BATCH OK` and every data quality check passes. If Databricks is unreachable the stage fails with the connector's message; it is not retried.
13. **Power BI source check.** For every Postgres table `dashboards/powerbi/MODEL.md` lists as a source and every Gold table it lists, assert the table exists with the columns the DAX in `dashboards/powerbi/measures.dax` references (parse `'Table'[column]` references and check them against `information_schema` on the smoke database or the Gold column list of `warehouse/tables.py`). A measure that names a missing column fails the stage with the measure's name.
14. **Report.** Write `docs/smoke-test.md`: the date, the command, the git commit, the environment (OS, Docker memory, clock offset, Python), one row per stage and per Spark file, the evidence, the failures in full, and a verdict line `SMOKE TEST PASSED` or `SMOKE TEST FAILED: <n> stages failed`. Exit code 0 only when nothing failed.

Tests (host, fake runner and fake clock, no Docker): the plan lists the stages in order; `--dry-run` runs nothing; the three refusals; a failing stage does not stop the later independent stages but skips its dependants with the reason; the macro expander gives the expected SQL for each Grafana macro used in `monitoring/dashboards.py`; the DAX reference parser finds `'Table'[column]` pairs in a sample and in `measures.dax` itself (every referenced table is one of the model's friendly names); the report renderer writes the verdict line and never a pass for a failed or skipped stage; the report contains no placeholder text.

Run the real smoke test only after Tasks 1 to 10 (and 9B) are verified and the product owner confirms the Kafka recreate. Commit only when told to commit: `Add the end-to-end smoke test`.

## Self-review

1. **Spec coverage.** Purpose and success criteria: Tasks 7, 8, 9, 10. Contracts (section 4): Tasks 1, 2. Store map (section 5): Task 3. Simulator model and scenario (section 6): Tasks 5, 6. Silver and live serving (section 7): Tasks 2, 4, 7. Operations dashboard and alert (section 8): Task 8. Marketing D-1 (section 9): Task 9. Testing (section 10): every task is test first; Spark tests per file. Order and risks (section 11): the header order and Task 9 as the cut candidate. Assumptions (section 12): written into Global Constraints and the Task 7 definitions. Not covered on purpose: the Databricks mirror and `agg_oos_demand_daily` (spec says only if time remains).
2. **Spec and plan agree.** The D-1 out-of-stock clicks are not computed from an hourly stock state: the plan reuses the demand the live sink records minute by minute in `live_oos_demand_minute` (all served demand, `campaign_id`, `landing_clicks`, `oos_at_write`), and spec sections 7 and 9 now say so. Both sides of the exposed-spend ratio come from that table. The cost: the flag is the stock when the minute was written, not replay-safe, which the dashboards and documents state as a limitation.
3. **Placeholders.** None: every step has code or an exact command. The only value that depends on running code is `DEFAULT_HERO_UNITS`, with a test that fails if it is wrong and a rule for fixing it (lower the constant, never the test).
4. **Type and name consistency.** `StockLedger` methods `opening/order/due/finish` are used identically in `iter_batch` (Task 5) and `add_inventory` (Task 6). `compute_stock_rows(spark, root, pairs, now)` and `compute_demand_rows(spark, root, minutes)` match between `live_stock.py`, the stubs in `test_live_stock_sink.py` and `LiveSink`. `DEMAND_KEYS` equals the table key in `live_tables.py` and the DDL. `stats["stock_pairs"]` (Task 2) is what `silver_clean.handle` passes to `on_batch(stock_pairs=...)` (Task 7).
5. **Review Focus.** All five lines have a test: replay (Task 2 Spark, Task 7), late movement (Task 7), unserved customers (Tasks 3 and 5), oversold clamp (Task 5), failing compute (Task 7).
