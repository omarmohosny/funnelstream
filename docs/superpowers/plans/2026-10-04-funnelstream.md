# FunnelStream Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the FunnelStream pipeline from the approved v3 spec: simulated app, order and delivery events through Kafka and Spark into Delta Lake (Bronze, Silver, Gold), a batch path for campaign and catalog data, Postgres serving Gold, and Power BI pages on top.

**Architecture:** Python simulator writes to Kafka. Spark Structured Streaming moves data Bronze → Silver → Gold and writes order and funnel Gold tables to Postgres. Airflow (or a Python fallback script) runs the batch path: ad snapshots, catalog SCD2, attribution, campaign daily, recommendations, city-product. Power BI reads Postgres in Import mode.

**Tech Stack:** Python 3.11, PySpark 3.5.1, delta-spark 3.2.0, Apache Kafka (KRaft), MinIO, PostgreSQL 16, Apache Airflow 2.9, Docker Compose, Power BI Desktop, pytest, confluent-kafka, psycopg 3.

**Spec:** `docs/superpowers/specs/2026-10-04-funnelstream-design.md` (v3). Metric formulas: `docs/metrics.md`.

## Global Constraints

- Currency is EGP. Column names end in `_egp` or are documented as EGP in the spec.
- All timestamps are stored in UTC. `event_date` is derived in `Africa/Cairo`.
- Processing is at-least-once with idempotent writes. Silver uses Delta `MERGE` on `event_id` (events) or the natural key. Gold upserts on its primary key. No task may claim exactly-once.
- Each Gold table has exactly one writer: the Databricks Gold build (spec section 16). The local Spark stream writes only Bronze, Silver and `live_metrics_minute` (local serving, not Gold).
- Current order status is the highest `status_rank`, never the latest arrival. `order_cancelled` is valid only before `order_shipped`.
- Gold holds no names or phone numbers. Power BI sees `customer_id` only.
- Kafka keys: `app.events` by `session_id`; `orders` and `delivery.events` by `order_id`.
- Required volume: pipeline runs at 1–2 million events. 10 million is a one-off benchmark.
- Python 3.11 only for this project. PySpark 3.5.1 does not support Python 3.14 (the machine default).
- Secrets live in `.env`. `.env` is git-ignored. `.env.example` lists names only.
- Pin versions in `requirements.txt`.

## Review Focus

1. **Late and out-of-order events:** an event 12 minutes late, and a `delivered` event that arrives before `order_shipped`. Expected: late row kept in Silver with `is_late = true`, excluded from live metrics; `current_status` stays `delivered`.
2. **Duplicates across a restart:** the same Kafka batch replayed after a job kill. Expected: no duplicate rows in Silver or Gold.
3. **Cancel after shipment:** an `order_cancelled` event after `order_shipped`. Expected: quarantined, order status unchanged.
4. **Zero spend or zero orders:** a campaign with zero spend or zero orders. Expected: CPA and ROAS are NULL, not a division error.
5. **Anonymous events and midnight in Cairo:** an app event with no `customer_id`, and an event at 23:30 UTC that is already the next day in Cairo. Expected: anonymous event kept keyed by `session_id`; `event_date` is the Cairo date.

Each line is covered by a test in the owning task (Tasks 4, 7, 8, 9, 12).

---

## Task 1: Repository scaffold, Python environment and Compose stack

**Files:**
- Create: `requirements.txt`, `pytest.ini`, `.env.example`, `.gitignore`, `docker-compose.yml`
- Create: `tests/test_compose.py`
- Create: `README.md` (placeholder section headings only; filled in Task 17)

**Interfaces:**
- Consumes: nothing
- Produces: service names used by every later task: `kafka:9092` (internal), `postgres:5432`, `minio:9000`. Database `funnelstream`, user `fs`, password from `.env`.

- [ ] **Step 1: Initialise the repository and Python 3.11 environment**

```bash
cd C:/Users/bahr/Desktop/Claude
git init
py -3.11 -m venv .venv
.venv/Scripts/activate
```

If `py -3.11` is missing, install Python 3.11 from python.org first. Java 17 is also required for Spark (`java -version` must print 17.x).

- [ ] **Step 2: Write `requirements.txt`**

```text
pyspark==3.5.1
delta-spark==3.2.0
confluent-kafka==2.4.0
psycopg[binary]==3.1.19
pyyaml==6.0.1
jsonschema==4.22.0
requests==2.32.3
pytest==8.2.2
```

Install: `pip install -r requirements.txt`

- [ ] **Step 3: Write `pytest.ini`**

```ini
[pytest]
pythonpath = .
testpaths = tests
```

- [ ] **Step 4: Write `.gitignore` and `.env.example`**

`.gitignore`:

```text
.venv/
.env
__pycache__/
*.pyc
samples/full/
data/
checkpoints/
```

`.env.example`:

```text
POSTGRES_USER=fs
POSTGRES_PASSWORD=change_me
POSTGRES_DB=funnelstream
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=change_me_too
```

- [ ] **Step 5: Write the failing test for memory limits**

`tests/test_compose.py`:

```python
import yaml
from pathlib import Path

COMPOSE = Path("docker-compose.yml")
REQUIRED = {"kafka", "postgres", "minio", "airflow"}


def load():
    return yaml.safe_load(COMPOSE.read_text())


def test_required_services_exist():
    services = load()["services"]
    assert REQUIRED.issubset(services.keys())


def test_every_service_has_memory_limit():
    services = load()["services"]
    for name, svc in services.items():
        assert "mem_limit" in svc, f"{name} has no mem_limit"


def test_postgres_is_16():
    assert load()["services"]["postgres"]["image"].startswith("postgres:16")
```

- [ ] **Step 6: Run the test to verify it fails**

Run: `pytest tests/test_compose.py -v`
Expected: FAIL with `FileNotFoundError` for `docker-compose.yml`.

- [ ] **Step 7: Write `docker-compose.yml`**

```yaml
name: funnelstream

services:
  kafka:
    image: apache/kafka:3.7.0
    mem_limit: 1g
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_RETENTION_HOURS: 168
    restart: unless-stopped

  postgres:
    image: postgres:16
    mem_limit: 512m
    ports: ["5432:5432"]
    env_file: .env
    volumes: ["pgdata:/var/lib/postgresql/data"]
    restart: unless-stopped

  minio:
    image: minio/minio:RELEASE.2024-06-13T22-53-53Z
    mem_limit: 512m
    command: server /data --console-address ":9001"
    ports: ["9000:9000", "9001:9001"]
    env_file: .env
    volumes: ["miniodata:/data"]
    restart: unless-stopped

  airflow:
    image: apache/airflow:2.9.2-python3.11
    mem_limit: 1g
    ports: ["8080:8080"]
    env_file: .env
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./batch:/opt/airflow/batch
      - ./simulator:/opt/airflow/simulator
    command: standalone
    restart: unless-stopped

volumes:
  pgdata:
  miniodata:
```

- [ ] **Step 8: Run the test to verify it passes**

Run: `pytest tests/test_compose.py -v`
Expected: 3 passed.

- [ ] **Step 9: Start the stack and check health**

Run: `docker compose up -d kafka postgres minio`
Run: `docker compose ps`
Expected: `kafka`, `postgres`, `minio` show `running`.

- [ ] **Step 10: Commit**

```bash
git add .
git commit -m "chore: scaffold repo, python env and compose stack"
```

---

## Task 2: Event contracts and validator

**Files:**
- Create: `contracts/schemas/event_envelope.json`, `contracts/schemas/order_event.json`, `contracts/topics.yaml`
- Create: `contracts/validate.py`
- Test: `tests/test_contracts.py`

**Interfaces:**
- Consumes: nothing
- Produces: `validate_event(event: dict) -> tuple[bool, str]` returns `(True, "")` or `(False, error_code)`. Error codes: `MISSING_FIELD`, `UNKNOWN_EVENT_TYPE`, `NEGATIVE_PRICE`, `FUTURE_TIMESTAMP`, `BAD_VERSION`.

- [ ] **Step 1: Write the failing tests**

`tests/test_contracts.py`:

```python
from datetime import datetime, timedelta, timezone
from contracts.validate import validate_event

NOW = datetime(2026, 10, 4, 12, 0, tzinfo=timezone.utc)


def base(event_type="product_view", **extra):
    e = {
        "event_id": "evt_1",
        "event_type": event_type,
        "event_version": 1,
        "event_time": NOW.isoformat(),
        "session_id": "sess_1",
        "device": "mobile",
        "channel": "paid_social",
        "country": "EG",
        "city": "Cairo",
        "payload": {"product_id": "prod_501", "price": 79.9},
    }
    e.update(extra)
    return e


def test_valid_event_passes():
    assert validate_event(base(), now=NOW) == (True, "")


def test_missing_session_fails():
    e = base()
    del e["session_id"]
    assert validate_event(e, now=NOW) == (False, "MISSING_FIELD")


def test_unknown_type_fails():
    assert validate_event(base("teleport"), now=NOW) == (False, "UNKNOWN_EVENT_TYPE")


def test_negative_price_fails():
    e = base()
    e["payload"]["price"] = -5
    assert validate_event(e, now=NOW) == (False, "NEGATIVE_PRICE")


def test_future_timestamp_fails():
    e = base(event_time=(NOW + timedelta(hours=2)).isoformat())
    assert validate_event(e, now=NOW) == (False, "FUTURE_TIMESTAMP")


def test_unknown_version_fails():
    assert validate_event(base(event_version=9), now=NOW) == (False, "BAD_VERSION")


def test_anonymous_event_without_customer_passes():
    e = base()
    assert "customer_id" not in e
    assert validate_event(e, now=NOW) == (True, "")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_contracts.py -v`
Expected: FAIL with `ModuleNotFoundError: contracts.validate`.

- [ ] **Step 3: Write `contracts/topics.yaml`**

```yaml
topics:
  app.events:      {partitions: 6, key: session_id, retention_hours: 72}
  orders:          {partitions: 3, key: order_id,   retention_hours: 168}
  delivery.events: {partitions: 3, key: order_id,   retention_hours: 168}
  app.events.dlq:      {partitions: 1, key: session_id, retention_hours: 336}
  orders.dlq:          {partitions: 1, key: order_id,   retention_hours: 336}
  delivery.events.dlq: {partitions: 1, key: order_id,   retention_hours: 336}

app_event_types:
  - app_open
  - page_view
  - product_view
  - product_click
  - product_view_end
  - product_search
  - add_to_cart
  - remove_from_cart
  - checkout_started
  - purchase

order_event_types:
  - order_created
  - payment_authorised
  - order_packed
  - order_shipped
  - order_cancelled
  - order_completed

delivery_event_types:
  - out_for_delivery
  - delivered
  - delivery_failed
```

- [ ] **Step 4: Write `contracts/validate.py`**

```python
from datetime import datetime, timedelta, timezone
import yaml
from pathlib import Path

_TOPICS = yaml.safe_load((Path(__file__).parent / "topics.yaml").read_text())
KNOWN_TYPES = set(
    _TOPICS["app_event_types"] + _TOPICS["order_event_types"] + _TOPICS["delivery_event_types"]
)
REQUIRED = ["event_id", "event_type", "event_version", "event_time", "session_id", "payload"]
SUPPORTED_VERSIONS = {1}
CLOCK_SKEW = timedelta(minutes=5)


def validate_event(event: dict, now: datetime | None = None) -> tuple[bool, str]:
    now = now or datetime.now(timezone.utc)
    for field in REQUIRED:
        if event.get(field) in (None, ""):
            return False, "MISSING_FIELD"
    if event["event_type"] not in KNOWN_TYPES:
        return False, "UNKNOWN_EVENT_TYPE"
    if event["event_version"] not in SUPPORTED_VERSIONS:
        return False, "BAD_VERSION"
    ts = datetime.fromisoformat(event["event_time"])
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    if ts > now + CLOCK_SKEW:
        return False, "FUTURE_TIMESTAMP"
    price = event["payload"].get("price")
    if price is not None and price < 0:
        return False, "NEGATIVE_PRICE"
    return True, ""
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_contracts.py -v`
Expected: 7 passed.

- [ ] **Step 6: Commit**

```bash
git add contracts tests/test_contracts.py
git commit -m "feat: add event contracts and validator"
```

---

## Task 3: Simulator, customer journeys

**Files:**
- Create: `simulator/__init__.py`, `simulator/core.py`, `simulator/catalog.py`
- Test: `tests/test_simulator_journeys.py`

**Interfaces:**
- Consumes: `contracts.validate.validate_event` (tests only)
- Produces:
  - `make_catalog(seed: int, n_products: int = 200) -> list[dict]` with keys `product_id, name, category, price, is_active`
  - `make_customers(seed: int, n: int) -> list[dict]` with keys `customer_id, country, city, device_primary, signup_date, marketing_consent`
  - `journey(rng, customer, catalog, campaign: dict | None, start: datetime) -> list[dict]` returns ordered app events (dicts matching the envelope in Task 2). Anonymous journeys set `customer_id` to None and omit it from the envelope.

Journey funnel rules: for every session, `product_view` ≥ `add_to_cart` ≥ `checkout_started` ≥ `purchase`. A session with a campaign carries `utm_campaign` and `utm_source` on its first `page_view` and `product_view` only.

- [ ] **Step 1: Write the failing tests**

`tests/test_simulator_journeys.py`:

```python
import random
from datetime import datetime, timezone
from simulator.catalog import make_catalog, make_customers
from simulator.core import journey
from contracts.validate import validate_event

START = datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc)
CAMPAIGN = {"campaign_id": "camp_202", "utm_campaign": "ramadan_offer_1", "utm_source": "meta", "channel": "paid_social"}


def _journeys(n=200, seed=7):
    catalog = make_catalog(seed)
    customers = make_customers(seed, 50)
    rng = random.Random(seed)
    out = []
    for i in range(n):
        cust = customers[i % len(customers)]
        camp = CAMPAIGN if i % 2 == 0 else None
        out.append(journey(rng, cust, catalog, camp, START))
    return out


def test_every_event_is_valid():
    for events in _journeys():
        for e in events:
            ok, code = validate_event(e, now=datetime(2026, 10, 5, tzinfo=timezone.utc))
            assert ok, code


def test_funnel_is_monotonic():
    for events in _journeys():
        counts = {}
        for e in events:
            counts[e["event_type"]] = counts.get(e["event_type"], 0) + 1
        assert counts.get("product_view", 0) >= counts.get("add_to_cart", 0)
        assert counts.get("add_to_cart", 0) >= counts.get("checkout_started", 0)
        assert counts.get("checkout_started", 0) >= counts.get("purchase", 0)


def test_session_id_is_constant_within_journey():
    for events in _journeys(50):
        assert len({e["session_id"] for e in events}) == 1


def test_utm_only_on_campaign_sessions_first_touch_events():
    for events in _journeys():
        with_utm = [e for e in events if "utm_campaign" in e["payload"]]
        for e in with_utm:
            assert e["event_type"] in ("page_view", "product_view")


def test_anonymous_journeys_have_no_customer_id():
    catalog = make_catalog(1)
    rng = random.Random(1)
    events = journey(rng, None, catalog, None, START)
    assert all("customer_id" not in e or e["customer_id"] is None for e in events)


def test_event_times_are_non_decreasing():
    for events in _journeys(30):
        times = [e["event_time"] for e in events]
        assert times == sorted(times)


def test_phone_format_is_egyptian():
    for c in make_customers(3, 20):
        phone = c["phone"]
        assert phone.startswith("+20")
        assert phone[3:5] in ("10", "11", "12", "15")
        assert len(phone) == 13
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_simulator_journeys.py -v`
Expected: FAIL with `ModuleNotFoundError: simulator.catalog`.

- [ ] **Step 3: Write `simulator/catalog.py`**

```python
import random
from datetime import date, timedelta

CATEGORIES = {
    "fashion": ["Galabeya", "Abaya", "Denim jacket", "Sneakers", "Hijab set"],
    "beauty": ["Argan oil", "Face serum", "Lipstick", "Kohl pencil"],
    "electronics": ["Earbuds", "Power bank", "Phone case", "Smart watch"],
    "home": ["Coffee set", "Bed sheets", "Towel set", "Water filter"],
}
FIRST_NAMES = ["Mohamed", "Ahmed", "Mariam", "Nour", "Youssef", "Hagar", "Omar", "Salma", "Karim", "Dina"]
LAST_NAMES = ["Hassan", "Mahmoud", "Ibrahim", "Abdelaziz", "Fathy", "Sayed", "Gamal", "Ragab"]
CITIES = [("Cairo", 0.30), ("Giza", 0.18), ("Alexandria", 0.14), ("Mansoura", 0.06), ("Tanta", 0.05), ("Assiut", 0.04), ("Other", 0.23)]
COUNTRY = "EG"


def make_catalog(seed: int, n_products: int = 200) -> list[dict]:
    rng = random.Random(seed)
    products = []
    cats = list(CATEGORIES)
    for i in range(n_products):
        cat = cats[i % len(cats)]
        base = {"fashion": 350, "beauty": 180, "electronics": 900, "home": 260}[cat]
        products.append({
            "product_id": f"prod_{i + 1:04d}",
            "name": f"{rng.choice(CATEGORIES[cat])} {i + 1}",
            "category": cat,
            "price": round(base * rng.uniform(0.6, 1.8), 2),
            "is_active": rng.random() > 0.05,
        })
    return products


def _weighted_city(rng: random.Random) -> str:
    r, acc = rng.random(), 0.0
    for city, w in CITIES:
        acc += w
        if r <= acc:
            return city
    return "Other"


def make_customers(seed: int, n: int) -> list[dict]:
    rng = random.Random(seed)
    out = []
    for i in range(n):
        prefix = rng.choice(["10", "11", "12", "15"])
        phone = "+20" + prefix + "".join(str(rng.randint(0, 9)) for _ in range(8))
        out.append({
            "customer_id": f"cust_{i + 1:06d}",
            "name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
            "phone": phone,
            "country": COUNTRY,
            "city": _weighted_city(rng),
            "device_primary": rng.choice(["mobile", "mobile", "mobile", "desktop"]),
            "signup_date": (date(2025, 1, 1) + timedelta(days=rng.randint(0, 600))).isoformat(),
            "marketing_consent": rng.random() < 0.6,
        })
    return out
```

- [ ] **Step 4: Write `simulator/core.py`**

```python
import uuid
from datetime import timedelta


def _event(event_type, t, session_id, customer_id, device, channel, country, city, payload):
    e = {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_type": event_type,
        "event_version": 1,
        "event_time": t.isoformat(),
        "session_id": session_id,
        "device": device,
        "channel": channel,
        "country": country,
        "city": city,
        "payload": payload,
    }
    if customer_id is not None:
        e["customer_id"] = customer_id
    return e


def journey(rng, customer, catalog, campaign, start):
    """Build one session of app events. Funnel drop-off happens at each step."""
    session_id = f"sess_{uuid.uuid4().hex[:10]}"
    customer_id = customer["customer_id"] if customer else None
    device = customer["device_primary"] if customer else rng.choice(["mobile", "desktop"])
    city = customer["city"] if customer else "Cairo"
    country = "EG"
    channel = campaign["channel"] if campaign else rng.choice(["organic", "direct", "email"])
    t = start
    events = []

    def add(etype, payload, dt=5):
        nonlocal t
        t = t + timedelta(seconds=rng.randint(dt, dt * 4))
        events.append(_event(etype, t, session_id, customer_id, device, channel, country, city, payload))

    add("app_open", {})
    first_page = {"page": "home"}
    if campaign:
        first_page.update({"utm_campaign": campaign["utm_campaign"], "utm_source": campaign["utm_source"]})
    add("page_view", first_page)

    n_views = rng.randint(1, 6)
    viewed = rng.sample(catalog, n_views)
    for i, product in enumerate(viewed):
        pv = {"product_id": product["product_id"], "price": product["price"], "category": product["category"]}
        if i == 0 and campaign:
            pv.update({"utm_campaign": campaign["utm_campaign"], "utm_source": campaign["utm_source"]})
        add("product_view", pv)
        if rng.random() < 0.5:
            add("product_click", {"product_id": product["product_id"], "element": rng.choice(["image", "size", "color", "reviews"])})
        add("product_view_end", {"product_id": product["product_id"], "dwell_seconds": rng.randint(8, 240)})

    if rng.random() < 0.35:
        item = rng.choice(viewed)
        add("add_to_cart", {"product_id": item["product_id"], "quantity": 1, "cart_id": f"cart_{session_id[5:11]}"})
        if rng.random() < 0.45:
            add("checkout_started", {"cart_id": f"cart_{session_id[5:11]}"})
            if rng.random() < 0.7:
                add("purchase", {"order_id": f"ord_{uuid.uuid4().hex[:8]}", "total": item["price"]})
    return events
```

- [ ] **Step 5: Write `simulator/__init__.py`**

Empty file.

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_simulator_journeys.py -v`
Expected: 7 passed.

- [ ] **Step 7: Commit**

```bash
git add simulator tests/test_simulator_journeys.py
git commit -m "feat: simulator customer journeys with funnel drop-off"
```

---

## Task 4: Simulator, orders and delivery lifecycle

**Files:**
- Create: `simulator/orders.py`
- Create: `streaming/common/status.py` (shared status rank, used by Tasks 8 and 9)
- Test: `tests/test_orders_lifecycle.py`

**Interfaces:**
- Consumes: `make_catalog`, `make_customers` (Task 3)
- Produces:
  - `STATUS_RANK: dict[str, int]` (in `streaming/common/status.py`)
  - `resolve_current_status(statuses: list[str]) -> str` returns the status with the highest rank; `order_cancelled` only counts if no `order_shipped` exists in the list
  - `make_order(rng, customer, catalog, campaign, t, order_id) -> list[dict]` returns `order_created` on `orders` and any delivery events on `delivery.events`, as `(topic, event)` tuples

- [ ] **Step 1: Write the failing tests**

`tests/test_orders_lifecycle.py`:

```python
import random
from datetime import datetime, timezone
from simulator.catalog import make_catalog, make_customers
from simulator.orders import make_order
from streaming.common.status import STATUS_RANK, resolve_current_status

T0 = datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc)


def _orders(n=300, seed=5):
    catalog = make_catalog(seed)
    customers = make_customers(seed, 40)
    rng = random.Random(seed)
    return [make_order(rng, customers[i % 40], catalog, None, T0, f"ord_{i:05d}") for i in range(n)]


def test_rank_is_strictly_ordered_on_main_path():
    path = ["order_created", "payment_authorised", "order_packed", "order_shipped",
            "out_for_delivery", "delivered", "order_completed"]
    ranks = [STATUS_RANK[s] for s in path]
    assert ranks == sorted(ranks)
    assert len(set(ranks[:4])) == 4


def test_resolve_ignores_arrival_order():
    assert resolve_current_status(["delivered", "order_shipped", "order_created"]) == "delivered"


def test_cancel_before_shipment_wins():
    assert resolve_current_status(["order_created", "order_cancelled"]) == "order_cancelled"


def test_cancel_after_shipment_is_not_current():
    assert resolve_current_status(["order_created", "order_shipped", "order_cancelled"]) == "order_shipped"


def test_every_delivered_order_was_shipped():
    for events in _orders():
        types = [e["event_type"] for _, e in events if e["event_type"] in ("order_shipped", "delivered")]
        if "delivered" in types:
            assert "order_shipped" in types


def test_failed_attempts_are_followed_by_retry_or_end():
    for events in _orders():
        seq = [e["event_type"] for _, e in events if e["event_type"].startswith("delivery") or e["event_type"] in ("delivered", "delivery_failed", "out_for_delivery")]
        for i, s in enumerate(seq):
            if s == "delivery_failed":
                assert i + 1 == len(seq) or seq[i + 1] == "out_for_delivery"


def test_topics_are_routed_correctly():
    for events in _orders(50):
        for topic, e in events:
            if e["event_type"].startswith("order_") or e["event_type"] == "payment_authorised":
                assert topic == "orders"
            else:
                assert topic == "delivery.events"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_orders_lifecycle.py -v`
Expected: FAIL with `ModuleNotFoundError: streaming.common.status`.

- [ ] **Step 3: Write `streaming/common/status.py`**

```python
STATUS_RANK = {
    "order_created": 1,
    "payment_authorised": 2,
    "order_packed": 3,
    "order_shipped": 4,
    "out_for_delivery": 5,
    "delivery_failed": 5,
    "delivered": 6,
    "order_completed": 7,
    "order_cancelled": 2,  # valid only before order_shipped; see resolve_current_status
}


def resolve_current_status(statuses: list[str]) -> str:
    """Highest-rank status. A cancel counts only if the order never shipped."""
    shipped = "order_shipped" in statuses
    candidates = [s for s in statuses if not (s == "order_cancelled" and shipped)]
    return max(candidates, key=lambda s: STATUS_RANK[s])
```

- [ ] **Step 4: Write `simulator/orders.py`**

```python
import uuid
from datetime import timedelta


def _ev(etype, t, order_id, customer, payload):
    return {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_type": etype,
        "event_version": 1,
        "event_time": t.isoformat(),
        "session_id": None,
        "order_id": order_id,
        "customer_id": customer["customer_id"],
        "country": "EG",
        "city": customer["city"],
        "payload": payload,
    }


def make_order(rng, customer, catalog, campaign, t, order_id):
    """Return a list of (topic, event) tuples for one order's lifecycle."""
    items = rng.sample(catalog, rng.randint(1, 3))
    lines = [{"product_id": p["product_id"], "quantity": q, "unit_price": p["price"],
              "line_total": round(p["price"] * q, 2)}
             for p in items for q in [rng.randint(1, 2)]]
    total = round(sum(l["line_total"] for l in lines), 2)
    payment = rng.choices(["cod", "fawry", "vodafone_cash", "instapay", "card"],
                          weights=[55, 12, 10, 8, 15])[0]
    carrier = rng.choice(["bosta", "aramex", "jt_express"])
    created = {"order_id": order_id, "payment_method": payment, "items": lines, "order_total": total,
               "channel": campaign["channel"] if campaign else "organic",
               "utm_source": campaign["utm_source"] if campaign else None,
               "utm_campaign": campaign["utm_campaign"] if campaign else None}
    out = [("orders", _ev("order_created", t, order_id, customer, created))]

    if rng.random() < 0.05:  # cancel before shipment
        t2 = t + timedelta(minutes=rng.randint(5, 180))
        out.append(("orders", _ev("order_cancelled", t2, order_id, customer, {"reason": "customer_request"})))
        return out

    t += timedelta(minutes=rng.randint(10, 60))
    out.append(("orders", _ev("payment_authorised", t, order_id, customer, {"payment_method": payment})))
    t += timedelta(hours=rng.randint(2, 12))
    out.append(("orders", _ev("order_packed", t, order_id, customer, {})))
    t += timedelta(hours=rng.randint(2, 10))
    out.append(("orders", _ev("order_shipped", t, order_id, customer, {"carrier": carrier})))

    attempts = 1 if rng.random() > 0.2 else rng.randint(2, 3)
    for a in range(attempts):
        t += timedelta(hours=rng.randint(4, 24))
        out.append(("delivery.events", _ev("out_for_delivery", t, order_id, customer, {"carrier": carrier, "attempt": a + 1})))
        last = a == attempts - 1
        if last and rng.random() < 0.9 or (not last and rng.random() < 0.3):
            pass
        if (a < attempts - 1) or (rng.random() < 0.9):
            if a < attempts - 1:
                t += timedelta(hours=1)
                out.append(("delivery.events", _ev("delivery_failed", t, order_id, customer, {"reason": "no_answer", "attempt": a + 1})))
                continue
            t += timedelta(hours=rng.randint(1, 6))
            out.append(("delivery.events", _ev("delivered", t, order_id, customer, {"carrier": carrier})))
            t += timedelta(hours=1)
            out.append(("orders", _ev("order_completed", t, order_id, customer, {})))
        else:
            t += timedelta(hours=1)
            out.append(("delivery.events", _ev("delivery_failed", t, order_id, customer, {"reason": "refused", "attempt": a + 1})))
    return out
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_orders_lifecycle.py -v`
Expected: 7 passed. If `test_failed_attempts_are_followed_by_retry_or_end` fails, the generator branch is wrong; fix the loop so that every `delivery_failed` is followed by `out_for_delivery` or ends the sequence.

- [ ] **Step 6: Commit**

```bash
git add simulator/orders.py streaming/common/status.py tests/test_orders_lifecycle.py
git commit -m "feat: order and delivery lifecycle with status rank"
```

---

## Task 5: Mock ad API and campaign dataset

**Files:**
- Create: `simulator/campaigns.py` (campaign dataset, cumulative per day)
- Create: `mock_apis/ad_api.py` (stdlib HTTP server)
- Test: `tests/test_ad_api.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `campaign_numbers(campaign_id: str, report_date: date, as_of: datetime, seed: int) -> dict` with keys `campaign_id, report_date, pulled_at, impressions, clicks, spend, currency` (`currency = "EGP"`). Numbers are cumulative from 00:00 UTC of `report_date` up to `as_of`, and never decrease as `as_of` increases.
  - HTTP `GET /campaigns?report_date=YYYY-MM-DD&as_of=ISO8601` returns JSON list, one entry per campaign in `CAMPAIGNS`.

- [ ] **Step 1: Write the failing tests**

`tests/test_ad_api.py`:

```python
from datetime import date, datetime, timezone
from simulator.campaigns import campaign_numbers, CAMPAIGNS


def test_cumulative_never_decreases_within_day():
    d = date(2026, 10, 4)
    earlier = campaign_numbers("camp_202", d, datetime(2026, 10, 4, 10, tzinfo=timezone.utc), seed=1)
    later = campaign_numbers("camp_202", d, datetime(2026, 10, 4, 18, tzinfo=timezone.utc), seed=1)
    assert later["impressions"] >= earlier["impressions"]
    assert later["clicks"] >= earlier["clicks"]
    assert later["spend"] >= earlier["spend"]


def test_clicks_never_exceed_impressions():
    for c in CAMPAIGNS:
        n = campaign_numbers(c["campaign_id"], date(2026, 10, 4), datetime(2026, 10, 4, 23, tzinfo=timezone.utc), seed=2)
        assert n["clicks"] <= n["impressions"]


def test_spend_is_non_negative_and_egp():
    n = campaign_numbers("camp_202", date(2026, 10, 4), datetime(2026, 10, 4, 12, tzinfo=timezone.utc), seed=3)
    assert n["spend"] >= 0
    assert n["currency"] == "EGP"


def test_same_inputs_give_same_numbers():
    a = campaign_numbers("camp_202", date(2026, 10, 4), datetime(2026, 10, 4, 12, tzinfo=timezone.utc), seed=4)
    b = campaign_numbers("camp_202", date(2026, 10, 4), datetime(2026, 10, 4, 12, tzinfo=timezone.utc), seed=4)
    assert a == b
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_ad_api.py -v`
Expected: FAIL with `ModuleNotFoundError: simulator.campaigns`.

- [ ] **Step 3: Write `simulator/campaigns.py`**

```python
import random
from datetime import date, datetime, timezone

CAMPAIGNS = [
    {"campaign_id": "camp_201", "utm_campaign": "eid_sale_1", "utm_source": "google", "platform": "google", "channel": "paid_search", "daily_budget": 1500.0},
    {"campaign_id": "camp_202", "utm_campaign": "ramadan_offer_1", "utm_source": "meta", "platform": "meta", "channel": "paid_social", "daily_budget": 2000.0},
    {"campaign_id": "camp_203", "utm_campaign": "retarget_cart_1", "utm_source": "meta", "platform": "meta", "channel": "paid_social", "daily_budget": 600.0},
]


def campaign_numbers(campaign_id: str, report_date: date, as_of: datetime, seed: int) -> dict:
    camp = next(c for c in CAMPAIGNS if c["campaign_id"] == campaign_id)
    day_start = datetime(report_date.year, report_date.month, report_date.day, tzinfo=timezone.utc)
    frac = max(0.0, min(1.0, (as_of - day_start).total_seconds() / 86400))
    rng = random.Random(f"{seed}-{campaign_id}-{report_date.isoformat()}")
    cpm = rng.uniform(18, 35)
    ctr = rng.uniform(0.01, 0.04)
    full_spend = camp["daily_budget"] * rng.uniform(0.85, 1.05)
    impressions = int(full_spend / cpm * 1000 * frac)
    clicks = int(impressions * ctr)
    return {
        "campaign_id": campaign_id,
        "report_date": report_date.isoformat(),
        "pulled_at": as_of.isoformat(),
        "impressions": impressions,
        "clicks": clicks,
        "spend": round(full_spend * frac, 4),
        "currency": "EGP",
    }
```

- [ ] **Step 4: Write `mock_apis/ad_api.py`**

```python
import json
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from simulator.campaigns import CAMPAIGNS, campaign_numbers

SEED = 42


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        if url.path != "/campaigns":
            self.send_response(404)
            self.end_headers()
            return
        q = parse_qs(url.query)
        report_date = date.fromisoformat(q["report_date"][0])
        as_of = datetime.fromisoformat(q["as_of"][0]).replace(tzinfo=timezone.utc) if "as_of" in q else datetime.now(timezone.utc)
        body = [campaign_numbers(c["campaign_id"], report_date, as_of, SEED) for c in CAMPAIGNS]
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_ad_api.py -v`
Expected: 4 passed.

- [ ] **Step 6: Start the mock and check one call**

Run: `python mock_apis/ad_api.py` in one terminal.
Run: `curl "http://localhost:8000/campaigns?report_date=2026-10-04&as_of=2026-10-04T12:00:00"`
Expected: JSON list with 3 campaigns and `pulled_at` set to 12:00.

- [ ] **Step 7: Commit**

```bash
git add simulator/campaigns.py mock_apis tests/test_ad_api.py
git commit -m "feat: mock ad API with cumulative daily numbers"
```

---

## Task 6: Kafka topics and idempotent producer

**Files:**
- Create: `kafka/create_topics.py`
- Create: `simulator/publish.py`
- Create: `scripts/run_simulator.py`
- Test: `tests/test_publish.py`

**Interfaces:**
- Consumes: `contracts/topics.yaml` (Task 2), events from Tasks 3–4
- Produces:
  - `route(topic_hint: str, event: dict) -> tuple[str, str]` returns `(topic, key)` where key is `session_id` for app events and `order_id` for order and delivery events
  - `Publisher(bootstrap: str)` with `.send(topic, key, event)` and `.flush()`. Producer config: `acks=all`, `enable.idempotence=True`, `linger.ms=20`.
  - `scripts/run_simulator.py --events N --seed S --bootstrap` generates journeys and orders, publishes, prints a count per topic.

- [ ] **Step 1: Write the failing tests**

`tests/test_publish.py`:

```python
from simulator.publish import route
from simulator.publish import build_producer_config


def test_app_events_keyed_by_session():
    topic, key = route("app", {"event_type": "page_view", "session_id": "sess_1"})
    assert (topic, key) == ("app.events", "sess_1")


def test_order_events_keyed_by_order():
    topic, key = route("order", {"event_type": "order_created", "order_id": "ord_9"})
    assert (topic, key) == ("orders", "ord_9")


def test_delivery_events_keyed_by_order():
    topic, key = route("order", {"event_type": "delivered", "order_id": "ord_9"})
    assert (topic, key) == ("delivery.events", "ord_9")


def test_producer_is_idempotent_and_acks_all():
    cfg = build_producer_config("kafka:9092")
    assert cfg["enable.idempotence"] is True
    assert cfg["acks"] == "all"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_publish.py -v`
Expected: FAIL with `ModuleNotFoundError: simulator.publish`.

- [ ] **Step 3: Write `simulator/publish.py`**

```python
import json
from confluent_kafka import Producer


def route(source: str, event: dict) -> tuple[str, str]:
    """source is 'app' for app/web events, 'order' for order and delivery events."""
    if source == "app":
        return "app.events", event["session_id"]
    if event["event_type"] in ("out_for_delivery", "delivered", "delivery_failed"):
        return "delivery.events", event["order_id"]
    return "orders", event["order_id"]


def build_producer_config(bootstrap: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
    }


class Publisher:
    def __init__(self, bootstrap: str):
        self._p = Producer(build_producer_config(bootstrap))

    def send(self, topic: str, key: str, event: dict) -> None:
        self._p.produce(topic, key=key.encode(), value=json.dumps(event).encode())
        self._p.poll(0)

    def flush(self) -> None:
        self._p.flush()
```

- [ ] **Step 4: Write `kafka/create_topics.py`**

```python
import yaml
from pathlib import Path
from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = yaml.safe_load((Path(__file__).parent.parent / "contracts" / "topics.yaml").read_text())["topics"]


def create(bootstrap: str = "localhost:9092") -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    new = [NewTopic(name, num_partitions=cfg["partitions"], replication_factor=1,
                    config={"retention.ms": str(cfg["retention_hours"] * 3600 * 1000)})
           for name, cfg in TOPICS.items()]
    for name, fut in admin.create_topics(new).items():
        try:
            fut.result()
            print("created", name)
        except Exception as e:
            print("skip", name, e)


if __name__ == "__main__":
    create()
```

- [ ] **Step 5: Write `scripts/run_simulator.py`**

```python
import argparse
import random
from collections import Counter
from datetime import datetime, timedelta, timezone

from simulator.catalog import make_catalog, make_customers
from simulator.core import journey
from simulator.orders import make_order
from simulator.campaigns import CAMPAIGNS
from simulator.publish import Publisher, route


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=100_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--start", default="2026-10-04T00:00:00+00:00")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    catalog = make_catalog(args.seed)
    customers = make_customers(args.seed, 5_000)
    pub = Publisher(args.bootstrap)
    t = datetime.fromisoformat(args.start)
    counts = Counter()
    n = 0
    order_no = 0
    while n < args.events:
        t += timedelta(seconds=rng.randint(1, 3))
        cust = rng.choice(customers) if rng.random() < 0.85 else None
        camp = rng.choice(CAMPAIGNS) if rng.random() < 0.4 else None
        camp_dict = {"channel": "paid_social" if camp is None else camp["channel"],
                     "utm_source": camp["utm_source"] if camp else None,
                     "utm_campaign": camp["utm_campaign"] if camp else None} if camp else None
        for e in journey(rng, cust, catalog, camp_dict, t):
            topic, key = route("app", e)
            pub.send(topic, key, e)
            counts[topic] += 1
            n += 1
        if rng.random() < 0.3:
            order_no += 1
            for topic, e in make_order(rng, cust or customers[0], catalog, camp_dict, t, f"ord_{order_no:08d}"):
                pub_topic, key = route("order", e)
                pub.send(pub_topic, key, e)
                counts[pub_topic] += 1
                n += 1
    pub.flush()
    print(dict(counts), "total", n)


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_publish.py -v`
Expected: 4 passed.

- [ ] **Step 7: Create topics and publish a small run**

Run: `python kafka/create_topics.py`
Expected: lines `created app.events`, `created orders`, etc.
Run: `python scripts/run_simulator.py --events 5000`
Expected: a dict of counts per topic, total ≥ 5000.
Run: `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 2`
Expected: two JSON `order_created`/`payment_authorised` lines.

- [ ] **Step 8: Commit**

```bash
git add kafka simulator/publish.py scripts/run_simulator.py tests/test_publish.py
git commit -m "feat: kafka topics, idempotent publisher, simulator runner"
```

---

## Task 6b: Noise injection (added after review; spec section 4 "Injected noise")

**Files:**
- Create: `simulator/noise.py`
- Modify: `scripts/run_simulator.py` (apply noise between `iter_batch` and publish; write `noise_manifest.json`)
- Test: `tests/test_noise.py`

**Interfaces:**
- Consumes: `(topic_hint, event)` stream from `iter_batch` (Task 6), `make_catalog` ids (Task 3)
- Produces:
  - `NoiseConfig` dataclass with one rate field per row of the spec noise table (defaults as in the spec) and `NoiseConfig.off()` returning all zeros
  - `apply_noise(stream, rng, config, catalog_ids) -> Iterator[tuple[str, str | dict]]` yields `(topic_hint, payload)` where payload is a dict, or a raw string for malformed JSON; also yields duplicate copies
  - `NoiseCounter` with `.counts: dict[str, int]` and `.total`, written by the runner to `noise_manifest.json` as `{"total_messages": N, "injected": {"duplicate": n, ...}}`

Rules: all randomness comes from the passed `rng`, so a fixed seed gives identical noise. With `NoiseConfig.off()` the output equals the input. Noise never mutates the input event dict in place. Every injected change increments exactly one counter.

- [ ] **Step 1: Write failing tests** covering: off() leaves the stream identical; same seed twice gives identical output and identical manifest; duplicate rate lands within 0.5 to 1.5 percent over 20,000 events; malformed rows are strings that `json.loads` rejects; each validator-visible noise type (missing field, unknown type, negative price, future timestamp, bad version) makes `validate_event` return the expected error code; late events have `event_time` at least 10 minutes older than the publish clock; dirty text changes only `city` or `device` casing/spacing; unknown `product_id` is absent from the catalog ids; input events are not mutated.
- [ ] **Step 2: Run tests, verify they fail.** `.venv\Scripts\python -m pytest tests/test_noise.py -q`
- [ ] **Step 3: Implement `simulator/noise.py` and wire the runner.** Add CLI flags `--noise-off` and `--noise-scale F` (multiplies every rate, default 1.0).
- [ ] **Step 4: Run the full suite; commit and push.**

Downstream handling is part of later tasks, and their tests must use noisy input:
- Task 8 (Silver): quarantine reasons `BAD_JSON`, `UNKNOWN_PRODUCT`; standardise `city` and `device`; normalise timestamps to UTC; cast numeric strings; flag `is_bot` (more than 100 events in 10 seconds per session); wrap `validate_event` so a bad timestamp or non-dict payload maps to `BAD_VALUE` instead of raising.
- Task 9 (Gold): exclude `is_bot` sessions from funnel and attribution; guard `resolve_current_status` against empty or unknown statuses.
- Task 12 (campaigns): the mock API gets optional noise (duplicate pull, `clicks > impressions`, negative spend, 503); the batch retries 503 and keeps the latest pull.
- Task 16 (SQL checks): add a check that Silver quarantine rate and duplicate count match `noise_manifest.json` within tolerance.

---

## Amendment A: Databricks hybrid (replaces MinIO; ruled 2026-10-04, spec section 3)

Binding changes to the tasks below. Where an older task text says MinIO, `s3a://`, or "Postgres serves Gold", this amendment wins.

- **Task 1c (new, small, do first):** remove the `minio` service and `miniodata` volume from `docker-compose.yml`, remove MinIO variables from `.env.example`, update `tests/test_compose.py` (`REQUIRED = {"kafka", "postgres", "airflow"}`), keep the memory-limit test. Add `data/` to the layout (already git-ignored).
- **Task 7:** Bronze path `data/lake/bronze/<table>`, checkpoint `data/checkpoints/<job>`. No `s3a://`, no hadoop-aws. `run()` takes local paths.
- **Task 8:** Silver path `data/lake/silver/<table>`. Use `contracts.validate.validate_raw`, `contracts.normalize` and `streaming.common.bots.find_bot_sessions` as ruled in the ledger.
- **Task 9:** the Gold builders stay pure Spark functions (`build_fact_orders`, `build_fact_order_lines`, `build_fact_funnel_daily`, `build_live_metrics_minute`). They run on Databricks over the Silver mirror, except `build_live_metrics_minute`, which also runs locally. No streaming writer to Postgres.
- **Task 10:** `warehouse/ddl.sql` stays as the Postgres fallback schema. Add the Databricks DDL `warehouse/databricks_ddl.sql` (same tables in schema `funnelstream.gold`).
- **Task 10b (new, after Task 10):** `warehouse/store.py` with `GoldStore` (abstract `upsert(table, rows, key_cols) -> int`), `PostgresStore` (wraps Task 10's `upsert`), `DatabricksStore` (databricks-sql-connector, `MERGE INTO` on the key columns, reads `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID` from the environment) and `get_store()` choosing the backend from env `GOLD_BACKEND` (`databricks` or `postgres`, default `postgres`). Add `scripts/publish_silver.py` that exports local Silver Delta tables to Parquet and uploads them to a Databricks volume, then runs `COPY INTO`/`MERGE` into the Silver mirror. Tests use a fake connection (no network): MERGE statement text per table, idempotence, backend selection, missing env raises a clear error. Add `databricks-sql-connector` and `databricks-sdk` to `requirements.txt`.
- **Task 12, 13, 14, 15:** all writes go through `get_store()`; none imports psycopg directly.
- **Task 16:** SQL checks run through the store's `query()` (add `query(sql) -> list[tuple]` to `GoldStore`); Databricks SQL dialect differences are covered by writing checks in ANSI SQL.
- **Task 17:** `export_samples` reads from the store; the failure demo runs the local stream job only.
- **Task 18:** Power BI connects with the Databricks connector (fallback: PostgreSQL connector).
- **Prerequisite (human):** create a Databricks workspace, a SQL warehouse, and a personal access token; put the three values in `.env`. The code is built and tested without them (fake connection), so the build is not blocked.

---

## Amendment B: compatibility rulings (supersede older task text)

The authoritative list is spec section 16 ("Compatibility rulings"). Task-level consequences:

- **Task 1:** built, superseded by Task 1c. **Task 2:** built; `contracts/validate.py` as built (with `validate_raw`, `BAD_VALUE`, `known_products`) is the authority, not the code in this plan.
- **Task 1d (new, small, do before Task 7):** `docker-compose.yml` Kafka listeners (`EXTERNAL://localhost:9092` for the host, `INTERNAL://kafka:19092` inside Docker); Airflow service gets `_PIP_ADDITIONAL_REQUIREMENTS` (databricks-sql-connector requests pyarrow), mounts for `warehouse`, `contracts`, `data`, and env `AD_API_URL=http://host.docker.internal:8000`; `requirements.txt` adds `databricks-sql-connector`, `databricks-sdk`, `pyarrow`; `.env.example` adds `PG_DSN`, `AD_API_URL`, `SIM_SEED=42`, `DATABRICKS_CATALOG=funnelstream`, `DATABRICKS_VOLUME=silver_upload`. Tests: compose listeners, mounts, env names present.
- **Task 7:** three Bronze tables, one streaming query per topic, local paths `data/lake/bronze/<table>`, checkpoints `data/checkpoints/<job>`. Spark runs in the `spark` Compose service (Dockerfile at `infrastructure/docker/spark/Dockerfile`, profile `spark`, `docker compose run --rm spark ...`), because Spark needs winutils on Windows; Spark tests carry the pytest marker `spark` and are skipped on the Windows host. Replace the `s3a://` text in the interface block.
- **Task 8 (rewrite):** use `validate_raw(raw, known_products=...)`, `contracts.normalize`, and flatten the payload into the Silver columns of spec section 7; produce `silver_events` (with `is_late`, `is_bot` placeholder false until the Gold build recomputes it), `silver_orders`, `silver_order_status`, `silver_order_items`, `silver_quarantine` (all codes in spec section 16.3). `silver_products` is loaded first and passed as `known_products`. Cancel after shipment goes to quarantine here. No Map<string,string> payload.
- **Task 9:** remove the `is_late` filter from the funnel builder (keep it only in `build_live_metrics_minute`); exclude `is_bot`; Cairo dates; `live_metrics_minute` is local serving at `data/lake/live/live_metrics_minute`, not Gold, with `scripts/show_live.py` printing the latest minutes. Builders stay as Spark reference implementations; production Gold is the SQL in Task 10c.
- **Task 10:** keep `warehouse/ddl.sql` as the Postgres fallback; add `warehouse/databricks_ddl.sql` with the Delta types of spec 16.8; drop `live_metrics_minute` from both.
- **Task 10b:** `GoldStore` has `upsert` and `query`; `publish_silver` uses a column allow-list.
- **Task 10c (new, after 10b):** `warehouse/gold_build.py` and `warehouse/gold/*.sql` (MERGE ... SELECT) in the order of spec 16.2, plus loads of `silver_campaign_snapshots`, `silver_customers`, `silver_products`, `dim_campaign`, `dim_date` (with `is_ramadan`, `is_eid`, `is_friday_saturday`); `dim_campaign` rows come from `simulator/campaigns.CAMPAIGNS`; seed from `SIM_SEED`. Tests with a fake store check statement order and idempotence; a parity test runs the Python references (Tasks 12 to 14) and the SQL on a tiny dataset using Postgres-compatible SQL where possible, otherwise compares the Python reference to hand-computed expected rows.
- **Task 11:** `INFINITY = datetime(9999, 12, 31, tzinfo=utc)`; `load_dimensions(store, ...)` uses `store.query`/`store.upsert` and MERGE on `(customer_id, valid_from)`.
- **Task 12:** `pull` retries HTTP 503 with backoff (3 tries); `campaign_daily` treats placed as all attributed orders; campaign Silver quarantines `NEGATIVE_SPEND` and `CLICKS_GT_IMPRESSIONS`.
- **Task 14:** `city_product` counts distinct sessions.
- **Task 15:** `run_batch(report_date, as_of, api_url, store=None)`; no `pg_dsn`, no psycopg, DAG reads `AD_API_URL`; it calls publish status check then the Gold build.
- **Task 16:** `run_checks(store, checks)` uses `store.query`; add the manifest check type and the reported negative-lead-time check; remove `checks.sql` or generate it from `CHECKS`.
- **Task 17:** `export_samples` and the failure demo use the store and local Silver Delta as in spec 16.11.
- **Task 18:** Databricks connector steps: server hostname and HTTP path from the SQL warehouse, read-only token, relationship on `customer_key`/`product_key`.

---

## Amendment C: model fixes and live monitoring (supersedes older task text; spec section 17)

Task order from here: **C1** simulator and contracts, **C2** Silver, **C3** Gold model (renames, unknown members, new columns, builders, DDL), **10c** Gold build SQL, **G1** live and health tables in Postgres, **G2** Grafana (provisioning, alerts, Compose changes), **G3** failure demo, then Tasks 11 to 19 (recommendations use distinct sessions; attribution per spec 17.11; the DAG is optional).

- **C1:** `order_returned` event type and rank 7 (contracts/topics.yaml, validator, STATUS_RANK, simulator); COD orders skip `payment_authorised`; add `platform` (`app` or `web`) to app events and to `order_created` payload; mock ad API returns `platform_conversions`; remove the `*.dlq` topics (topics.yaml, create_topics, tests, docs); noise code keeps working (manifest, validator-visible kinds) with the new event types.
- **C2:** Silver: `platform` in silver_events and silver_orders; `order_returned` rows in silver_order_status; `platform_conversions` in silver_campaign_snapshots (Spark schema, column lists, publish allow-list, DDL mirror); Silver quarantine unchanged.
- **C3:** Gold model: rename tables per spec 17.13 (tables.py, generated DDL for both dialects, Spark builders, tests, docs); dimension keys `BY DEFAULT` identity and `-1` unknown rows seeded by apply_ddl; funnel grain per 17.9; `fact_orders` gets `platform`, `is_returned`, `returned_at`, `has_failed_attempt` and the cancel-by-time rule 17.6; `agg_orders_daily` builder; `agg_campaign_daily` columns per 17.10, 17.12, 17.15 (no `direct_share`, add `attribution_model`, `cost_per_delivered_order`, `platform_conversions`, `failed_attempt_rate`, `return_rate`, `open_rate`); recreate affected empty Databricks tables.
- **G1, G2, G3:** spec 17.17 to 17.21.

---

## Task 7: Spark session and Bronze ingest

**Files:**
- Create: `streaming/common/spark.py`
- Create: `streaming/jobs/bronze_ingest.py`
- Test: `tests/test_bronze.py`

**Interfaces:**
- Consumes: Kafka topics (Task 6)
- Produces:
  - `get_spark(app_name: str) -> SparkSession` (Delta enabled, 8 shuffle partitions, local[*])
  - `bronze_frame(raw_df) -> DataFrame` takes a Kafka source frame (`value`, `key`, `topic`, `partition`, `offset`, `timestamp`) and returns columns `raw_value, kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, ingest_time, ingest_date`
  - `run(topic_names, bronze_path, checkpoint_path, bootstrap)` runs a streaming query writing Delta to `bronze_path` partitioned by `ingest_date`. Path is `s3a://bronze/<topic>` on MinIO in production, a local folder in tests.

- [ ] **Step 1: Write the failing test**

`tests/test_bronze.py`:

```python
from pyspark.sql import Row
from streaming.common.spark import get_spark
from streaming.jobs.bronze_ingest import bronze_frame


def test_bronze_keeps_raw_value_and_kafka_metadata():
    spark = get_spark("test-bronze")
    raw = spark.createDataFrame([
        Row(key=b"sess_1", value=b'{"event_id":"evt_1"}', topic="app.events",
            partition=2, offset=17, timestamp=None),
    ])
    out = bronze_frame(raw)
    row = out.collect()[0]
    assert row["raw_value"] == '{"event_id":"evt_1"}'
    assert row["kafka_topic"] == "app.events"
    assert row["kafka_partition"] == 2
    assert row["kafka_offset"] == 17
    assert "ingest_date" in out.columns
    spark.stop()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bronze.py -v`
Expected: FAIL with `ModuleNotFoundError: streaming.common.spark`.

- [ ] **Step 3: Write `streaming/common/spark.py`**

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
```

- [ ] **Step 4: Write `streaming/jobs/bronze_ingest.py`**

```python
from pyspark.sql import DataFrame, functions as F


def bronze_frame(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingest_time"),
    ).withColumn("ingest_date", F.to_date("ingest_time"))


def run(topics: str, bronze_path: str, checkpoint_path: str, bootstrap: str):
    from streaming.common.spark import get_spark
    spark = get_spark("bronze_ingest")
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topics)
           .option("startingOffsets", "earliest")
           .load())
    return (bronze_frame(raw).writeStream.format("delta")
            .option("checkpointLocation", checkpoint_path)
            .partitionBy("ingest_date")
            .trigger(processingTime="10 seconds")
            .start(bronze_path))
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_bronze.py -v`
Expected: 1 passed. If Java is missing, the test fails with `JAVA_HOME is not set`; install Java 17 and retry.

- [ ] **Step 6: Commit**

```bash
git add streaming tests/test_bronze.py
git commit -m "feat: spark session and bronze ingest from kafka"
```

---

## Task 8: Silver clean, dedup, late flag, quarantine

**Files:**
- Create: `streaming/jobs/silver_clean.py`
- Test: `tests/test_silver.py`

**Interfaces:**
- Consumes: Bronze frame (Task 7), `contracts.validate.validate_event` (Task 2), `resolve_current_status` (Task 4)
- Produces:
  - `parse_and_validate(bronze_df) -> (good_df, quarantine_df)`. `good_df` has the envelope columns plus `event_time` (timestamp, UTC), `kafka_timestamp`, `is_late`, `event_date` (Africa/Cairo date). `quarantine_df` has `raw_payload, source_topic, error_code, error_detail, received_at`.
  - `dedupe_batch(df, key: str) -> DataFrame` keeps one row per key within the batch.
  - `merge_into(target_path, batch_df, key)` runs Delta `MERGE` on `key`.
  - Late rule: `is_late = (kafka_timestamp - event_time) > 10 minutes`.
  - A cancel after shipment is routed to quarantine with `error_code = CANCEL_AFTER_SHIPMENT`. This check runs against the order events already in Silver, so it lives in the order status job (Task 9), and Task 8 only exposes the helper `cancel_after_shipment(statuses: list[str]) -> bool`.

- [ ] **Step 1: Write the failing tests**

`tests/test_silver.py`:

```python
import json
from datetime import datetime, timedelta, timezone
from pyspark.sql import Row
from streaming.common.spark import get_spark
from streaming.jobs.silver_clean import parse_and_validate, dedupe_batch, cancel_after_shipment

NOW = datetime(2026, 10, 4, 12, 0, tzinfo=timezone.utc)


def _bronze(spark, events):
    rows = []
    for e, kts in events:
        rows.append(Row(raw_value=json.dumps(e), kafka_topic="app.events",
                        kafka_timestamp=kts, ingest_time=kts, ingest_date=kts.date()))
    return spark.createDataFrame(rows)


def _event(eid, t, **kw):
    e = {"event_id": eid, "event_type": "page_view", "event_version": 1,
         "event_time": t.isoformat(), "session_id": "sess_1", "device": "mobile",
         "channel": "organic", "country": "EG", "city": "Cairo", "payload": {}}
    e.update(kw)
    return e


def test_duplicate_event_id_is_dropped_in_batch():
    spark = get_spark("test-silver-dup")
    e = _event("evt_dup", NOW)
    b = _bronze(spark, [(e, NOW), (e, NOW)])
    good, _ = parse_and_validate(b)
    assert dedupe_batch(good, "event_id").count() == 1
    spark.stop()


def test_late_event_is_flagged_not_dropped():
    spark = get_spark("test-silver-late")
    late = _event("evt_late", NOW - timedelta(minutes=12))
    on_time = _event("evt_ok", NOW)
    b = _bronze(spark, [(late, NOW), (on_time, NOW)])
    good, _ = parse_and_validate(b)
    flags = {r["event_id"]: r["is_late"] for r in good.collect()}
    assert flags == {"evt_late": True, "evt_ok": False}
    spark.stop()


def test_invalid_event_goes_to_quarantine_with_code():
    spark = get_spark("test-silver-q")
    bad = _event("evt_bad", NOW, payload={"price": -1})
    b = _bronze(spark, [(bad, NOW)])
    good, q = parse_and_validate(b)
    assert good.count() == 0
    row = q.collect()[0]
    assert row["error_code"] == "NEGATIVE_PRICE"
    spark.stop()


def test_event_date_uses_cairo_time():
    spark = get_spark("test-silver-tz")
    late_night_utc = datetime(2026, 10, 4, 23, 30, tzinfo=timezone.utc)
    b = _bronze(spark, [(_event("evt_tz", late_night_utc), late_night_utc)])
    good, _ = parse_and_validate(b)
    assert str(good.collect()[0]["event_date"]) == "2026-10-05"
    spark.stop()


def test_cancel_after_shipment_detected():
    assert cancel_after_shipment(["order_created", "order_shipped", "order_cancelled"]) is True
    assert cancel_after_shipment(["order_created", "order_cancelled"]) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_silver.py -v`
Expected: FAIL with `ModuleNotFoundError: streaming.jobs.silver_clean`.

- [ ] **Step 3: Write `streaming/jobs/silver_clean.py`**

```python
import json
from datetime import datetime, timezone
from pyspark.sql import DataFrame, functions as F, types as T
from contracts.validate import validate_event
from streaming.common.spark import get_spark

LATE_AFTER_SECONDS = 600
ENVELOPE = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_version", T.IntegerType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("device", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("payload", T.MapType(T.StringType(), T.StringType())),
])


def _validate_row(raw: str, now_iso: str):
    try:
        e = json.loads(raw)
    except Exception:
        return False, "BAD_JSON"
    now = datetime.fromisoformat(now_iso)
    ok, code = validate_event(e, now=now)
    return ok, code or ""


def parse_and_validate(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    spark = bronze.sparkSession
    now_iso = datetime.now(timezone.utc).isoformat()

    def _check(raw):
        return _validate_row(raw, now_iso)

    # Row-level validation in Python for clarity; the job is sized for 1-2M events.
    rows = bronze.collect()
    good, bad = [], []
    for r in rows:
        ok, code = _validate_row(r["raw_value"], now_iso)
        if ok:
            e = json.loads(r["raw_value"])
            good.append({
                "event_id": e["event_id"], "event_type": e["event_type"],
                "event_version": e["event_version"], "event_time": e["event_time"],
                "session_id": e["session_id"], "customer_id": e.get("customer_id"),
                "order_id": e.get("order_id"), "device": e.get("device"),
                "channel": e.get("channel"), "country": e.get("country"),
                "city": e.get("city"), "payload": {k: str(v) for k, v in e["payload"].items()},
                "kafka_timestamp": r["kafka_timestamp"],
            })
        else:
            bad.append({"raw_payload": r["raw_value"], "source_topic": r["kafka_topic"],
                        "error_code": code, "error_detail": code, "received_at": r["ingest_time"]})

    good_df = spark.createDataFrame(good) if good else spark.createDataFrame([], ENVELOPE)
    good_df = (good_df
               .withColumn("event_time", F.to_timestamp("event_time"))
               .withColumn("kafka_timestamp", F.to_timestamp("kafka_timestamp"))
               .withColumn("is_late", (F.unix_timestamp("kafka_timestamp") - F.unix_timestamp("event_time")) > LATE_AFTER_SECONDS)
               .withColumn("event_date", F.to_date(F.from_utc_timestamp("event_time", "Africa/Cairo"))))
    q_df = spark.createDataFrame(bad) if bad else spark.createDataFrame([], "raw_payload string, source_topic string, error_code string, error_detail string, received_at timestamp")
    return good_df, q_df


def dedupe_batch(df: DataFrame, key: str) -> DataFrame:
    return df.dropDuplicates([key])


def cancel_after_shipment(statuses: list[str]) -> bool:
    return "order_cancelled" in statuses and "order_shipped" in statuses


def merge_into(target_path: str, batch: DataFrame, key: str) -> None:
    from delta.tables import DeltaTable
    spark = batch.sparkSession
    if not DeltaTable.isDeltaTable(spark, target_path):
        batch.write.format("delta").mode("overwrite").save(target_path)
        return
    (DeltaTable.forPath(spark, target_path).alias("t")
     .merge(batch.alias("s"), f"t.{key} = s.{key}")
     .whenNotMatchedInsertAll()
     .whenMatchedUpdateAll()
     .execute())
```

Note for the implementer: the row-level Python loop is acceptable for the demo (1–2M events in micro-batches). If a benchmark shows it is too slow, replace it with a Spark UDF that returns a struct. Do not change the function signatures.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_silver.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add streaming/jobs/silver_clean.py tests/test_silver.py
git commit -m "feat: silver clean with dedup, late flag and quarantine"
```

---

## Task 9: Gold builders as Spark reference implementations (production Gold is Task 10c SQL)

**Files:**
- Create: `streaming/jobs/gold_orders.py` (builds `fact_orders` and `fact_order_lines`)
- Create: `streaming/jobs/gold_funnel.py` (builds `fact_funnel_daily` and `live_metrics_minute`)
- Test: `tests/test_gold_stream.py`

**Interfaces:**
- Consumes: Silver frames from Task 8 (`silver_order_status` built from `orders` and `delivery.events`; `silver_events`)
- Produces:
  - `build_fact_orders(status_df) -> DataFrame`: one row per `order_id` with `current_status`, `status_rank`, `delivered_at`, `cancelled_at`, `delivery_lead_hours`, `delivery_failed_count`, `first_attempt_delivered`, `is_cancelled`, `is_delivered`, `is_first_order`, `order_seq`, `payment_method`, `carrier`, `city`, `country`, `order_total`, `order_created_at`, `date_key`
  - `build_fact_order_lines(items_df) -> DataFrame` keyed by `order_id, line_no`
  - `build_fact_funnel_daily(events_df) -> DataFrame` keyed by `event_date, country, channel, payment_method`, `payment_method` is `unknown` for app events
  - `build_live_metrics_minute(events_df) -> DataFrame` on 1-minute tumbling windows, excludes `is_late = true` rows

- [ ] **Step 1: Write the failing tests**

`tests/test_gold_stream.py`:

```python
from datetime import datetime
from pyspark.sql import Row
from streaming.common.spark import get_spark
from streaming.jobs.gold_orders import build_fact_orders
from streaming.jobs.gold_funnel import build_live_metrics_minute, build_fact_funnel_daily


def _status(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


def test_current_status_ignores_arrival_order():
    spark = get_spark("test-gold-status")
    df = _status(spark, [
        {"order_id": "ord_1", "customer_id": "c1", "status": "delivered", "status_rank": 6,
         "status_time": datetime(2026, 10, 5, 12), "payment_method": "cod", "carrier": "bosta",
         "city": "Cairo", "country": "EG", "order_total": 500.0, "order_created_at": datetime(2026, 10, 4, 9)},
        {"order_id": "ord_1", "customer_id": "c1", "status": "order_shipped", "status_rank": 4,
         "status_time": datetime(2026, 10, 4, 20), "payment_method": "cod", "carrier": "bosta",
         "city": "Cairo", "country": "EG", "order_total": 500.0, "order_created_at": datetime(2026, 10, 4, 9)},
    ])
    out = build_fact_orders(df).collect()[0]
    assert out["current_status"] == "delivered"
    assert out["is_delivered"] is True
    spark.stop()


def test_cancel_after_shipment_not_current():
    spark = get_spark("test-gold-cancel")
    df = _status(spark, [
        {"order_id": "ord_2", "customer_id": "c1", "status": "order_shipped", "status_rank": 4,
         "status_time": datetime(2026, 10, 4, 12), "payment_method": "card", "carrier": "aramex",
         "city": "Giza", "country": "EG", "order_total": 300.0, "order_created_at": datetime(2026, 10, 4, 9)},
        {"order_id": "ord_2", "customer_id": "c1", "status": "order_cancelled", "status_rank": 2,
         "status_time": datetime(2026, 10, 4, 13), "payment_method": "card", "carrier": "aramex",
         "city": "Giza", "country": "EG", "order_total": 300.0, "order_created_at": datetime(2026, 10, 4, 9)},
    ])
    out = build_fact_orders(df).collect()[0]
    assert out["current_status"] == "order_shipped"
    assert out["is_cancelled"] is False
    spark.stop()


def test_failed_delivery_count_and_first_attempt():
    spark = get_spark("test-gold-fail")
    rows = [
        {"order_id": "ord_3", "customer_id": "c2", "status": "delivery_failed", "status_rank": 5,
         "status_time": datetime(2026, 10, 5, 10), "payment_method": "cod", "carrier": "jt_express",
         "city": "Tanta", "country": "EG", "order_total": 200.0, "order_created_at": datetime(2026, 10, 4, 9)},
        {"order_id": "ord_3", "customer_id": "c2", "status": "delivered", "status_rank": 6,
         "status_time": datetime(2026, 10, 6, 10), "payment_method": "cod", "carrier": "jt_express",
         "city": "Tanta", "country": "EG", "order_total": 200.0, "order_created_at": datetime(2026, 10, 4, 9)},
    ]
    out = build_fact_orders(_status(spark, rows)).collect()[0]
    assert out["delivery_failed_count"] == 1
    assert out["first_attempt_delivered"] is False
    spark.stop()


def test_live_metrics_exclude_late_events():
    spark = get_spark("test-gold-late")
    ev = spark.createDataFrame([
        Row(event_id="a", event_type="page_view", event_time=datetime(2026, 10, 4, 12, 0, 5), country="EG", session_id="s1", is_late=False),
        Row(event_id="b", event_type="page_view", event_time=datetime(2026, 10, 4, 12, 0, 9), country="EG", session_id="s2", is_late=True),
    ])
    out = build_live_metrics_minute(ev).collect()
    assert sum(r["events"] for r in out) == 1
    spark.stop()


def test_funnel_has_unknown_payment_for_app_events():
    spark = get_spark("test-gold-funnel")
    ev = spark.createDataFrame([
        Row(event_id="a", event_type="product_view", event_date=datetime(2026, 10, 4).date(), country="EG",
            channel="paid_social", session_id="s1", is_late=False, payment_method=None),
    ])
    out = build_fact_funnel_daily(ev).collect()[0]
    assert out["payment_method"] == "unknown"
    spark.stop()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_gold_stream.py -v`
Expected: FAIL with `ModuleNotFoundError: streaming.jobs.gold_orders`.

- [ ] **Step 3: Write `streaming/jobs/gold_orders.py`**

```python
from pyspark.sql import DataFrame, Window, functions as F
from streaming.common.status import STATUS_RANK


def build_fact_orders(status_df: DataFrame) -> DataFrame:
    rank = F.create_map([F.lit(x) for pair in STATUS_RANK.items() for x in pair])
    s = status_df.withColumn("rank", rank[F.col("status")])
    # A cancel is ignored when the order also has a shipped event.
    shipped = s.groupBy("order_id").agg(F.max(F.when(F.col("status") == "order_shipped", 1).otherwise(0)).alias("was_shipped"))
    s = s.join(shipped, "order_id").filter(~((F.col("status") == "order_cancelled") & (F.col("was_shipped") == 1)))

    w = Window.partitionBy("order_id").orderBy(F.desc("rank"), F.desc("status_time"))
    current = s.withColumn("rn", F.row_number().over(w)).filter("rn = 1").select(
        "order_id", "customer_id", F.col("status").alias("current_status"), F.col("rank").alias("status_rank"),
        "payment_method", "carrier", "city", "country", "order_total", "order_created_at")

    agg = s.groupBy("order_id").agg(
        F.min(F.when(F.col("status") == "delivered", F.col("status_time"))).alias("delivered_at"),
        F.min(F.when(F.col("status") == "order_cancelled", F.col("status_time"))).alias("cancelled_at"),
        F.sum(F.when(F.col("status") == "delivery_failed", 1).otherwise(0)).alias("delivery_failed_count"),
    )
    out = (current.join(agg, "order_id")
           .withColumn("delivery_lead_hours", (F.unix_timestamp("delivered_at") - F.unix_timestamp("order_created_at")) / 3600)
           .withColumn("is_cancelled", F.col("cancelled_at").isNotNull())
           .withColumn("is_delivered", F.col("delivered_at").isNotNull())
           .withColumn("first_attempt_delivered", F.col("is_delivered") & (F.col("delivery_failed_count") == 0))
           .withColumn("date_key", F.date_format("order_created_at", "yyyyMMdd").cast("int")))
    return out


def build_fact_order_lines(items_df: DataFrame) -> DataFrame:
    return items_df.select("order_id", "line_no", "customer_key", "product_key", "date_key",
                           "city", "quantity", "line_total")
```

- [ ] **Step 4: Write `streaming/jobs/gold_funnel.py`**

```python
from pyspark.sql import DataFrame, functions as F


def build_live_metrics_minute(events: DataFrame) -> DataFrame:
    fresh = events.filter(~F.col("is_late"))
    return (fresh.groupBy(F.window("event_time", "1 minute").alias("w"), "country")
            .agg(F.count("*").alias("events"),
                 F.countDistinct("session_id").alias("active_sessions"),
                 F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
                 F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
                 F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("orders_placed"))
            .select(F.col("w.start").alias("minute_start"), "country", "events", "active_sessions",
                    "product_views", "add_to_cart", "orders_placed"))


def build_fact_funnel_daily(events: DataFrame) -> DataFrame:
    base = events.filter(~F.col("is_late")).withColumn(
        "payment_method", F.coalesce(F.col("payment_method"), F.lit("unknown")))
    return (base.groupBy("event_date", "country", "channel", "payment_method")
            .agg(F.countDistinct("session_id").alias("sessions"),
                 F.countDistinct(F.when(F.col("event_type") == "product_view", F.col("session_id"))).alias("product_views"),
                 F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("session_id"))).alias("cart_sessions"),
                 F.countDistinct(F.when(F.col("event_type") == "checkout_started", F.col("session_id"))).alias("checkout_sessions"),
                 F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("session_id"))).alias("orders"))
            .withColumn("cart_abandon_rate", F.when(F.col("cart_sessions") > 0, 1 - F.col("orders") / F.col("cart_sessions")))
            .withColumn("checkout_abandon_rate", F.when(F.col("checkout_sessions") > 0, 1 - F.col("orders") / F.col("checkout_sessions"))))
```

Note for the implementer: `build_fact_funnel_daily` expects a `payment_method` column on the events frame. Join it from `silver_order_status` on `session_id` for purchases before calling this function.

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_gold_stream.py -v`
Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
git add streaming/jobs/gold_orders.py streaming/jobs/gold_funnel.py tests/test_gold_stream.py
git commit -m "feat: spark gold tables for orders, funnel and live metrics"
```

---

## Task 10: Postgres DDL and idempotent Gold loader

**Files:**
- Create: `warehouse/ddl.sql`
- Create: `warehouse/load.py`
- Test: `tests/test_load.py` (integration; needs `postgres` from Task 1)

**Interfaces:**
- Consumes: Gold frames from Tasks 9 and 12–14
- Produces: `upsert(conn, table: str, rows: list[dict], key_cols: list[str]) -> int` runs `INSERT ... ON CONFLICT (key) DO UPDATE`. Returns the number of rows written. Connection from `psycopg.connect(os.environ["PG_DSN"])`.

- [ ] **Step 1: Write `warehouse/ddl.sql`**

```sql
CREATE TABLE IF NOT EXISTS dim_campaign (
  campaign_key serial PRIMARY KEY,
  campaign_id text UNIQUE NOT NULL,
  utm_campaign text UNIQUE NOT NULL,
  platform text NOT NULL,
  name text,
  start_date date,
  end_date date
);

CREATE TABLE IF NOT EXISTS dim_customer (
  customer_key bigserial PRIMARY KEY,
  customer_id text NOT NULL,
  country text, city text, signup_date date, primary_device text,
  marketing_consent boolean,
  valid_from timestamptz NOT NULL, valid_to timestamptz NOT NULL DEFAULT 'infinity',
  is_current boolean NOT NULL DEFAULT true,
  UNIQUE (customer_id, valid_from)
);

CREATE TABLE IF NOT EXISTS dim_product (
  product_key bigserial PRIMARY KEY,
  product_id text NOT NULL,
  name text, category text, price numeric(10,2), is_active boolean,
  valid_from timestamptz NOT NULL, valid_to timestamptz NOT NULL DEFAULT 'infinity',
  is_current boolean NOT NULL DEFAULT true,
  UNIQUE (product_id, valid_from)
);

CREATE TABLE IF NOT EXISTS dim_date (
  date_key int PRIMARY KEY, date date NOT NULL, week int, month int,
  is_weekend boolean, is_friday_saturday boolean, is_ramadan boolean, is_eid boolean, is_holiday boolean
);

CREATE TABLE IF NOT EXISTS fact_orders (
  order_id text PRIMARY KEY, customer_key bigint, session_id text, channel text,
  utm_source text, utm_campaign text, click_id text, campaign_id text,
  payment_method text, carrier text, city text, country text,
  order_total numeric(12,2), current_status text, status_rank int,
  order_created_at timestamptz, delivered_at timestamptz, cancelled_at timestamptz,
  delivery_lead_hours numeric(8,2), delivery_failed_count int, first_attempt_delivered boolean,
  is_cancelled boolean, is_delivered boolean, order_seq int, is_first_order boolean, date_key int
);

CREATE TABLE IF NOT EXISTS fact_order_lines (
  order_id text, line_no int, customer_key bigint, product_key bigint, date_key int,
  city text, quantity int, line_total numeric(12,2),
  PRIMARY KEY (order_id, line_no)
);

CREATE TABLE IF NOT EXISTS fact_funnel_daily (
  event_date date, country text, channel text, payment_method text,
  sessions int, product_views int, cart_sessions int, checkout_sessions int, orders int,
  cart_abandon_rate numeric(6,4), checkout_abandon_rate numeric(6,4),
  PRIMARY KEY (event_date, country, channel, payment_method)
);

CREATE TABLE IF NOT EXISTS live_metrics_minute (
  minute_start timestamptz, country text, events int, active_sessions int,
  product_views int, add_to_cart int, orders_placed int,
  PRIMARY KEY (minute_start, country)
);

CREATE TABLE IF NOT EXISTS fact_attribution (
  order_id text, model text, campaign_id text, click_id text, order_total numeric(12,2),
  PRIMARY KEY (order_id, model)
);

CREATE TABLE IF NOT EXISTS fact_campaign_daily (
  campaign_id text, report_date date, platform text, impressions bigint, clicks bigint,
  spend numeric(12,4), orders_placed int, orders_delivered int,
  revenue_placed numeric(12,2), revenue_delivered numeric(12,2),
  orders_cancelled int, orders_failed_delivery int,
  cancel_rate numeric(6,4), failed_delivery_rate numeric(6,4),
  new_customer_orders int, aov numeric(12,2), ctr numeric(6,4), conversion_rate numeric(6,4),
  cpa numeric(12,2), roas_placed numeric(8,2), roas_delivered numeric(8,2), direct_share numeric(6,4),
  PRIMARY KEY (campaign_id, report_date)
);

CREATE TABLE IF NOT EXISTS fact_product_recs (
  segment_level text, segment_value text, anchor_product_id text, rank int,
  rec_product_id text, co_view_count int, window_end date,
  PRIMARY KEY (segment_level, segment_value, anchor_product_id, rank)
);

CREATE TABLE IF NOT EXISTS fact_city_product_daily (
  city text, product_id text, date_key int,
  sessions_viewed int, add_to_cart int, units int, revenue numeric(12,2),
  PRIMARY KEY (city, product_id, date_key)
);
```

- [ ] **Step 2: Write the failing integration test**

`tests/test_load.py`:

```python
import os
import psycopg
import pytest
from warehouse.load import upsert

DSN = os.environ.get("PG_DSN", "postgresql://fs:change_me@localhost:5432/funnelstream")

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    try:
        c = psycopg.connect(DSN, autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("postgres not running")
    c.execute("DROP TABLE IF EXISTS test_upsert")
    c.execute("CREATE TABLE test_upsert (id text PRIMARY KEY, v int)")
    yield c
    c.execute("DROP TABLE test_upsert")
    c.close()


def test_upsert_is_idempotent(conn):
    rows = [{"id": "a", "v": 1}, {"id": "b", "v": 2}]
    upsert(conn, "test_upsert", rows, ["id"])
    upsert(conn, "test_upsert", rows, ["id"])
    count = conn.execute("SELECT count(*) FROM test_upsert").fetchone()[0]
    assert count == 2


def test_upsert_updates_existing_key(conn):
    upsert(conn, "test_upsert", [{"id": "a", "v": 1}], ["id"])
    upsert(conn, "test_upsert", [{"id": "a", "v": 9}], ["id"])
    v = conn.execute("SELECT v FROM test_upsert WHERE id='a'").fetchone()[0]
    assert v == 9
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_load.py -v`
Expected: FAIL with `ModuleNotFoundError: warehouse.load` (or skip if Postgres is down; start it with `docker compose up -d postgres`).

- [ ] **Step 4: Write `warehouse/load.py`**

```python
import os
import psycopg


def upsert(conn, table: str, rows: list[dict], key_cols: list[str]) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(cols))
    update = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key_cols)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
           f"ON CONFLICT ({', '.join(key_cols)}) DO UPDATE SET {update}" if update else
           f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING")
    with conn.cursor() as cur:
        cur.executemany(sql, [tuple(r[c] for c in cols) for r in rows])
    conn.commit()
    return len(rows)


def connect():
    return psycopg.connect(os.environ["PG_DSN"])
```

- [ ] **Step 5: Apply DDL and run tests to verify they pass**

Run: `docker compose exec -T postgres psql -U fs -d funnelstream < warehouse/ddl.sql`
Run: `PG_DSN=postgresql://fs:change_me@localhost:5432/funnelstream pytest tests/test_load.py -v`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add warehouse tests/test_load.py
git commit -m "feat: postgres gold DDL and idempotent upsert loader"
```

---

## Task 11: Batch, catalog and customer SCD2 dimensions

**Files:**
- Create: `batch/dimensions.py`
- Test: `tests/test_scd2.py`

**Interfaces:**
- Consumes: catalog and customer rows from `simulator/catalog.py` (Task 3)
- Produces:
  - `apply_scd2(current_rows: list[dict], incoming: list[dict], key: str, tracked: list[str], now: datetime) -> tuple[list[dict], list[dict]]` returns `(to_close, to_insert)`. `to_close` are the old versions with `valid_to = now` and `is_current = False`. `to_insert` are new versions with `valid_from = now`, `valid_to = 'infinity'`, `is_current = True`. Only tracked columns trigger a new version.
  - `load_dimensions(conn, catalog_rows, customer_rows, now)` applies SCD2 to `dim_product` (tracked: `price`, `category`) and `dim_customer` (tracked: `city`, `primary_device`).

- [ ] **Step 1: Write the failing tests**

`tests/test_scd2.py`:

```python
from datetime import datetime, timezone
from batch.dimensions import apply_scd2

NOW = datetime(2026, 10, 4, 6, 0, tzinfo=timezone.utc)


def test_unchanged_row_creates_no_version():
    current = [{"product_id": "p1", "price": 100.0, "category": "beauty", "is_current": True}]
    incoming = [{"product_id": "p1", "price": 100.0, "category": "beauty"}]
    close, insert = apply_scd2(current, incoming, "product_id", ["price", "category"], NOW)
    assert close == [] and insert == []


def test_price_change_closes_old_and_inserts_new():
    current = [{"product_id": "p1", "price": 100.0, "category": "beauty", "is_current": True}]
    incoming = [{"product_id": "p1", "price": 120.0, "category": "beauty"}]
    close, insert = apply_scd2(current, incoming, "product_id", ["price", "category"], NOW)
    assert len(close) == 1 and close[0]["is_current"] is False and close[0]["valid_to"] == NOW
    assert len(insert) == 1 and insert[0]["price"] == 120.0 and insert[0]["is_current"] is True


def test_untracked_change_does_not_version():
    current = [{"product_id": "p1", "price": 100.0, "category": "beauty", "name": "old", "is_current": True}]
    incoming = [{"product_id": "p1", "price": 100.0, "category": "beauty", "name": "new"}]
    close, insert = apply_scd2(current, incoming, "product_id", ["price", "category"], NOW)
    assert close == [] and insert == []


def test_new_key_is_inserted_as_current():
    close, insert = apply_scd2([], [{"product_id": "p9", "price": 5.0, "category": "home"}],
                               "product_id", ["price", "category"], NOW)
    assert close == [] and len(insert) == 1 and insert[0]["is_current"] is True


def test_exactly_one_current_per_key_after_apply():
    current = [{"product_id": "p1", "price": 100.0, "category": "beauty", "is_current": True}]
    incoming = [{"product_id": "p1", "price": 120.0, "category": "beauty"}]
    close, insert = apply_scd2(current, incoming, "product_id", ["price", "category"], NOW)
    after = [dict(r, is_current=False) for r in close] + [r for r in current if r not in close] + insert
    current_after = [r for r in after if r["is_current"]]
    assert len({r["product_id"] for r in current_after}) == len(current_after) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_scd2.py -v`
Expected: FAIL with `ModuleNotFoundError: batch.dimensions`.

- [ ] **Step 3: Write `batch/dimensions.py`**

```python
from datetime import datetime

INFINITY = "infinity"


def apply_scd2(current_rows, incoming, key, tracked, now: datetime):
    """Return (to_close, to_insert). Only changes to tracked columns create a version."""
    current = {r[key]: r for r in current_rows if r.get("is_current", True)}
    to_close, to_insert = [], []
    for row in incoming:
        old = current.get(row[key])
        if old is None:
            to_insert.append({**row, "valid_from": now, "valid_to": INFINITY, "is_current": True})
            continue
        if any(old.get(c) != row.get(c) for c in tracked):
            closed = {**old, "valid_to": now, "is_current": False}
            to_close.append(closed)
            to_insert.append({**row, "valid_from": now, "valid_to": INFINITY, "is_current": True})
    return to_close, to_insert


def load_dimensions(conn, catalog_rows, customer_rows, now: datetime) -> None:
    """Apply SCD2 to dim_product and dim_customer and write them in one transaction."""
    from warehouse.load import upsert
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, price, category, is_active, valid_from, valid_to, is_current FROM dim_product WHERE is_current")
        cols = [d.name for d in cur.description]
        cur_products = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.execute("SELECT customer_id, city, primary_device, country, signup_date, marketing_consent, valid_from, valid_to, is_current FROM dim_customer WHERE is_current")
        cols = [d.name for d in cur.description]
        cur_customers = [dict(zip(cols, r)) for r in cur.fetchall()]

    close_p, insert_p = apply_scd2(cur_products, catalog_rows, "product_id", ["price", "category"], now)
    close_c, insert_c = apply_scd2(cur_customers, customer_rows, "customer_id", ["city", "primary_device"], now)
    for r in close_p:
        upsert(conn, "dim_product", [r], ["product_id", "valid_from"])
    for r in close_c:
        upsert(conn, "dim_customer", [r], ["customer_id", "valid_from"])
    upsert(conn, "dim_product", insert_p, ["product_id", "valid_from"])
    upsert(conn, "dim_customer", insert_c, ["customer_id", "valid_from"])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_scd2.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add batch/dimensions.py tests/test_scd2.py
git commit -m "feat: scd2 apply for customer and product dimensions"
```

---

## Task 12: Campaign snapshots and fact_campaign_daily

**Files:**
- Create: `batch/ingest_ads.py` (pull from mock API, latest pull per campaign per day)
- Create: `batch/campaign_daily.py`
- Test: `tests/test_campaign_daily.py`

**Interfaces:**
- Consumes: mock API (Task 5), `fact_orders` and `fact_attribution` (Tasks 9, 13)
- Produces:
  - `pull(api_url: str, report_date: date, as_of: datetime) -> list[dict]` fetches the mock API and returns rows with `campaign_id, report_date, pulled_at, impressions, clicks, spend, currency, platform`
  - `latest_per_day(snapshots: list[dict]) -> list[dict]` keeps one row per `(campaign_id, report_date)` with the max `pulled_at`
  - `campaign_daily(snapshots, orders, attribution) -> list[dict]` returns rows with the `fact_campaign_daily` columns; `cpa`, `roas_placed`, `roas_delivered`, `ctr`, `conversion_rate` are `None` when the denominator is zero

- [ ] **Step 1: Write the failing tests**

`tests/test_campaign_daily.py`:

```python
from datetime import date, datetime
from batch.campaign_daily import campaign_daily
from batch.ingest_ads import latest_per_day


def _snap(pulled, spend, impressions=1000, clicks=20):
    return {"campaign_id": "camp_202", "report_date": date(2026, 10, 4), "platform": "meta",
            "pulled_at": datetime.fromisoformat(pulled), "spend": spend,
            "impressions": impressions, "clicks": clicks, "currency": "EGP"}


def test_two_pulls_same_day_use_latest_and_do_not_sum_spend():
    rows = latest_per_day([_snap("2026-10-04T10:00:00", 100.0), _snap("2026-10-04T18:00:00", 180.0)])
    assert len(rows) == 1
    assert rows[0]["spend"] == 180.0


def test_zero_orders_gives_null_cpa_and_roas():
    snaps = latest_per_day([_snap("2026-10-04T18:00:00", 500.0)])
    out = campaign_daily(snaps, orders=[], attribution=[])
    row = out[0]
    assert row["orders_placed"] == 0
    assert row["cpa"] is None
    assert row["roas_placed"] is None
    assert row["roas_delivered"] is None


def test_zero_spend_gives_null_roas_not_error():
    snaps = latest_per_day([_snap("2026-10-04T18:00:00", 0.0)])
    attribution = [{"order_id": "o1", "model": "last_touch", "campaign_id": "camp_202", "order_total": 300.0}]
    orders = [{"order_id": "o1", "is_delivered": True, "is_cancelled": False, "order_total": 300.0,
               "is_first_order": True, "current_status": "delivered", "delivery_failed_count": 0}]
    row = campaign_daily(snaps, orders=orders, attribution=attribution)[0]
    assert row["roas_delivered"] is None
    assert row["revenue_delivered"] == 300.0


def test_delivered_roas_uses_only_delivered_revenue():
    snaps = latest_per_day([_snap("2026-10-04T18:00:00", 100.0)])
    attribution = [{"order_id": "o1", "model": "last_touch", "campaign_id": "camp_202", "order_total": 200.0},
                   {"order_id": "o2", "model": "last_touch", "campaign_id": "camp_202", "order_total": 300.0}]
    orders = [{"order_id": "o1", "is_delivered": True, "is_cancelled": False, "order_total": 200.0,
               "is_first_order": True, "current_status": "delivered", "delivery_failed_count": 0},
              {"order_id": "o2", "is_delivered": False, "is_cancelled": True, "order_total": 300.0,
               "is_first_order": True, "current_status": "order_cancelled", "delivery_failed_count": 0}]
    row = campaign_daily(snaps, orders=orders, attribution=attribution)[0]
    assert row["revenue_placed"] == 500.0
    assert row["revenue_delivered"] == 200.0
    assert row["roas_delivered"] == 2.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_campaign_daily.py -v`
Expected: FAIL with `ModuleNotFoundError: batch.ingest_ads`.

- [ ] **Step 3: Write `batch/ingest_ads.py`**

```python
import json
import os
from datetime import date, datetime
import requests


def pull(api_url: str, report_date: date, as_of: datetime) -> list[dict]:
    r = requests.get(f"{api_url}/campaigns", params={
        "report_date": report_date.isoformat(), "as_of": as_of.replace(tzinfo=None).isoformat()}, timeout=30)
    r.raise_for_status()
    rows = r.json()
    for row in rows:
        row["pulled_at"] = datetime.fromisoformat(row["pulled_at"])
        row["report_date"] = date.fromisoformat(row["report_date"])
        row["platform"] = "meta" if row["campaign_id"] in ("camp_202", "camp_203") else "google"
    return rows


def latest_per_day(snapshots: list[dict]) -> list[dict]:
    best = {}
    for s in snapshots:
        key = (s["campaign_id"], s["report_date"])
        if key not in best or s["pulled_at"] > best[key]["pulled_at"]:
            best[key] = s
    return list(best.values())
```

- [ ] **Step 4: Write `batch/campaign_daily.py`**

```python
from collections import defaultdict


def _div(a, b):
    return None if not b else a / b


def campaign_daily(snapshots, orders, attribution):
    orders_by_id = {o["order_id"]: o for o in orders}
    attributed = defaultdict(list)
    for a in attribution:
        if a["model"] == "last_touch" and a["campaign_id"]:
            attributed[a["campaign_id"]].append(orders_by_id[a["order_id"]] | {"order_total": a["order_total"]})

    out = []
    for s in snapshots:
        rows = attributed.get(s["campaign_id"], [])
        placed = [o for o in rows if not o["is_cancelled"]]
        delivered = [o for o in rows if o["is_delivered"]]
        failed = [o for o in rows if o["delivery_failed_count"] > 0]
        cancelled = [o for o in rows if o["is_cancelled"]]
        rev_placed = round(sum(o["order_total"] for o in placed), 2)
        rev_delivered = round(sum(o["order_total"] for o in delivered), 2)
        n_placed = len(placed)
        out.append({
            "campaign_id": s["campaign_id"], "report_date": s["report_date"], "platform": s["platform"],
            "impressions": s["impressions"], "clicks": s["clicks"], "spend": s["spend"],
            "orders_placed": n_placed, "orders_delivered": len(delivered),
            "revenue_placed": rev_placed, "revenue_delivered": rev_delivered,
            "orders_cancelled": len(cancelled), "orders_failed_delivery": len(failed),
            "cancel_rate": _div(len(cancelled), n_placed),
            "failed_delivery_rate": _div(len(failed), n_placed),
            "new_customer_orders": sum(1 for o in placed if o["is_first_order"]),
            "aov": _div(rev_placed, n_placed),
            "ctr": _div(s["clicks"], s["impressions"]),
            "conversion_rate": _div(n_placed, s["clicks"]),
            "cpa": _div(s["spend"], n_placed),
            "roas_placed": _div(rev_placed, s["spend"]),
            "roas_delivered": _div(rev_delivered, s["spend"]),
            "direct_share": None,
        })
    return out
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_campaign_daily.py -v`
Expected: 4 passed. Note `test_delivered_roas_uses_only_delivered_revenue` expects `roas_delivered == 2.0`: 200 / 100.

- [ ] **Step 6: Commit**

```bash
git add batch tests/test_campaign_daily.py
git commit -m "feat: campaign snapshots and campaign daily with null-safe ratios"
```

---

## Task 13: Attribution (last touch and first touch)

**Files:**
- Create: `batch/attribution.py`
- Test: `tests/test_attribution.py`

**Interfaces:**
- Consumes: session click events from `silver_events` (Task 8) and orders from `fact_orders` (Task 9)
- Produces: `attribute(orders, clicks, window_days=7) -> list[dict]` with keys `order_id, model, campaign_id, click_id, order_total`. Returns two rows per order (`last_touch`, `first_touch`). Direct orders have `campaign_id = None` and `click_id = None`.

A session click is the first `page_view` or `product_view` in a session with `utm_campaign` (spec section 4). Clicks are given as dicts with `session_id, customer_id, click_id, campaign_id, click_time`.

- [ ] **Step 1: Write the failing tests**

`tests/test_attribution.py`:

```python
from datetime import datetime, timedelta
from batch.attribution import attribute

ORDER_T = datetime(2026, 10, 5, 12, 0)


def _click(cid, t, campaign="camp_202"):
    return {"customer_id": "c1", "click_id": cid, "campaign_id": campaign, "click_time": t}


def _order(oid="o1", t=ORDER_T, total=300.0):
    return {"order_id": oid, "customer_id": "c1", "order_created_at": t, "order_total": total}


def _pick(rows, model):
    return next(r for r in rows if r["model"] == model and r["order_id"] == "o1")


def test_last_touch_picks_most_recent_click_in_window():
    clicks = [_click("k_old", ORDER_T - timedelta(days=5), "camp_201"),
              _click("k_new", ORDER_T - timedelta(days=1), "camp_202")]
    out = attribute([_order()], clicks)
    assert _pick(out, "last_touch")["click_id"] == "k_new"


def test_first_touch_picks_earliest_click_in_window():
    clicks = [_click("k_old", ORDER_T - timedelta(days=5), "camp_201"),
              _click("k_new", ORDER_T - timedelta(days=1), "camp_202")]
    out = attribute([_order()], clicks)
    assert _pick(out, "first_touch")["click_id"] == "k_old"


def test_click_outside_window_is_ignored():
    clicks = [_click("k_stale", ORDER_T - timedelta(days=8))]
    out = attribute([_order()], clicks)
    assert _pick(out, "last_touch")["campaign_id"] is None


def test_direct_order_has_null_campaign_and_one_row_per_model():
    out = attribute([_order()], [])
    rows = [r for r in out if r["order_id"] == "o1"]
    assert len(rows) == 2
    assert all(r["campaign_id"] is None and r["click_id"] is None for r in rows)


def test_click_after_order_is_not_used():
    clicks = [_click("k_future", ORDER_T + timedelta(hours=1))]
    out = attribute([_order()], clicks)
    assert _pick(out, "last_touch")["campaign_id"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_attribution.py -v`
Expected: FAIL with `ModuleNotFoundError: batch.attribution`.

- [ ] **Step 3: Write `batch/attribution.py`**

```python
from datetime import timedelta


def attribute(orders, clicks, window_days: int = 7):
    window = timedelta(days=window_days)
    out = []
    for o in orders:
        t = o["order_created_at"]
        candidates = [c for c in clicks
                      if c["customer_id"] == o["customer_id"]
                      and timedelta(0) <= t - c["click_time"] <= window]
        candidates.sort(key=lambda c: c["click_time"])
        last = candidates[-1] if candidates else None
        first = candidates[0] if candidates else None
        for model, pick in (("last_touch", last), ("first_touch", first)):
            out.append({
                "order_id": o["order_id"],
                "model": model,
                "campaign_id": pick["campaign_id"] if pick else None,
                "click_id": pick["click_id"] if pick else None,
                "order_total": o["order_total"],
            })
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_attribution.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add batch/attribution.py tests/test_attribution.py
git commit -m "feat: last-touch and first-touch attribution with 7-day window"
```

---

## Task 14: Recommendations and city-product table

**Files:**
- Create: `batch/recs.py`
- Test: `tests/test_recs.py`

**Interfaces:**
- Consumes: session views from `silver_events` (product_view) and active products from `silver_products`
- Produces:
  - `city_product(views, orders_lines) -> list[dict]` keys `city, product_id, date_key, sessions_viewed, add_to_cart, units, revenue`
  - `recommend(views, min_co_viewers=20, top_n=5, window_days=30) -> list[dict]` keys `segment_level, segment_value, anchor_product_id, rank, rec_product_id, co_view_count, window_end`. `views` items: `customer_id, city, product_id, event_date, is_active`.
- Fallback rule: for an anchor product in a city, if fewer than `min_co_viewers` customers co-viewed any recommendation, use country-level counts, then global. `is_active = False` products are never recommended.

- [ ] **Step 1: Write the failing tests**

`tests/test_recs.py`:

```python
from datetime import date
from batch.recs import recommend, city_product


def _v(cust, city, pid, active=True):
    return {"customer_id": cust, "city": city, "country": "EG", "product_id": pid,
            "event_date": date(2026, 10, 4), "is_active": active}


def test_co_view_counts_customers_who_viewed_both():
    views = [_v("c1", "Cairo", "X"), _v("c1", "Cairo", "Y"), _v("c2", "Cairo", "X"), _v("c2", "Cairo", "Y")]
    views += [_v(f"c{i}", "Cairo", "X") for i in range(3, 25)]
    recs = recommend(views, min_co_viewers=2, top_n=5)
    cairo_x = [r for r in recs if r["segment_level"] == "city" and r["anchor_product_id"] == "X"]
    assert cairo_x[0]["rec_product_id"] == "Y"
    assert cairo_x[0]["co_view_count"] == 2


def test_falls_back_to_country_when_city_has_too_few_co_viewers():
    views = [_v("c1", "Cairo", "X"), _v("c1", "Cairo", "Y"),
             _v("c2", "Giza", "X"), _v("c2", "Giza", "Y")]
    recs = recommend(views, min_co_viewers=20, top_n=5)
    levels = {r["segment_level"] for r in recs if r["anchor_product_id"] == "X"}
    assert "city" not in levels
    assert "country" in levels or "global" in levels


def test_inactive_products_never_recommended():
    views = [_v("c1", "Cairo", "X"), _v("c1", "Cairo", "Y", active=False),
             _v("c2", "Cairo", "X"), _v("c2", "Cairo", "Y", active=False)]
    recs = recommend(views, min_co_viewers=1, top_n=5)
    assert all(r["rec_product_id"] != "Y" for r in recs)


def test_city_product_counts_sessions_viewed():
    views = [_v("c1", "Cairo", "X"), _v("c2", "Cairo", "X")]
    out = city_product(views, orders_lines=[])
    row = next(r for r in out if r["product_id"] == "X")
    assert row["sessions_viewed"] == 2
    assert row["units"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_recs.py -v`
Expected: FAIL with `ModuleNotFoundError: batch.recs`.

- [ ] **Step 3: Write `batch/recs.py`**

```python
from collections import defaultdict
from itertools import combinations


def _co_views(views):
    """Map anchor -> {rec: set(customer)} over viewers, active products only."""
    by_customer = defaultdict(set)
    for v in views:
        if v["is_active"]:
            by_customer[v["customer_id"]].add(v["product_id"])
    co = defaultdict(lambda: defaultdict(set))
    for cust, prods in by_customer.items():
        for a, b in combinations(sorted(prods), 2):
            co[a][b].add(cust)
            co[b][a].add(cust)
    return co


def recommend(views, min_co_viewers: int = 20, top_n: int = 5, window_days: int = 30):
    out = []
    window_end = max(v["event_date"] for v in views) if views else None
    scopes = {}
    for v in views:
        scopes.setdefault(("city", v["city"]), []).append(v)
        scopes.setdefault(("country", v["country"]), []).append(v)
    scopes[("global", "ALL")] = views

    for (level, value), scoped in scopes.items():
        co = _co_views(scoped)
        for anchor, recs in co.items():
            ranked = sorted(recs.items(), key=lambda kv: -len(kv[1]))
            if level == "city" and len(ranked[0][1]) < min_co_viewers:
                continue
            for rank, (rec, custs) in enumerate(ranked[:top_n], start=1):
                out.append({"segment_level": level, "segment_value": value,
                            "anchor_product_id": anchor, "rank": rank,
                            "rec_product_id": rec, "co_view_count": len(custs),
                            "window_end": window_end})
    return out


def city_product(views, orders_lines):
    sessions = defaultdict(set)
    for v in views:
        sessions[(v["city"], v["product_id"], v["event_date"])].add(v["customer_id"])
    units = defaultdict(int)
    revenue = defaultdict(float)
    for line in orders_lines:
        k = (line["city"], line["product_id"], line["date"])
        units[k] += line["quantity"]
        revenue[k] += line["line_total"]
    out = []
    for (city, pid, d), custs in sessions.items():
        k = (city, pid, d)
        out.append({"city": city, "product_id": pid, "date_key": int(d.strftime("%Y%m%d")),
                    "sessions_viewed": len(custs), "add_to_cart": 0,
                    "units": units[k], "revenue": round(revenue[k], 2)})
    return out
```

Note for the implementer: the spec's "fewer than 20 co-viewing customers" rule is applied to the best recommendation for a city anchor. Keep it as written.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_recs.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add batch/recs.py tests/test_recs.py
git commit -m "feat: co-view recommendations with city, country, global fallback"
```

---

## Task 15: Batch runner and Airflow DAG

**Files:**
- Create: `batch/run_batch.py` (the single batch entry point; the Python fallback)
- Create: `airflow/dags/funnelstream_batch.py`
- Test: `tests/test_batch_run.py`

**Interfaces:**
- Consumes: Tasks 5, 11, 12, 13, 14, 10
- Produces: `run_batch(report_date, as_of, api_url, pg_dsn) -> dict` returns row counts per table. The DAG calls the same function, so the fallback and the DAG cannot diverge.

- [ ] **Step 1: Write the failing test**

`tests/test_batch_run.py`:

```python
import inspect
from batch import run_batch as rb


def test_run_batch_signature():
    sig = inspect.signature(rb.run_batch)
    assert list(sig.parameters) == ["report_date", "as_of", "api_url", "pg_dsn"]


def test_dag_calls_the_same_function():
    text = open("airflow/dags/funnelstream_batch.py").read()
    assert "from batch.run_batch import run_batch" in text


def test_dag_has_retries_and_manual_trigger():
    text = open("airflow/dags/funnelstream_batch.py").read()
    assert "retries" in text and "schedule=None" in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_batch_run.py -v`
Expected: FAIL with `ModuleNotFoundError: batch.run_batch`.

- [ ] **Step 3: Write `batch/run_batch.py`**

```python
import argparse
import os
from datetime import date, datetime, timezone
from batch.ingest_ads import pull, latest_per_day
from batch.campaign_daily import campaign_daily
from batch.attribution import attribute
from batch.recs import recommend, city_product
from batch.dimensions import load_dimensions
from warehouse.load import upsert, connect
from simulator.catalog import make_catalog, make_customers


def run_batch(report_date: date, as_of: datetime, api_url: str, pg_dsn: str) -> dict:
    conn = connect() if pg_dsn is None else __import__("psycopg").connect(pg_dsn)
    counts = {}
    try:
        snaps = latest_per_day(pull(api_url, report_date, as_of))
        # Orders, clicks and views come from Gold and Silver tables. Reading them is
        # environment-specific (Delta on MinIO or Postgres); the readers live in
        # batch/sources.py and are called here.
        from batch.sources import read_orders, read_clicks, read_views, read_order_lines
        orders = read_orders(conn)
        clicks = read_clicks(conn)
        views = read_views(conn)
        lines = read_order_lines(conn)

        now = as_of.replace(tzinfo=timezone.utc)
        load_dimensions(conn, make_catalog(42), make_customers(42, 5000), now)

        attr = attribute(orders, clicks)
        counts["fact_attribution"] = upsert(conn, "fact_attribution", attr, ["order_id", "model"])
        camp = campaign_daily(snaps, orders, attr)
        counts["fact_campaign_daily"] = upsert(conn, "fact_campaign_daily", camp, ["campaign_id", "report_date"])
        recs = recommend(views)
        counts["fact_product_recs"] = upsert(conn, "fact_product_recs", recs,
                                             ["segment_level", "segment_value", "anchor_product_id", "rank"])
        cp = city_product(views, lines)
        counts["fact_city_product_daily"] = upsert(conn, "fact_city_product_daily", cp, ["city", "product_id", "date_key"])
    finally:
        conn.close()
    return counts


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--as-of", default=datetime.now(timezone.utc).isoformat())
    ap.add_argument("--api", default=os.environ.get("AD_API_URL", "http://localhost:8000"))
    args = ap.parse_args()
    print(run_batch(date.fromisoformat(args.date), datetime.fromisoformat(args.as_of),
                    args.api, os.environ.get("PG_DSN")))
```

Note for the implementer: `batch/sources.py` holds the four readers (`read_orders`, `read_clicks`, `read_views`, `read_order_lines`). Each returns plain dicts shaped as the tests in Tasks 13 and 14 expect. Write them in this task; read from Postgres `fact_orders`, `fact_order_lines`, and a `silver_events` table that Task 8 publishes to Postgres for the batch layer. Keep the signatures.

- [ ] **Step 4: Write `airflow/dags/funnelstream_batch.py`**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"retries": 2, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True}


def _run(**context):
    import os
    from datetime import datetime as dt, timezone
    from batch.run_batch import run_batch
    now = dt.now(timezone.utc)
    return run_batch(now.date(), now, os.environ["AD_API_URL"], os.environ["PG_DSN"])


with DAG(
    dag_id="funnelstream_batch",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["funnelstream"],
) as dag:
    PythonOperator(task_id="run_batch", python_callable=_run)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_batch_run.py -v`
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add batch/run_batch.py airflow/dags/funnelstream_batch.py tests/test_batch_run.py
git commit -m "feat: batch entry point shared by airflow dag and python fallback"
```

---

## Task 16: SQL data-quality checks

**Files:**
- Create: `warehouse/checks.sql`
- Create: `warehouse/run_checks.py`
- Test: `tests/test_checks.py` (integration)

**Interfaces:**
- Consumes: Gold tables (Task 10)
- Produces: `run_checks(conn) -> list[dict]` returns failed checks as `{name, value, threshold}`. An empty list means pass. Each check is one SQL query that returns a single number; the runner compares it with the threshold.

- [ ] **Step 1: Write the failing integration test**

`tests/test_checks.py`:

```python
import os
import psycopg
import pytest
from warehouse.run_checks import run_checks, CHECKS

DSN = os.environ.get("PG_DSN", "postgresql://fs:change_me@localhost:5432/funnelstream")
pytestmark = pytest.mark.integration


def test_checks_are_defined_with_thresholds():
    assert len(CHECKS) >= 8
    assert all("name" in c and "sql" in c and "max" in c for c in CHECKS)


def test_check_fails_when_threshold_exceeded():
    try:
        conn = psycopg.connect(DSN, autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("postgres not running")
    conn.execute("DROP TABLE IF EXISTS fact_orders_chk")
    conn.execute("CREATE TABLE fact_orders_chk (order_id text, n int)")
    conn.execute("INSERT INTO fact_orders_chk VALUES ('a',1),('a',2)")
    fake = [{"name": "dup", "sql": "SELECT count(*) - count(DISTINCT order_id) FROM fact_orders_chk", "max": 0}]
    failed = run_checks(conn, fake)
    assert failed and failed[0]["name"] == "dup"
    conn.execute("DROP TABLE fact_orders_chk")
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PG_DSN=postgresql://fs:change_me@localhost:5432/funnelstream pytest tests/test_checks.py -v`
Expected: FAIL with `ModuleNotFoundError: warehouse.run_checks`.

- [ ] **Step 3: Write `warehouse/run_checks.py`**

```python
CHECKS = [
    {"name": "orders_no_duplicate_keys", "max": 0,
     "sql": "SELECT count(*) - count(DISTINCT order_id) FROM fact_orders"},
    {"name": "order_lines_total_matches_order",  "max": 0,
     "sql": ("SELECT count(*) FROM fact_orders o JOIN "
             "(SELECT order_id, sum(line_total) s FROM fact_order_lines GROUP BY order_id) l "
             "USING (order_id) WHERE abs(o.order_total - l.s) > 0.01")},
    {"name": "cancelled_orders_not_delivered", "max": 0,
     "sql": "SELECT count(*) FROM fact_orders WHERE is_cancelled AND is_delivered"},
    {"name": "scd2_one_current_product", "max": 0,
     "sql": ("SELECT count(*) FROM (SELECT product_id FROM dim_product WHERE is_current "
             "GROUP BY product_id HAVING count(*) > 1) x")},
    {"name": "scd2_one_current_customer", "max": 0,
     "sql": ("SELECT count(*) FROM (SELECT customer_id FROM dim_customer WHERE is_current "
             "GROUP BY customer_id HAVING count(*) > 1) x")},
    {"name": "clicks_not_above_impressions", "max": 0,
     "sql": "SELECT count(*) FROM fact_campaign_daily WHERE clicks > impressions"},
    {"name": "no_negative_spend", "max": 0,
     "sql": "SELECT count(*) FROM fact_campaign_daily WHERE spend < 0"},
    {"name": "orphan_order_campaigns", "max": 0,
     "sql": ("SELECT count(*) FROM fact_orders o WHERE o.campaign_id IS NOT NULL AND NOT EXISTS "
             "(SELECT 1 FROM dim_campaign c WHERE c.campaign_id = o.campaign_id)")},
]


def run_checks(conn, checks=None):
    failed = []
    for c in (checks or CHECKS):
        value = conn.execute(c["sql"]).fetchone()[0] or 0
        if value > c["max"]:
            failed.append({"name": c["name"], "value": value, "threshold": c["max"]})
    return failed


if __name__ == "__main__":
    import sys
    from warehouse.load import connect
    failures = run_checks(connect())
    for f in failures:
        print("FAIL", f)
    sys.exit(1 if failures else 0)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PG_DSN=postgresql://fs:change_me@localhost:5432/funnelstream pytest tests/test_checks.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add warehouse/checks.sql warehouse/run_checks.py tests/test_checks.py
git commit -m "feat: sql data quality checks with thresholds"
```

Note: `warehouse/checks.sql` is the readable copy of the same queries; `CHECKS` in `run_checks.py` is the source the runner uses. Keep them identical or delete `checks.sql`.

---

## Task 17: Samples, benchmark, failure demo and README

**Files:**
- Create: `scripts/export_samples.py`
- Create: `scripts/benchmark.py`
- Create: `scripts/failure_demo.py`
- Modify: `README.md`
- Test: `tests/test_scripts.py`

**Interfaces:**
- Consumes: Gold tables, Kafka, Spark job from Tasks 7–9
- Produces:
  - `export_samples(conn, out_dir, rows=10_000) -> list[str]` writes one CSV per Gold and Silver table, capped at `rows`.
  - `benchmark(events: int) -> dict` returns `{events, seconds, events_per_second}` and writes it to `docs/benchmark.md`.
  - `failure_demo()` kills and restarts the Spark query, then runs a duplicate check.

- [ ] **Step 1: Write the failing tests**

`tests/test_scripts.py`:

```python
import csv
from pathlib import Path
from scripts.export_samples import export_rows


def test_export_caps_rows_and_writes_header(tmp_path):
    rows = [{"order_id": f"o{i}", "total": i} for i in range(25_000)]
    path = export_rows(rows, tmp_path / "fact_orders.csv", limit=10_000)
    with open(path, newline="") as f:
        r = list(csv.reader(f))
    assert r[0] == ["order_id", "total"]
    assert len(r) - 1 == 10_000


def test_export_handles_fewer_rows_than_limit(tmp_path):
    rows = [{"a": 1}, {"a": 2}]
    path = export_rows(rows, tmp_path / "x.csv", limit=10_000)
    with open(path, newline="") as f:
        assert len(list(csv.reader(f))) == 3


def test_benchmark_report_fields():
    from scripts.benchmark import summarise
    s = summarise(events=2_000_000, seconds=400.0)
    assert s == {"events": 2_000_000, "seconds": 400.0, "events_per_second": 5000.0}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_scripts.py -v`
Expected: FAIL with `ModuleNotFoundError: scripts.export_samples`.

- [ ] **Step 3: Write `scripts/export_samples.py`**

```python
import csv
from pathlib import Path


def export_rows(rows: list[dict], path, limit: int = 10_000) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = rows[:limit]
    fields = list(rows[0].keys()) if rows else []
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    return path


if __name__ == "__main__":
    from warehouse.load import connect
    conn = connect()
    for table in ["fact_orders", "fact_order_lines", "fact_funnel_daily", "fact_campaign_daily",
                  "fact_attribution", "fact_product_recs", "fact_city_product_daily",
                  "dim_customer", "dim_product", "dim_campaign", "dim_date"]:
        cur = conn.execute(f"SELECT * FROM {table} LIMIT 10000")
        cols = [d.name for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        print(export_rows(rows, Path("samples") / f"{table}.csv"))
```

- [ ] **Step 4: Write `scripts/benchmark.py`**

```python
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def summarise(events: int, seconds: float) -> dict:
    return {"events": events, "seconds": seconds, "events_per_second": round(events / seconds, 2)}


def run(events: int) -> dict:
    t0 = time.time()
    subprocess.run(["python", "scripts/run_simulator.py", "--events", str(events)], check=True)
    result = summarise(events, round(time.time() - t0, 2))
    out = Path("docs/benchmark.md")
    out.parent.mkdir(exist_ok=True)
    out.write_text(f"# Benchmark\n\nRun at {datetime.now(timezone.utc).isoformat()}\n\n```json\n{json.dumps(result, indent=2)}\n```\n")
    return result


if __name__ == "__main__":
    import sys
    print(run(int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000))
```

- [ ] **Step 5: Write `scripts/failure_demo.py`**

```python
"""Kill the Spark query mid-run, restart it from its checkpoint, then check for duplicates."""
import subprocess
import time


def duplicates_in_silver(conn) -> int:
    return conn.execute("SELECT count(*) - count(DISTINCT event_id) FROM silver_events").fetchone()[0]


def main():
    proc = subprocess.Popen(["python", "streaming/jobs/silver_clean.py"])
    time.sleep(60)
    proc.kill()
    proc.wait()
    print("killed; restarting from checkpoint")
    subprocess.run(["python", "streaming/jobs/silver_clean.py"], timeout=300, check=False)
    from warehouse.load import connect
    d = duplicates_in_silver(connect())
    print("duplicates in silver_events:", d)
    raise SystemExit(0 if d == 0 else 1)


if __name__ == "__main__":
    main()
```

Note for the implementer: `silver_clean.py` needs a `__main__` that runs the streaming query from Tasks 7 and 8 with a checkpoint on MinIO. Add it in this task; the demo depends on it.

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_scripts.py -v`
Expected: 3 passed.

- [ ] **Step 7: Update `README.md`**

Write these sections in `README.md`: Quick start (`cp .env.example .env`, `docker compose up -d`, `python kafka/create_topics.py`, `python mock_apis/ad_api.py`, `python scripts/run_simulator.py --events 2000000`), Architecture (link to the spec), Metrics (link to `docs/metrics.md`), Demo (failure demo, benchmark), Troubleshooting (Java 17 required, Python 3.11 required).

- [ ] **Step 8: Commit**

```bash
git add scripts README.md tests/test_scripts.py samples
git commit -m "feat: sample export, benchmark, failure demo and readme"
```

---

## Task 18: Power BI model and pages (manual, with checks)

**Files:**
- Create: `dashboards/powerbi/funnelstream.pbix` (saved from Power BI Desktop)
- Create: `dashboards/powerbi/measures.dax`
- Create: `dashboards/powerbi/screenshots/` (one PNG per page)

**Interfaces:**
- Consumes: Postgres Gold tables (Task 10) through the Postgres connector in Import mode
- Produces: five pages (Executive, Campaign, Funnel, Delivery and COD, City and product) plus Customer 360 if Task 19 is done.

This task has no unit tests. Each step ends with a visible check.

- [ ] **Step 1: Connect Power BI to Postgres**

In Power BI Desktop: Get Data → PostgreSQL database. Server `localhost:5432`, database `funnelstream`, user `analyst`. Import mode. Load `fact_orders`, `fact_campaign_daily`, `fact_funnel_daily`, `fact_order_lines`, `fact_city_product_daily`, `fact_product_recs`, `dim_campaign`, `dim_date`, `dim_customer`, `dim_product`.
Check: each table shows row counts in Data view.

- [ ] **Step 2: Create relationships**

Model view: `fact_campaign_daily[campaign_id]` → `dim_campaign[campaign_id]` (many to one). `fact_orders[date_key]` → `dim_date[date_key]`. `fact_order_lines[product_key]` → `dim_product[product_key]`.
Check: no ambiguous relationship warnings.

- [ ] **Step 3: Add measures**

Write `dashboards/powerbi/measures.dax` with the headline measures and paste them into a `_Measures` table:

```dax
ROAS Delivered =
DIVIDE ( SUM ( fact_campaign_daily[revenue_delivered] ), SUM ( fact_campaign_daily[spend] ) )

ROAS Placed =
DIVIDE ( SUM ( fact_campaign_daily[revenue_placed] ), SUM ( fact_campaign_daily[spend] ) )

CPA =
DIVIDE ( SUM ( fact_campaign_daily[spend] ), SUM ( fact_campaign_daily[orders_placed] ) )

Cancel Rate =
DIVIDE ( SUM ( fact_campaign_daily[orders_cancelled] ), SUM ( fact_campaign_daily[orders_placed] ) )

Failed Delivery Rate =
DIVIDE ( SUM ( fact_campaign_daily[orders_failed_delivery] ), SUM ( fact_campaign_daily[orders_placed] ) )
```

Check: `ROAS Delivered` on a campaign with zero spend shows blank, not an error.

- [ ] **Step 4: Build the five pages**

Build the pages in the order listed in spec section 9. Campaign page shows `ROAS Placed` and `ROAS Delivered` side by side and the 2–4 day lag note.
Check: each page renders with no error icons.

- [ ] **Step 5: Save and export**

Save as `dashboards/powerbi/funnelstream.pbix`. Save one PNG per page into `dashboards/powerbi/screenshots/`.
Check: files exist; the `.pbix` opens after closing Power BI Desktop.

- [ ] **Step 6: Commit**

```bash
git add dashboards
git commit -m "feat: power bi model, measures and five dashboard pages"
```

---

## Task 19: Customer 360 (stretch; build only if Tasks 1–18 pass)

**Files:**
- Create: `batch/customer_360.py`
- Modify: `warehouse/ddl.sql` (add `fact_customer_360`)
- Test: `tests/test_customer_360.py`

**Interfaces:**
- Consumes: `fact_orders`, `fact_order_lines`, `silver_events` (session counts), `fact_attribution` (last campaign)
- Produces: `build_customer_360(orders, events, attribution) -> list[dict]` one row per `customer_id` with the columns in spec section 7.

- [ ] **Step 1: Write the failing tests**

`tests/test_customer_360.py`:

```python
from datetime import datetime
from batch.customer_360 import build_customer_360


def test_one_row_per_customer():
    orders = [
        {"order_id": "o1", "customer_id": "c1", "order_total": 100.0, "is_delivered": True,
         "is_cancelled": False, "order_created_at": datetime(2026, 10, 1), "payment_method": "cod",
         "city": "Cairo", "country": "EG", "customer_key": 1},
        {"order_id": "o2", "customer_id": "c1", "order_total": 50.0, "is_delivered": False,
         "is_cancelled": True, "order_created_at": datetime(2026, 10, 2), "payment_method": "card",
         "city": "Cairo", "country": "EG", "customer_key": 1},
    ]
    rows = build_customer_360(orders, events=[], attribution=[])
    assert len(rows) == 1
    r = rows[0]
    assert r["orders_count"] == 2
    assert r["delivered_count"] == 1
    assert r["cancelled_count"] == 1
    assert r["total_spend"] == 100.0
    assert r["avg_order_value"] == 75.0
    assert r["is_repeat_buyer"] is True
    assert r["preferred_payment_method"] == "cod"


def test_no_orders_customer_is_not_in_table():
    events = [{"customer_id": "c9", "session_id": "s1", "event_type": "page_view", "event_time": datetime(2026, 10, 1)}]
    assert build_customer_360([], events, []) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_customer_360.py -v`
Expected: FAIL with `ModuleNotFoundError: batch.customer_360`.

- [ ] **Step 3: Write `batch/customer_360.py`**

```python
from collections import Counter, defaultdict


def build_customer_360(orders, events, attribution):
    by_c = defaultdict(list)
    for o in orders:
        by_c[o["customer_id"]].append(o)
    sessions = defaultdict(set)
    for e in events:
        if e.get("customer_id"):
            sessions[e["customer_id"]].add(e["session_id"])

    out = []
    for cust, os_ in by_c.items():
        placed = [o for o in os_ if not o["is_cancelled"]]
        total = round(sum(o["order_total"] for o in placed), 2)
        pay = Counter(o["payment_method"] for o in os_)
        last = max(os_, key=lambda o: o["order_created_at"])
        out.append({
            "customer_id": cust, "customer_key": os_[0]["customer_key"],
            "country": os_[0]["country"], "city": os_[0]["city"],
            "total_sessions": len(sessions[cust]),
            "orders_count": len(os_),
            "delivered_count": sum(1 for o in os_ if o["is_delivered"]),
            "cancelled_count": sum(1 for o in os_ if o["is_cancelled"]),
            "total_spend": total,
            "avg_order_value": round(total / len(placed), 2) if placed else None,
            "first_order_at": min(o["order_created_at"] for o in os_),
            "last_order_at": last["order_created_at"],
            "is_repeat_buyer": len(os_) > 1,
            "preferred_payment_method": pay.most_common(1)[0][0],
        })
    return out
```

- [ ] **Step 4: Add the DDL and run tests**

Append to `warehouse/ddl.sql`:

```sql
CREATE TABLE IF NOT EXISTS fact_customer_360 (
  customer_id text PRIMARY KEY, customer_key bigint, country text, city text,
  total_sessions int, orders_count int, delivered_count int, cancelled_count int,
  total_spend numeric(12,2), avg_order_value numeric(12,2),
  first_order_at timestamptz, last_order_at timestamptz, is_repeat_buyer boolean,
  preferred_payment_method text
);
```

Run: `pytest tests/test_customer_360.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add batch/customer_360.py warehouse/ddl.sql tests/test_customer_360.py
git commit -m "feat: customer 360 table (stretch)"
```

---

## Task Order and Timeline

| Dates | Tasks | Gate before moving on |
|---|---|---|
| 4–5 Oct | 1, 2, 3 | Compose stack healthy; simulator journeys pass tests |
| 6–7 Oct | 4, 5, 6, 7, 8 | Kafka receives events; Silver has no duplicates |
| 8–9 Oct | 9, 10, 11 | Gold tables load to Postgres idempotently |
| 10–11 Oct | 12, 13, 14, 15, 16 | Batch runs end to end (DAG or fallback); checks pass |
| 11–12 Oct | 17, 18 | Demo works twice in a row; Power BI pages render |
| 11–12 Oct | 19 (stretch) | Only if 1–18 pass |
| 12 Oct | Failure demo, 1–2M run, rehearsal | |
| 13 Oct | Defense | |

## Cut Order If Behind

1. Task 19 (Customer 360).
2. Task 16 SQL checks reduced to the five most important.
3. Task 15 DAG replaced by `batch/run_batch.py` only (the fallback is already the same code).
4. Task 18 reduced to Campaign and Funnel pages.
5. Never cut: Tasks 4, 8, 9, 12, 13 (correctness of the headline numbers) and Task 17 failure demo.

---

## Self-Review

**Spec coverage.** Spec sections map to tasks as follows: §2 scope → Tasks 1–19; §3 architecture → Tasks 6–10, 15; §4 sources and realism → Tasks 3, 5; §5 Kafka → Tasks 2, 6; §6 lifecycle and rank → Tasks 4, 9; §7 medallion → Tasks 7, 8, 9, 10, 11, 12, 13, 14; §8 metrics → Task 12 and `docs/metrics.md`; §9 Power BI → Task 18; §10 SQL checks → Task 16; §11 reliability → Tasks 7, 8, 15; §12 security → Task 1 (`.env`), Task 10 (roles noted); §13 success criteria → Tasks 17, 18; §14 risks → Task order and cut order.

**Gaps found and fixed in this plan.**
- Security roles (`analyst`, `engineer`, `admin`) are not created by a task. Add `CREATE ROLE` statements to Task 10's `ddl.sql` when running it; Power BI connects as `analyst`.
- `silver_clean.py` needs a `__main__` streaming entry point. Noted in Task 17 Step 5; the implementer must add it there.
- `batch/sources.py` readers are described in the Task 15 note and must be written in that task.

**Placeholder scan.** No "TBD" or "TODO" steps. Notes marked "for the implementer" name the exact file and function to write.

**Type consistency.** `resolve_current_status` (Task 4) is the one used in Tasks 8–9. `STATUS_RANK` is defined once in `streaming/common/status.py`. `upsert` signature is the same in Tasks 10–15. `route` signature is the same in Tasks 6 and 17.

---

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-10-04-funnelstream.md`. Please review the plan. Which execution approach would you prefer?

- **Subagent-driven**: a fresh subagent implements each task and a fresh reviewer checks it before the next one starts, then a whole-branch review at the end. Most thorough; costs a fresh context per task and per review.
- **Native**: I implement every task myself in this session, then one fresh reviewer checks the whole branch. Cheapest and fastest; no independent review until the end.

For this plan I recommend **Native**. The tasks depend on each other's interfaces, but there are 19 of them and the deadline is 9 days; per-task reviewers would cost more time than they save. Does the plan capture what you want, and which approach should we use?
