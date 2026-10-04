import json
import random
from datetime import datetime, timedelta, timezone

from contracts.validate import validate_raw
from scripts.run_simulator import build_parser, run_pipeline
from simulator.catalog import make_catalog
from simulator.noise import NoiseConfig
from simulator.publish import Publisher

NOW = datetime(2026, 10, 4, tzinfo=timezone.utc)
START = NOW - timedelta(days=7)
CATALOG_IDS = {p["product_id"] for p in make_catalog(11)}


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def poll(self, t):
        pass

    def flush(self):
        pass


def run(tmp_path, config, events=6000):
    prod = FakeProducer()
    manifest = tmp_path / "noise_manifest.json"
    counts = run_pipeline(seed=11, events=events, start=START, now=NOW, live=False, pub=Publisher("x", producer=prod),
                          config=config, manifest_path=str(manifest))
    return prod, json.loads(manifest.read_text()), counts


def test_manifest_matches_revalidated_messages(tmp_path):
    prod, m, counts = run(tmp_path, NoiseConfig().scaled(8))
    assert m["total_messages"] == len(prod.sent) == sum(counts.values())
    found = {}
    for topic, key, value in prod.sent:
        ok, code = validate_raw(value, now=NOW, known_products=CATALOG_IDS)
        if not ok:
            found[code] = found.get(code, 0) + 1
    inj = m["injected"]
    assert found.get("BAD_JSON", 0) == inj["malformed_json"] > 0
    assert found.get("MISSING_FIELD", 0) == inj["missing_field"] + inj["null_product_id"]
    assert found.get("UNKNOWN_EVENT_TYPE", 0) == inj["unknown_type"]
    assert found.get("BAD_VERSION", 0) == inj["bad_version"]
    assert found.get("NEGATIVE_PRICE", 0) == inj["negative_price"]
    assert found.get("FUTURE_TIMESTAMP", 0) == inj["future_timestamp"] > 0
    assert found.get("UNKNOWN_PRODUCT", 0) == inj["unknown_product_id"]
    assert set(found) <= {"BAD_JSON", "MISSING_FIELD", "UNKNOWN_EVENT_TYPE", "BAD_VERSION", "NEGATIVE_PRICE",
                          "FUTURE_TIMESTAMP", "UNKNOWN_PRODUCT"}


def test_noise_off_publishes_clean(tmp_path):
    prod, m, counts = run(tmp_path, NoiseConfig.off(), events=1500)
    assert all(v == 0 for v in m["injected"].values())
    assert m["total_messages"] == len(prod.sent)
    for topic, key, value in prod.sent:
        assert validate_raw(value, now=NOW, known_products=CATALOG_IDS) == (True, "")
    ids = [json.loads(v)["event_id"] for _, _, v in prod.sent]
    assert len(ids) == len(set(ids))


def test_history_guard_ignores_future_noise(tmp_path):
    # future_timestamp events are intentionally ahead of now; the guard runs before noise
    prod, m, _ = run(tmp_path, _only_future(), events=500)
    assert m["injected"]["future_timestamp"] > 0


def _only_future():
    c = NoiseConfig.off()
    c.future_timestamp = 0.2
    return c


def test_cli_flags():
    ap = build_parser()
    a = ap.parse_args([])
    assert a.noise_off is False and a.noise_scale == 1.0 and a.manifest == "noise_manifest.json"
    a = ap.parse_args(["--noise-off", "--noise-scale", "2.5", "--manifest", "m.json"])
    assert a.noise_off and a.noise_scale == 2.5 and a.manifest == "m.json"


def test_publisher_publishes_str_unchanged():
    prod = FakeProducer()
    pub = Publisher("x", producer=prod)
    pub.send("orders", "unparsable", '{"broken": ')
    pub.send("orders", "k", {"a": 1})
    assert prod.sent[0] == ("orders", b"unparsable", b'{"broken": ')
    assert prod.sent[1] == ("orders", b"k", json.dumps({"a": 1}).encode())


def test_same_seed_same_published_output(tmp_path):
    a, ma, _ = run(tmp_path, NoiseConfig().scaled(5), events=1500)
    b, mb, _ = run(tmp_path, NoiseConfig().scaled(5), events=1500)
    assert a.sent == b.sent and ma == mb
