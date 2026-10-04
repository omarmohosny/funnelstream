import json
import random
import threading
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from http.server import HTTPServer

from mock_apis.ad_api import Handler, apply_api_noise, make_handler
from simulator.campaigns import CAMPAIGNS, campaign_numbers

URL = "/campaigns?report_date=2026-10-04&as_of=2026-10-04T12:00:00"


def serve(handler):
    server = HTTPServer(("127.0.0.1", 0), handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


def pull(server, n):
    host, port = server.server_address
    out = []
    for _ in range(n):
        try:
            with urllib.request.urlopen(f"http://{host}:{port}{URL}") as r:
                out.append((r.status, json.loads(r.read())))
        except urllib.error.HTTPError as e:
            out.append((e.code, json.loads(e.read())))
    return out


def stop(server):
    server.shutdown()
    server.server_close()


def clean_rows():
    return [campaign_numbers(c["campaign_id"], date(2026, 10, 4), datetime(2026, 10, 4, 12, tzinfo=timezone.utc), 42)
            for c in CAMPAIGNS]


def test_noise_on_produces_all_kinds_and_is_deterministic():
    s1, s2 = serve(make_handler(noise=True, seed=7)), serve(make_handler(noise=True, seed=7))
    try:
        a, b = pull(s1, 200), pull(s2, 200)
    finally:
        stop(s1)
        stop(s2)
    assert a == b
    codes = [c for c, _ in a]
    assert 0 < codes.count(503) < 40
    assert all(body == {"error": "service unavailable"} for c, body in a if c == 503)
    ok = [body for c, body in a if c == 200]
    assert any(len(body) > len(CAMPAIGNS) for body in ok)  # duplicated row
    assert any(any(r["clicks"] > r["impressions"] for r in body) for body in ok)
    assert any(any(r["spend"] < 0 for r in body) for body in ok)


def test_different_seed_differs():
    s1, s2 = serve(make_handler(noise=True, seed=1)), serve(make_handler(noise=True, seed=2))
    try:
        a, b = pull(s1, 100), pull(s2, 100)
    finally:
        stop(s1)
        stop(s2)
    assert a != b


def test_noise_off_is_clean():
    s = serve(make_handler(noise=False))
    s0 = serve(Handler)
    try:
        res = pull(s, 100)
        base = pull(s0, 1)[0]
    finally:
        stop(s)
        stop(s0)
    assert all(c == 200 for c, _ in res)
    for _, body in res:
        assert body == base[1]
        assert len(body) == 3
        assert all(r["clicks"] <= r["impressions"] and r["spend"] >= 0 for r in body)


def test_apply_api_noise_pure_and_seeded():
    rows = clean_rows()
    snap = json.loads(json.dumps(rows))
    seen = set()
    for i in range(400):
        out = apply_api_noise(rows, random.Random(i))
        assert rows == snap
        if len(out) > len(rows):
            seen.add("dup")
        if any(r["clicks"] > r["impressions"] for r in out):
            seen.add("clicks")
        if any(r["spend"] < 0 for r in out):
            seen.add("spend")
    assert seen == {"dup", "clicks", "spend"}
    assert apply_api_noise(rows, random.Random(5)) == apply_api_noise(rows, random.Random(5))
