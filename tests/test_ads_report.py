"""Yesterday's ad report: pull once, cached on rerun, retries, validation. Fake store, real mock API server."""
import re
import threading
import urllib.error
from datetime import date, datetime, timezone
from http.server import HTTPServer

import pytest

from batch import ads_report
from batch.ads_report import COLUMNS, SNAPSHOT_KEYS, final_as_of, pull, run, validate, yesterday
from mock_apis.ad_api import make_handler
from simulator.campaigns import CAMPAIGNS

NOW = datetime(2026, 10, 6, 3, 0, tzinfo=timezone.utc)
YESTERDAY = date(2026, 10, 5)


class FakeStore:
    def __init__(self):
        self.tables = {"silver_campaign_snapshots": {}, "silver_quarantine": {}}
        self.upserts = []

    def upsert(self, table, rows, key_cols):
        self.upserts.append((table, len(rows)))
        for r in rows:
            self.tables[table][tuple(r[k] for k in key_cols)] = r
        return len(rows)

    def query(self, sql, params=None, retry=True):
        d = date.fromisoformat(re.search(r"DATE'([\d-]+)'", sql).group(1))
        rows = [r for r in self.tables["silver_campaign_snapshots"].values() if r["report_date"] == d]
        rows.sort(key=lambda r: (r["campaign_id"], r["pulled_at"]))
        return [tuple(r[c] for c in COLUMNS) for r in rows]

    def qualified(self, table):
        return table

    def close(self):
        pass


@pytest.fixture
def api():
    servers = []

    def start(noise=False, seed=0):
        srv = HTTPServer(("127.0.0.1", 0), make_handler(noise, seed))
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        servers.append(srv)
        return "http://127.0.0.1:%d" % srv.server_address[1]

    yield start
    for s in servers:
        s.shutdown()
        s.server_close()


def _run(store, url, **kw):
    return run(store, url, now=kw.pop("now", NOW), report_dir=kw.pop("report_dir"), **kw)


def test_yesterday_is_the_previous_utc_day():
    assert yesterday(NOW) == YESTERDAY
    assert yesterday(datetime(2026, 10, 6, 0, 0, tzinfo=timezone.utc)) == YESTERDAY
    assert yesterday(datetime(2026, 10, 5, 23, 59, tzinfo=timezone.utc)) == date(2026, 10, 4)


def test_first_run_pulls_and_stores_one_row_per_campaign(api, tmp_path):
    store = FakeStore()
    res = _run(store, api(), report_dir=tmp_path)
    assert res["status"] == "pulled"
    assert res["report_date"] == YESTERDAY
    assert [r["campaign_id"] for r in res["rows"]] == sorted(c["campaign_id"] for c in CAMPAIGNS)
    assert all(r["pulled_at"] == final_as_of(YESTERDAY) for r in res["rows"])
    assert (tmp_path / "ads_report_2026-10-05.json").exists()


def test_same_day_rerun_returns_the_same_report_without_pulling(api, tmp_path):
    store, url, calls = FakeStore(), api(), []

    def counting_pull(*a, **k):
        calls.append(1)
        return pull(*a, **k)

    first = _run(store, url, pull_fn=counting_pull, report_dir=tmp_path)
    again = _run(store, url, pull_fn=counting_pull, report_dir=tmp_path,
                 now=datetime(2026, 10, 6, 20, 0, tzinfo=timezone.utc))
    assert (first["status"], again["status"]) == ("pulled", "cached")
    assert len(calls) == 1
    assert again["rows"] == first["rows"]
    assert len(store.tables["silver_campaign_snapshots"]) == len(CAMPAIGNS)


def test_next_day_reports_the_new_yesterday(api, tmp_path):
    store, url = FakeStore(), api()
    _run(store, url, report_dir=tmp_path)
    nxt = _run(store, url, report_dir=tmp_path, now=datetime(2026, 10, 7, 3, 0, tzinfo=timezone.utc))
    assert nxt["report_date"] == date(2026, 10, 6) and nxt["status"] == "pulled"
    assert len(store.tables["silver_campaign_snapshots"]) == 2 * len(CAMPAIGNS)


def test_force_pulls_again_but_row_keys_do_not_multiply(api, tmp_path):
    store, url = FakeStore(), api()
    _run(store, url, report_dir=tmp_path)
    res = _run(store, url, force=True, report_dir=tmp_path)
    assert res["status"] == "pulled"
    assert len(store.tables["silver_campaign_snapshots"]) == len(CAMPAIGNS)


def test_partial_store_triggers_a_pull(api, tmp_path):
    store, url = FakeStore(), api()
    _run(store, url, report_dir=tmp_path)
    store.tables["silver_campaign_snapshots"].pop(next(iter(store.tables["silver_campaign_snapshots"])))
    assert _run(store, url, report_dir=tmp_path)["status"] == "pulled"


def test_today_or_future_is_refused(api, tmp_path):
    with pytest.raises(ValueError):
        _run(FakeStore(), api(), report_date=NOW.date(), report_dir=tmp_path)


def test_report_is_full_day_numbers(api, tmp_path):
    from simulator.campaigns import campaign_numbers
    res = _run(FakeStore(), api(), report_dir=tmp_path)
    row = res["rows"][0]
    exp = campaign_numbers(row["campaign_id"], YESTERDAY, final_as_of(YESTERDAY), 42)
    assert row["impressions"] == exp["impressions"] and float(row["spend"]) == exp["spend"]


def test_pull_retries_503_then_succeeds():
    calls, sleeps = [], []

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"[]"

    def fake_open(url, timeout=0):
        calls.append(url)
        if len(calls) < 3:
            raise urllib.error.HTTPError(url, 503, "unavailable", {}, None)
        return Resp()

    real = ads_report.urllib.request.urlopen
    ads_report.urllib.request.urlopen = fake_open
    try:
        assert pull("http://x", YESTERDAY, final_as_of(YESTERDAY), sleep=sleeps.append) == []
    finally:
        ads_report.urllib.request.urlopen = real
    assert len(calls) == 3 and sleeps == [1.0, 2.0]


def test_pull_gives_up_after_three_503_and_never_retries_400():
    def make(code):
        n = []

        def fake_open(url, timeout=0):
            n.append(1)
            raise urllib.error.HTTPError(url, code, "x", {}, None)
        return fake_open, n

    real = ads_report.urllib.request.urlopen
    try:
        fake, n = make(503)
        ads_report.urllib.request.urlopen = fake
        with pytest.raises(urllib.error.HTTPError):
            pull("http://x", YESTERDAY, final_as_of(YESTERDAY), sleep=lambda s: None)
        assert len(n) == 3
        fake, n = make(400)
        ads_report.urllib.request.urlopen = fake
        with pytest.raises(urllib.error.HTTPError):
            pull("http://x", YESTERDAY, final_as_of(YESTERDAY), sleep=lambda s: None)
        assert len(n) == 1
    finally:
        ads_report.urllib.request.urlopen = real


def _row(**kw):
    base = {"campaign_id": "camp_201", "report_date": "2026-10-05", "pulled_at": "2026-10-06T00:00:00+00:00",
            "impressions": 1000, "clicks": 20, "spend": 100.0, "platform_conversions": 1, "currency": "EGP"}
    base.update(kw)
    return base


def test_validate_rejects_bad_rows_and_collapses_duplicates():
    good, bad = validate([_row(), _row(), _row(campaign_id="camp_202", spend=-5.0),
                          _row(campaign_id="camp_203", clicks=2000)])
    assert [g["campaign_id"] for g in good] == ["camp_201"]
    assert [(b[0]["campaign_id"], b[1]) for b in bad] == [("camp_202", "NEGATIVE_SPEND"),
                                                          ("camp_203", "CLICKS_GT_IMPRESSIONS")]
    assert good[0]["utm_campaign"] == "eid_sale_1" and good[0]["ad_platform"] == "google"


def test_noisy_api_rows_are_quarantined_not_stored(api, tmp_path):
    store = FakeStore()
    seen = set()
    for seed in range(40):  # find a seed whose first response carries a bad row
        s = FakeStore()
        try:
            res = run(s, api(noise=True, seed=seed), now=NOW, report_dir=tmp_path)
        except urllib.error.HTTPError:
            continue
        if res["rejected"]:
            store, seen = s, {r[1] for r in s.tables["silver_quarantine"].values() and
                              [(0, q["error_code"]) for q in s.tables["silver_quarantine"].values()]}
            break
    assert seen and seen <= {"NEGATIVE_SPEND", "CLICKS_GT_IMPRESSIONS"}
    stored = store.tables["silver_campaign_snapshots"].values()
    assert all(r["spend"] >= 0 and r["clicks"] <= r["impressions"] for r in stored)


def test_snapshot_columns_match_the_table_spec():
    from warehouse.tables import SILVER_MIRROR_TABLES
    spec = next(t for t in SILVER_MIRROR_TABLES if t.name == "silver_campaign_snapshots")
    assert set(COLUMNS) == {c.name for c in spec.columns}
    assert set(SNAPSHOT_KEYS) <= set(COLUMNS)
