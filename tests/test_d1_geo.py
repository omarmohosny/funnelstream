"""Amendment D, task D1: geography reference, district on simulator output, tracking URL, noise."""
import json
import random
from datetime import date, datetime, timezone
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import pytest

from contracts.geo import (COUNTRY, DISTRICT_WEIGHTS, DISTRICTS, alias_variants, district_violation,
                           normalize_district)
from contracts.normalize import CANONICAL_CITIES
from contracts.validate import APP_EVENT_TYPES, validate_event, validate_raw
from scripts.run_simulator import iter_batch
from simulator.campaigns import CAMPAIGNS, campaign_numbers
from simulator.catalog import make_catalog, make_customers
from simulator.core import journey
from simulator.noise import NoiseConfig, NoiseCounter, apply_noise

START = datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 6, tzinfo=timezone.utc)
CATALOG = make_catalog(5)
CUSTOMERS = make_customers(5, 300)
CATALOG_IDS = {p["product_id"] for p in CATALOG}

# make_customers(7, 10) fields recorded BEFORE the district change (spec 18: older draws must not move)
PINNED_CUSTOMERS_SEED7 = [
    ("cust_000001", "Mohamed Abdelaziz", "+201226018159", "Cairo", "desktop", "2026-03-05", True),
    ("cust_000002", "Dina Gamal", "+201086091390", "Cairo", "mobile", "2025-02-17", True),
    ("cust_000003", "Mariam Mahmoud", "+201146281948", "Alexandria", "mobile", "2026-01-17", True),
    ("cust_000004", "Salma Ragab", "+201090937865", "Giza", "mobile", "2025-07-04", False),
    ("cust_000005", "Dina Mahmoud", "+201119487574", "Cairo", "desktop", "2025-06-18", False),
    ("cust_000006", "Hagar Ragab", "+201176018955", "Alexandria", "desktop", "2025-03-12", False),
    ("cust_000007", "Hagar Hassan", "+201271049746", "Other", "mobile", "2025-06-22", False),
    ("cust_000008", "Ahmed Ibrahim", "+201503423667", "Giza", "mobile", "2025-05-21", False),
    ("cust_000009", "Nour Abdelaziz", "+201265632122", "Cairo", "mobile", "2025-09-27", True),
    ("cust_000010", "Dina Hassan", "+201168599528", "Giza", "desktop", "2026-02-12", True),
]
FIELDS = ("customer_id", "name", "phone", "city", "device_primary", "signup_date", "marketing_consent")


def _batch(seed=3, max_events=4000):
    return list(iter_batch(random.Random(seed), CUSTOMERS, CATALOG, START, max_events=max_events))


# ---- contracts/geo.py -------------------------------------------------------------------------

def test_hierarchy_completeness_and_counts():
    assert COUNTRY == "EG"
    assert set(DISTRICTS) == set(CANONICAL_CITIES)
    counts = {c: len(d) for c, d in DISTRICTS.items()}
    assert counts == {"Cairo": 15, "Giza": 8, "Alexandria": 10, "Mansoura": 2, "Tanta": 2, "Assiut": 2, "Other": 1}
    assert DISTRICTS["Other"] == ("Other",)


def test_weights_match_districts_and_sum_to_one():
    assert set(DISTRICT_WEIGHTS) == set(DISTRICTS)
    for city, names in DISTRICTS.items():
        assert len(DISTRICT_WEIGHTS[city]) == len(names)
        assert sum(DISTRICT_WEIGHTS[city]) == pytest.approx(1.0)
        assert all(w > 0 for w in DISTRICT_WEIGHTS[city])


def test_districts_unique_within_city_and_across_cities():
    seen = {}
    for city, names in DISTRICTS.items():
        assert len(set(names)) == len(names)
        for n in names:
            assert n not in seen, "%s under both %s and %s" % (n, seen[n], city)
            seen[n] = city


def test_normalize_district_canonical_and_case_variants():
    for city, names in DISTRICTS.items():
        for n in names:
            assert normalize_district(city, n) == n
            assert normalize_district(city, n.upper()) == n
            assert normalize_district(city, "  %s " % n.lower()) == n
            assert normalize_district(city, n.replace(" ", "-")) == n
            assert normalize_district(city, n.replace(" ", "_")) == n


@pytest.mark.parametrize("city,value,expected", [
    ("Cairo", "madinet nasr", "Nasr City"), ("Cairo", "Nasr-City", "Nasr City"),
    ("Cairo", "masr el gedida", "Heliopolis"), ("Cairo", "Misr El Gedida", "Heliopolis"),
    ("Cairo", "heliopolis", "Heliopolis"), ("Cairo", "Tagamoa", "New Cairo"),
    ("Cairo", "fifth settlement", "New Cairo"), ("Cairo", "5th settlement", "New Cairo"),
    ("Cairo", "el maadi", "Maadi"), ("Giza", "el haram", "Haram"), ("Giza", "Pyramids", "Haram"),
    ("Giza", "al haram", "Haram"), ("Giza", "el dokki", "Dokki"), ("Giza", "al dokki", "Dokki"),
    ("Giza", "el agouza", "Agouza"), ("Alexandria", "sidi gabr", "Sidi Gaber"),
    ("Alexandria", "El Raml", "Raml"), ("Alexandria", "el montaza", "Montaza"),
    ("Mansoura", "mansoura 1", "Mansoura First"), ("Mansoura", "Mansoura 2", "Mansoura Second"),
    ("Tanta", "tanta 1", "Tanta First"), ("Tanta", "tanta 2", "Tanta Second"),
    ("Assiut", "assiut 1", "Assiut First"), ("Assiut", "assiut 2", "Assiut Second"),
])
def test_normalize_district_aliases(city, value, expected):
    assert normalize_district(city, value) == expected


def test_every_alias_variant_resolves_under_its_city():
    for city, names in DISTRICTS.items():
        for n in names:
            for a in alias_variants(n):
                assert normalize_district(city, a) == n


def test_normalize_district_wrong_city_other_rule_and_bad_inputs():
    assert normalize_district("Giza", "Nasr City") == "unknown"
    assert normalize_district("Cairo", "Dokki") == "unknown"
    assert normalize_district("Cairo", "no such place") == "unknown"
    assert normalize_district("Other", "Other") == "Other"
    assert normalize_district("Other", "OTHER ") == "Other"
    assert normalize_district("Other", "Maadi") == "unknown"
    assert normalize_district("Cairo", "Other") == "unknown"
    assert normalize_district("unknown", "Maadi") == "unknown"
    assert normalize_district("Atlantis", "Maadi") == "unknown"
    assert normalize_district(None, "Maadi") == "unknown"
    assert normalize_district(5, "Maadi") == "unknown"
    assert normalize_district("Cairo", None) == "unknown"
    assert normalize_district("Cairo", 7) == "unknown"
    assert normalize_district("Cairo", "") == "unknown"


def test_district_violation():
    assert district_violation("Cairo", "Dokki") is True
    assert district_violation("Giza", "garbage") is True
    assert district_violation("Cairo", "nasr-city") is False
    assert district_violation("Cairo", "") is False
    assert district_violation("Cairo", "   ") is False
    assert district_violation("Cairo", None) is False
    assert district_violation("unknown", "Dokki") is False
    assert district_violation(None, "Dokki") is False
    assert district_violation("Other", "Maadi") is True
    assert district_violation("Other", "Other") is False


# ---- customers ---------------------------------------------------------------------------------

def test_seeded_customer_fields_unchanged_by_district_draw():
    got = [tuple(c[f] for f in FIELDS) for c in make_customers(7, 10)]
    assert got == PINNED_CUSTOMERS_SEED7


def test_customer_districts_belong_to_city_and_key_order():
    cs = make_customers(11, 2000)
    for c in cs:
        assert c["district"] in DISTRICTS[c["city"]]
    keys = list(cs[0])
    assert keys.index("district") == keys.index("city") + 1
    assert make_customers(11, 50) == make_customers(11, 50)
    cairo = [c["district"] for c in cs if c["city"] == "Cairo"]
    assert len(set(cairo)) > 8  # the weighted draw reaches many districts


# ---- events ------------------------------------------------------------------------------------

def test_every_app_event_has_district_valid_for_its_city():
    batch = _batch()
    app = [e for h, e in batch if h == "app"]
    assert app
    keys = list(app[0])
    assert keys.index("district") == keys.index("city") + 1
    for e in app:
        assert e["district"] in DISTRICTS[e["city"]]


def test_session_district_constant_and_anonymous_is_cairo():
    anon_seen = False
    rng = random.Random(21)
    for i in range(300):
        cust = CUSTOMERS[i % len(CUSTOMERS)] if i % 3 else None
        events = journey(rng, cust, CATALOG, None, START)
        assert len({(e["city"], e["district"]) for e in events}) == 1
        e0 = events[0]
        if cust is None:
            anon_seen = True
            assert e0["city"] == "Cairo" and e0["district"] in DISTRICTS["Cairo"]
        else:
            assert (e0["city"], e0["district"]) == (cust["city"], cust["district"])
    assert anon_seen


def test_order_events_carry_customer_district():
    cust_by_id = {c["customer_id"]: c for c in CUSTOMERS}
    orders = [e for h, e in _batch() if h == "order"]
    assert orders
    for e in orders:
        c = cust_by_id[e["customer_id"]]
        assert e["district"] == c["district"] and e["city"] == c["city"]
        if e["event_type"] == "order_created":
            assert list(e).index("district") == list(e).index("city") + 1


def test_order_created_has_utm_medium_and_keeps_source_campaign():
    created = [e for h, e in _batch() if e["event_type"] == "order_created"]
    camp_by_name = {c["utm_campaign"]: c for c in CAMPAIGNS}
    seen_camp = seen_none = False
    for e in created:
        p = e["payload"]
        assert "utm_medium" in p and "utm_source" in p and "utm_campaign" in p
        if p["utm_campaign"] is None:
            assert p["utm_medium"] is None and p["utm_source"] is None
            seen_none = True
        else:
            c = camp_by_name[p["utm_campaign"]]
            assert (p["utm_source"], p["utm_medium"]) == (c["utm_source"], c["utm_medium"])
            seen_camp = True
    assert seen_camp and seen_none


def test_campaign_mediums():
    mediums = {c["campaign_id"]: c["utm_medium"] for c in CAMPAIGNS}
    assert mediums == {"camp_201": "cpc", "camp_202": "paid_social", "camp_203": "paid_social"}
    assert all(c["utm_medium"] == ("cpc" if c["utm_source"] == "google" else "paid_social") for c in CAMPAIGNS)


def test_campaign_numbers_unchanged():
    n = campaign_numbers("camp_202", date(2026, 10, 5), datetime(2026, 10, 5, 12, tzinfo=ZoneInfo("Africa/Cairo")), 42)  # noon in Cairo: half of the Cairo day
    assert n == {"campaign_id": "camp_202", "report_date": "2026-10-05",
                 "pulled_at": "2026-10-05T12:00:00+03:00", "impressions": 44177, "clicks": 1002,
                 "spend": 970.2089, "platform_conversions": 51, "currency": "EGP"}


# ---- tracking URL ------------------------------------------------------------------------------

def _sessions(seed=4, n=400):
    rng = random.Random(seed)
    out = []
    for i in range(n):
        camp = CAMPAIGNS[i % len(CAMPAIGNS)] if i % 2 == 0 else None
        cust = CUSTOMERS[i % len(CUSTOMERS)] if i % 5 else None
        out.append((camp, journey(rng, cust, CATALOG, camp, START)))
    return out


def test_campaign_sessions_first_page_and_product_view_carry_all_utm():
    for camp, events in _sessions():
        if camp is None:
            continue
        pages = [e for e in events if e["event_type"] == "page_view"]
        views = [e for e in events if e["event_type"] == "product_view"]
        assert len(pages) == 1 and pages[0]["payload"]["page"] == "home"
        for e in (pages[0], views[0]):
            url = urlparse(e["payload"]["page_url"])
            assert url.scheme == "https" and url.netloc == "shop.example.eg"
            q = parse_qs(url.query)
            assert q == {"utm_source": [camp["utm_source"]], "utm_medium": [camp["utm_medium"]],
                         "utm_campaign": [camp["utm_campaign"]]}
        assert pages[0]["payload"]["page_url"].startswith("https://shop.example.eg/?utm_source=")
        assert views[0]["payload"]["page_url"].startswith(
            "https://shop.example.eg/p/%s?" % views[0]["payload"]["product_id"])
        for e in views[1:]:  # later product views of the session are plain
            assert "?" not in e["payload"]["page_url"]


def test_non_campaign_sessions_have_plain_urls():
    for camp, events in _sessions():
        if camp is not None:
            continue
        for e in events:
            if e["event_type"] == "page_view":
                assert e["payload"]["page_url"] == "https://shop.example.eg/"
            elif e["event_type"] == "product_view":
                assert e["payload"]["page_url"] == "https://shop.example.eg/p/%s" % e["payload"]["product_id"]


def test_no_app_event_has_old_utm_keys():
    for h, e in _batch():
        if h == "app":
            assert "utm_source" not in e["payload"] and "utm_campaign" not in e["payload"]
        elif e["event_type"] == "order_created":
            assert "utm_campaign" in e["payload"]  # order attribution fields stay


def test_click_id_follows_the_campaign_url_events():
    batch = _batch()
    for h, e in batch:
        if h != "app":
            continue
        has_utm = "utm_campaign=" in e["payload"].get("page_url", "")
        assert ("click_id" in e["payload"]) == has_utm
        if has_utm:
            assert e["payload"]["click_id"] == "clk_" + e["session_id"][5:]


# ---- validator ---------------------------------------------------------------------------------

def _good_event():
    return next(e for h, e in _batch(max_events=200) if e["event_type"] == "product_view")


def test_validator_accepts_new_fields_and_checks_types():
    e = _good_event()
    assert validate_event(e, now=NOW) == (True, "")
    for mutate in (lambda x: x.update(district=5), lambda x: x.update(district=["Maadi"]),
                   lambda x: x["payload"].update(page_url=5), lambda x: x["payload"].update(utm_medium=3),
                   lambda x: x["payload"].update(page_url={"a": 1})):
        bad = json.loads(json.dumps(e))
        mutate(bad)
        assert validate_event(bad, now=NOW) == (False, "BAD_VALUE")
    for mutate in (lambda x: x.update(district=None), lambda x: x.pop("district"),
                   lambda x: x["payload"].update(page_url=None), lambda x: x["payload"].pop("page_url"),
                   lambda x: x["payload"].update(utm_medium="CPC "), lambda x: x.update(district="nasr-city")):
        ok = json.loads(json.dumps(e))
        mutate(ok)
        assert validate_event(ok, now=NOW) == (True, "")


# ---- noise -------------------------------------------------------------------------------------

def _only(kind, rate=1.0):
    cfg = NoiseConfig.off()
    setattr(cfg, kind, rate)
    return cfg


def _run(stream, cfg, seed=1):
    c = NoiseCounter()
    out = list(apply_noise(iter(stream), random.Random(seed), cfg, CATALOG_IDS, c, now=NOW))
    return out, c


def _changed(stream, out, key):
    assert len(stream) == len(out)
    return [(a[1], b[1]) for a, b in zip(stream, out) if key(a[1]) != key(b[1])]


def test_new_kinds_are_declared_and_scaled():
    cfg = NoiseConfig()
    assert 0 < cfg.dirty_district < 1 and 0 < cfg.dirty_utm < 1
    assert NoiseConfig().scaled(0).dirty_district == 0 and NoiseConfig().scaled(0).dirty_utm == 0
    assert NoiseConfig().scaled(2.0).dirty_utm == pytest.approx(cfg.dirty_utm * 2)
    c = NoiseCounter().to_manifest()["injected"]
    assert "dirty_district" in c and "dirty_utm" in c


def test_dirty_district_manifest_count_equals_changed_events_and_still_valid():
    stream = _batch(seed=3, max_events=3000)
    out, c = _run(stream, _only("dirty_district", 0.3))
    changed = _changed(stream, out, lambda e: e.get("district"))
    assert c.counts["dirty_district"] == len(changed) > 20
    assert sum(c.counts.values()) == c.counts["dirty_district"]
    for before, after in changed:
        assert before["event_type"] in APP_EVENT_TYPES
        assert after["district"] != before["district"]
        assert normalize_district(after["city"], after["district"]) == before["district"]
        assert after["city"] == before["city"]
        assert validate_raw(after, now=NOW, known_products=CATALOG_IDS) == (True, "")
    others = [(a, b) for a, b in zip(stream, out) if a[1].get("district") == b[1].get("district")]
    assert all(a[1] == b[1] for a, b in others)


def test_dirty_district_only_hits_events_with_valid_district():
    stream = _batch(seed=3, max_events=1500)
    for _, e in stream[::7]:
        if e["event_type"] in APP_EVENT_TYPES:
            e["district"] = None
    out, c = _run(stream, _only("dirty_district", 1.0))
    for (_, before), (_, after) in zip(stream, out):
        if before.get("district") is None or before["event_type"] not in APP_EVENT_TYPES:
            assert after == before
    assert c.counts["dirty_district"] == sum(
        1 for _, e in stream if e["event_type"] in APP_EVENT_TYPES and e.get("district"))


def test_dirty_district_variants_exist_for_every_district():
    from simulator.noise import _district_variants
    for city, names in DISTRICTS.items():
        for n in names:
            vs = _district_variants(city, n)
            assert vs and n not in vs
            assert all(normalize_district(city, v) == n for v in vs)


def test_dirty_utm_manifest_count_equals_changed_events_and_still_valid():
    stream = _batch(seed=3, max_events=3000)
    out, c = _run(stream, _only("dirty_utm", 0.5))
    changed = _changed(stream, out, lambda e: e["payload"].get("page_url"))
    assert c.counts["dirty_utm"] == len(changed) > 10
    assert sum(c.counts.values()) == c.counts["dirty_utm"]
    for before, after in changed:
        assert "?utm_" in before["payload"]["page_url"]
        u0, u1 = before["payload"]["page_url"], after["payload"]["page_url"]
        assert u1.partition("?")[0] == u0.partition("?")[0]
        q0, q1 = parse_qs(urlparse(u0).query), parse_qs(urlparse(u1).query)
        assert set(q0) == set(q1) == {"utm_source", "utm_medium", "utm_campaign"}
        for k in q0:
            assert q1[k][0].strip().lower() == q0[k][0]
        assert {k: v for k, v in after.items() if k != "payload"} == {k: v for k, v in before.items() if k != "payload"}
        assert validate_raw(after, now=NOW, known_products=CATALOG_IDS) == (True, "")


def test_dirty_utm_never_touches_plain_urls():
    stream = _batch(seed=3, max_events=2000)
    out, _ = _run(stream, _only("dirty_utm", 1.0))
    for (_, a), (_, b) in zip(stream, out):
        if "?utm_" not in a["payload"].get("page_url", ""):
            assert a == b


def test_scale_zero_gives_none_and_manifest_is_exact_with_all_kinds_on():
    stream = _batch(seed=5, max_events=2500)
    out, c = _run(stream, NoiseConfig().scaled(0))
    assert [o[1] for o in out] == [s[1] for s in stream]
    assert sum(c.counts.values()) == 0
    out, c = _run(stream, NoiseConfig().scaled(8))
    assert c.counts["dirty_district"] > 0 and c.counts["dirty_utm"] > 0


def test_new_kinds_do_not_move_older_noise_decisions():
    stream = _batch(seed=3, max_events=2500)
    base = NoiseConfig()
    base.dirty_district = base.dirty_utm = 0.0
    out_old, c_old = _run(stream, base)
    out_new, c_new = _run(stream, NoiseConfig())
    for k in c_old.counts:
        if k not in ("dirty_district", "dirty_utm"):
            assert c_new.counts[k] == c_old.counts[k]
    assert c_old.counts["dirty_text"] > 0
    # events not hit by the new kinds are byte-identical to the run without them
    same = sum(1 for (_, a), (_, b) in zip(out_old, out_new) if a == b)
    assert len(out_old) == len(out_new)
    assert same == len(out_old) - c_new.counts["dirty_district"] - c_new.counts["dirty_utm"]
