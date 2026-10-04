from decimal import Decimal

import pytest

from contracts.normalize import normalize_city, normalize_device, to_decimal, to_utc_iso


@pytest.mark.parametrize("raw,exp", [
    ("Cairo", "Cairo"), ("cairo ", "Cairo"), ("  CAIRO", "Cairo"), ("alex", "Alexandria"),
    ("ALEX", "Alexandria"), ("alexandria", "Alexandria"), ("el-giza", "Giza"), ("GIZA", "Giza"),
    ("Mansoura", "Mansoura"), ("tanta", "Tanta"), ("Assiut", "Assiut"), ("other", "Other"),
    ("Al  Exandria".replace("Al  Ex", "Alex"), "Alexandria"), ("el  giza", "Giza"),
    ("Atlantis", "unknown"), ("", "unknown"), (None, "unknown"), ("   ", "unknown"), (5, "unknown"),
])
def test_city(raw, exp):
    assert normalize_city(raw) == exp


@pytest.mark.parametrize("raw,exp", [
    ("mobile", "mobile"), ("Mobile", "mobile"), (" MOBILE ", "mobile"), ("phone", "mobile"),
    ("desktop", "desktop"), ("pc", "desktop"), ("PC", "desktop"), ("tablet", "tablet"),
    ("Tablet ", "tablet"), ("toaster", "unknown"), (None, "unknown"), ("", "unknown"),
])
def test_device(raw, exp):
    assert normalize_device(raw) == exp


def test_to_utc_iso():
    assert to_utc_iso("2026-10-04T10:00:00Z") == "2026-10-04T10:00:00+00:00"
    assert to_utc_iso("2026-10-04T12:00:00+02:00") == "2026-10-04T10:00:00+00:00"
    assert to_utc_iso("2026-10-04T10:00:00") == "2026-10-04T10:00:00+00:00"
    for bad in ("garbage", None, 5, ""):
        with pytest.raises(ValueError):
            to_utc_iso(bad)


def test_to_decimal():
    assert to_decimal(5) == Decimal("5")
    assert to_decimal(79.9) == Decimal("79.9")
    assert to_decimal("79.9") == Decimal("79.9")
    for bad in ("abc", None, True, False, [1], "nan", float("inf")):
        with pytest.raises(ValueError):
            to_decimal(bad)
