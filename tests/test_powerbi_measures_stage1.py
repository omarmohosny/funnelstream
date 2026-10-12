"""Stage 1 of the Power BI redesign: measures that show (Blank) or a meaningless number in the report."""
import re
from pathlib import Path

DAX = (Path(__file__).resolve().parent.parent / "dashboards" / "powerbi" / "measures.dax").read_text(encoding="utf-8")


def body(name):
    m = re.search(r"^%s =\n?(.*?)(?=\n\n|\Z)" % re.escape(name), DAX, re.S | re.M)
    assert m, name
    return m.group(1)


def test_stock_counts_are_zero_not_blank_when_nothing_matches():
    for name in ("Out of Stock Lines", "Low Stock Lines"):
        assert "COALESCE" in body(name) and re.search(r",\s*0\s*\)", body(name)), name
    assert "COALESCE" in body("Out of Stock Rate")


def test_the_freshness_and_lapsed_measures_are_gone_and_quiet_buyers_replaces_lapsed():
    assert "Stock Freshness" not in DAX and "Lapsed Buyers" not in DAX
    assert ">= 7" in body("Quiet Buyers (7+ days)") and "COALESCE" in body("Quiet Buyers (7+ days)")


def test_prior_week_change_compares_two_explicit_seven_day_windows():
    text = body("Revenue vs Prior 7 Days")
    assert text.count("DATESBETWEEN") == 2 and "DIVIDE" in text and "DATEADD" not in text


def test_delivery_hours_are_blank_without_delivered_orders():
    assert "is_delivered" in body("Average Delivery Hours") and "BLANK" in body("Average Delivery Hours")


def test_days_of_cover_states_its_cap_in_the_name_and_uses_a_plain_format():
    from scripts import make_pbip
    m = next(m for m in make_pbip.parse_measures() if m["name"] == "Days of Cover (max 90)")
    tmdl = make_pbip.measure_tmdl(m)
    assert 'formatString: "#,0.0"' in tmdl and "[>=" not in tmdl  # conditional format sections are not relied on


def test_days_of_cover_is_capped_and_never_blank_or_negative():
    text = body("Days of Cover (max 90)")
    assert "MIN ( DIVIDE" in text and "MAX ( 0," in text
    assert re.search(r"IF \( \[Closing Stock\] > 0, 90, 0 \)", text)  # nothing sold: 90 with stock, 0 without, never blank


def test_maturity_label_is_one_short_word_that_fits_a_card():
    text = body("Maturity Label")
    assert '"Settling"' in text and '"Settled"' in text  # "Provisional" was cut to "Provisio..." in a 172 px card


def test_delivery_hours_ignore_delivered_orders_without_a_delivery_time():
    # delivered orders without a carrier have delivery_lead_hours = 0; they made a (Blank) carrier bar at 0.0 hours
    assert re.search(r"delivery_lead_hours\]\s*>\s*0", body("Average Delivery Hours"))
