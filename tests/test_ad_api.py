from datetime import date, datetime, timezone, timedelta
from simulator.campaigns import campaign_numbers, CAMPAIGNS
import json
from http.server import HTTPServer
from mock_apis.ad_api import Handler
import urllib.request
import urllib.error
import threading


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


def test_http_server_returns_campaigns_with_currency():
    """Start HTTP server on free port, request /campaigns, verify 3 campaigns with EGP currency."""
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        url = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-04T12:00:00"
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read())

        assert len(data) == 3, f"Expected 3 campaigns, got {len(data)}"
        for campaign in data:
            assert campaign["currency"] == "EGP", f"Expected EGP, got {campaign['currency']}"
            assert campaign["pulled_at"] == "2026-10-04T12:00:00+00:00"
    finally:
        server.server_close()


def test_http_server_cumulative_increases():
    """Verify that as_of=2026-10-04T18:00:00 returns >= metrics as_of=2026-10-04T12:00:00 for all campaigns."""
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        url_12 = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-04T12:00:00"
        with urllib.request.urlopen(url_12) as response:
            data_12 = json.loads(response.read())

        url_18 = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-04T18:00:00"
        with urllib.request.urlopen(url_18) as response:
            data_18 = json.loads(response.read())

        assert len(data_12) == len(data_18) == 3
        for i in range(3):
            campaign_12 = data_12[i]
            campaign_18 = data_18[i]
            assert campaign_18["impressions"] >= campaign_12["impressions"], \
                f"Campaign {campaign_18['campaign_id']}: impressions at 18:00 < impressions at 12:00"
            assert campaign_18["clicks"] >= campaign_12["clicks"], \
                f"Campaign {campaign_18['campaign_id']}: clicks at 18:00 < clicks at 12:00"
            assert campaign_18["spend"] >= campaign_12["spend"], \
                f"Campaign {campaign_18['campaign_id']}: spend at 18:00 < spend at 12:00"
    finally:
        server.server_close()


def test_missing_report_date_returns_400():
    """Missing report_date parameter returns 400 with JSON error."""
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        url = f"http://{host}:{port}/campaigns?as_of=2026-10-04T12:00:00"
        try:
            urllib.request.urlopen(url)
            assert False, "Expected HTTP 400"
        except urllib.error.HTTPError as e:
            assert e.code == 400
            data = json.loads(e.read())
            assert "error" in data
    finally:
        server.server_close()


def test_malformed_report_date_returns_400():
    """Malformed report_date returns 400 with JSON error."""
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        url = f"http://{host}:{port}/campaigns?report_date=not-a-date&as_of=2026-10-04T12:00:00"
        try:
            urllib.request.urlopen(url)
            assert False, "Expected HTTP 400"
        except urllib.error.HTTPError as e:
            assert e.code == 400
            data = json.loads(e.read())
            assert "error" in data
    finally:
        server.server_close()


def test_malformed_as_of_returns_400():
    """Malformed as_of returns 400 with JSON error."""
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        url = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=not-a-datetime"
        try:
            urllib.request.urlopen(url)
            assert False, "Expected HTTP 400"
        except urllib.error.HTTPError as e:
            assert e.code == 400
            data = json.loads(e.read())
            assert "error" in data
    finally:
        server.server_close()


def test_timezone_offset_honored():
    """as_of=2026-10-04T14:00:00+02:00 returns same numbers as as_of=2026-10-04T12:00:00."""
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        # 14:00 +02:00 = 12:00 UTC
        url_with_offset = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-04T14:00:00%2B02:00"
        with urllib.request.urlopen(url_with_offset) as response:
            data_offset = json.loads(response.read())

        # 12:00 UTC
        url_utc = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-04T12:00:00"
        with urllib.request.urlopen(url_utc) as response:
            data_utc = json.loads(response.read())

        # Both should produce identical results
        assert len(data_offset) == len(data_utc) == 3
        for i in range(3):
            assert data_offset[i]["impressions"] == data_utc[i]["impressions"]
            assert data_offset[i]["clicks"] == data_utc[i]["clicks"]
            assert data_offset[i]["spend"] == data_utc[i]["spend"]
    finally:
        server.server_close()


def test_as_of_before_day_starts():
    """as_of before day starts (2026-10-03T23:00:00) returns 0 impressions, clicks, spend."""
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        url = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-03T23:00:00"
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read())

        assert len(data) == 3
        for campaign in data:
            assert campaign["impressions"] == 0, f"Expected 0 impressions before day starts"
            assert campaign["clicks"] == 0, f"Expected 0 clicks before day starts"
            assert campaign["spend"] == 0.0, f"Expected 0 spend before day starts"
    finally:
        server.server_close()


def test_as_of_after_day_ends():
    """as_of after day ends (2026-10-06T00:00:00) equals full-day value."""
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        # After day ends
        url_after = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-06T00:00:00"
        with urllib.request.urlopen(url_after) as response:
            data_after = json.loads(response.read())

        # Full day (end of day)
        url_full = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-05T00:00:00"
        with urllib.request.urlopen(url_full) as response:
            data_full = json.loads(response.read())

        assert len(data_after) == len(data_full) == 3
        for i in range(3):
            assert data_after[i]["impressions"] == data_full[i]["impressions"]
            assert data_after[i]["clicks"] == data_full[i]["clicks"]
            assert data_after[i]["spend"] == data_full[i]["spend"]
    finally:
        server.server_close()
