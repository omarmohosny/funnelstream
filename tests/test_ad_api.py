from datetime import date, datetime, timezone
from simulator.campaigns import campaign_numbers, CAMPAIGNS
import json
from http.server import HTTPServer
from mock_apis.ad_api import Handler
import urllib.request
import threading
import time


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
    # Find a free port
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address

    # Start server in daemon thread
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        # Give server time to start
        time.sleep(0.1)

        # Make request to the server
        url = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-04T12:00:00"
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read())

        # Verify response
        assert len(data) == 3, f"Expected 3 campaigns, got {len(data)}"
        for campaign in data:
            assert campaign["currency"] == "EGP", f"Expected EGP, got {campaign['currency']}"
            assert campaign["pulled_at"] == "2026-10-04T12:00:00+00:00"
    finally:
        server.shutdown()


def test_http_server_cumulative_increases():
    """Verify that as_of=2026-10-04T18:00:00 returns spend >= as_of=2026-10-04T12:00:00 for all campaigns."""
    # Find a free port
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address

    # Start server in daemon thread
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        # Give server time to start
        time.sleep(0.1)

        # Make request at 12:00
        url_12 = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-04T12:00:00"
        with urllib.request.urlopen(url_12) as response:
            data_12 = json.loads(response.read())

        # Make request at 18:00
        url_18 = f"http://{host}:{port}/campaigns?report_date=2026-10-04&as_of=2026-10-04T18:00:00"
        with urllib.request.urlopen(url_18) as response:
            data_18 = json.loads(response.read())

        # Verify cumulative property
        assert len(data_12) == len(data_18) == 3
        for i in range(3):
            campaign_12 = data_12[i]
            campaign_18 = data_18[i]
            assert campaign_18["spend"] >= campaign_12["spend"], \
                f"Campaign {campaign_18['campaign_id']}: spend at 18:00 ({campaign_18['spend']}) < spend at 12:00 ({campaign_12['spend']})"
    finally:
        server.shutdown()
