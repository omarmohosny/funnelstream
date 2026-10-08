"""G2: scripts/alert_sink.py against a real server on an ephemeral port."""
import io
import json
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from scripts import alert_sink

PAYLOAD = {
    "status": "firing",
    "alerts": [
        {"status": "firing", "labels": {"alertname": "No events for 2 minutes", "severity": "critical"},
         "annotations": {"summary": "No Silver events arrived in the last 2 minutes"}},
        {"status": "resolved", "labels": {"alertname": "Late-event rate above 5% (5 min)"},
         "annotations": {"summary": "late rate ok"}},
    ],
}


@pytest.fixture
def server(tmp_path):
    out = io.StringIO()
    log = tmp_path / "sub" / "alerts.log"
    srv = alert_sink.make_server(0, log, out, host="127.0.0.1")
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}", out, log
    srv.shutdown()
    srv.server_close()


def post(url, body, raw=False):
    data = body if raw else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    return urllib.request.urlopen(req, timeout=5)


def test_get_root_ok(server):
    url, _, _ = server
    with urllib.request.urlopen(url + "/", timeout=5) as r:
        assert r.status == 200 and r.read() == b"ok"


def test_post_prints_one_line_per_alert_and_logs_raw_json(server):
    url, out, log = server
    assert post(url + "/alert", PAYLOAD).status == 200
    post(url + "/alert", PAYLOAD)
    lines = out.getvalue().strip().splitlines()
    assert len(lines) == 4
    assert "FIRING" in lines[0] and "No events for 2 minutes" in lines[0] and "No Silver events arrived" in lines[0]
    assert "RESOLVED" in lines[1] and "Late-event rate above 5% (5 min)" in lines[1]
    logged = [json.loads(x) for x in log.read_text(encoding="utf-8").splitlines()]
    assert logged == [PAYLOAD, PAYLOAD]


def test_bad_json_and_unknown_paths(server):
    url, out, log = server
    with pytest.raises(urllib.error.HTTPError) as e:
        post(url + "/alert", b"not json", raw=True)
    assert e.value.code == 400
    with pytest.raises(urllib.error.HTTPError) as e:
        post(url + "/other", {})
    assert e.value.code == 404
    assert out.getvalue() == "" and not log.exists()


def test_format_alerts_falls_back_to_message():
    lines = alert_sink.format_alerts({"title": "T", "status": "firing", "message": "hello\nmore",
                                      "alerts": [{"labels": {}, "annotations": {}}]})
    assert "FIRING" in lines[0] and lines[0].endswith("T - hello")


def test_help_runs_as_plain_file_from_other_cwd(tmp_path):
    script = Path(__file__).resolve().parent.parent / "scripts" / "alert_sink.py"
    r = subprocess.run([sys.executable, str(script), "--help"], cwd=tmp_path, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and "usage" in r.stdout.lower()


def raw_post(url, headers):
    """POST with hand-written headers and no body (what a hostile Content-Length looks like)."""
    import http.client

    host, port = url.replace("http://", "").split(":")
    c = http.client.HTTPConnection(host, int(port), timeout=5)
    c.putrequest("POST", "/alert", skip_accept_encoding=True)
    for k, v in headers.items():
        c.putheader(k, v)
    c.endheaders()
    try:
        return c.getresponse().status
    finally:
        c.close()


@pytest.mark.parametrize("length", ["-5", "abc", "1.5", str(alert_sink.MAX_BODY + 1)])
def test_bad_or_oversized_content_length_is_rejected_with_400(server, length):
    url, out, log = server
    assert raw_post(url, {"Content-Length": length}) == 400
    assert out.getvalue() == "" and not log.exists()


def test_log_is_rotated_above_the_size_cap(server, monkeypatch):
    url, out, log = server
    monkeypatch.setattr(alert_sink, "MAX_LOG_BYTES", 200)
    for _ in range(4):
        post(url + "/alert", PAYLOAD)
    rotated = log.with_name("alerts.log.1")
    assert rotated.exists() and log.stat().st_size <= 200 + len(json.dumps(PAYLOAD, separators=(",", ":"))) + 1
    assert len(out.getvalue().strip().splitlines()) == 8  # every alert is still printed
