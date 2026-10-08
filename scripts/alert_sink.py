"""Local alert sink for the Grafana webhook contact point (spec 17.19).

Grafana (in Docker) POSTs alerts to http://host.docker.internal:9099/alert. This server prints one readable line per
alert and appends the raw JSON body to logs/alerts.log. Standard library only.

    python scripts/alert_sink.py [--port 9099] [--log logs/alerts.log]
"""
import argparse
import json
import sys
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DEFAULT_LOG = Path(__file__).resolve().parent.parent / "logs" / "alerts.log"
MAX_BODY = 1024 * 1024        # larger requests are rejected with 400
MAX_LOG_BYTES = 5 * 1024 * 1024  # above this alerts.log is moved to alerts.log.1 (replacing the old one)


def format_alerts(payload, now=None):
    """One readable line per alert: time, status, rule name, summary."""
    now = now or datetime.now()
    lines = []
    for alert in payload.get("alerts") or []:
        labels = alert.get("labels") or {}
        annotations = alert.get("annotations") or {}
        name = labels.get("alertname") or payload.get("title") or "unknown"
        status = alert.get("status") or payload.get("status") or "unknown"
        message_lines = (payload.get("message") or "").strip().splitlines()
        summary = annotations.get("summary") or (message_lines[0] if message_lines else "")
        lines.append(f"{now:%Y-%m-%d %H:%M:%S} {status.upper():8} {name} - {summary}")
    return lines


def make_server(port=9099, log_path=DEFAULT_LOG, out=None, host="0.0.0.0"):
    log_path = Path(log_path)
    out = out or sys.stdout
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def _reply(self, code, body=b"ok"):
            self.send_response(code)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/":
                self._reply(200)
            else:
                self._reply(404, b"not found")

        def do_POST(self):
            if self.path != "/alert":
                return self._reply(404, b"not found")
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                return self._reply(400, b"bad content-length")
            if length < 0 or length > MAX_BODY:
                return self._reply(400, b"bad content-length")
            raw = self.rfile.read(length)
            try:
                payload = json.loads(raw or b"{}")
                if not isinstance(payload, dict):
                    raise ValueError("payload must be a JSON object")
            except ValueError:
                return self._reply(400, b"bad json")
            with lock:
                log_path.parent.mkdir(parents=True, exist_ok=True)
                if log_path.exists() and log_path.stat().st_size > MAX_LOG_BYTES:
                    log_path.replace(log_path.with_name(log_path.name + ".1"))
                with log_path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(payload, separators=(",", ":")) + "\n")
                for line in format_alerts(payload):
                    print(line, file=out, flush=True)
            self._reply(200)

        def log_message(self, *args):
            pass

    return ThreadingHTTPServer((host, port), Handler)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--port", type=int, default=9099)
    ap.add_argument("--log", default=str(DEFAULT_LOG))
    args = ap.parse_args(argv)
    server = make_server(args.port, args.log)
    print(f"alert sink listening on :{args.port}, appending to {args.log} (Ctrl+C to stop)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
