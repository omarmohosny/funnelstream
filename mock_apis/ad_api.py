# Run this module as: .venv\Scripts\python -m mock_apis.ad_api (from the repo root)
# Direct script invocation fails because it imports simulator.
import json
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from simulator.campaigns import CAMPAIGNS, campaign_numbers

SEED = 42


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        if url.path != "/campaigns":
            self.send_response(404)
            self.end_headers()
            return
        q = parse_qs(url.query)
        report_date = date.fromisoformat(q["report_date"][0])
        as_of = datetime.fromisoformat(q["as_of"][0]).replace(tzinfo=timezone.utc) if "as_of" in q else datetime.now(timezone.utc)
        body = [campaign_numbers(c["campaign_id"], report_date, as_of, SEED) for c in CAMPAIGNS]
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
