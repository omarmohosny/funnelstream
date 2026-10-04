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
            error_body = json.dumps({"error": "Not Found"}).encode()
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(error_body)))
            self.end_headers()
            self.wfile.write(error_body)
            return

        try:
            q = parse_qs(url.query)
            if "report_date" not in q:
                raise KeyError("report_date")

            report_date = date.fromisoformat(q["report_date"][0])

            if "as_of" in q:
                as_of_str = q["as_of"][0]
                # Handle trailing Z by replacing with +00:00 for compatibility
                as_of_str = as_of_str.replace("Z", "+00:00")
                as_of = datetime.fromisoformat(as_of_str)
                # Handle timezone offset: if aware, convert to UTC; if naive, assume UTC
                if as_of.tzinfo is not None:
                    as_of = as_of.astimezone(timezone.utc)
                else:
                    as_of = as_of.replace(tzinfo=timezone.utc)
            else:
                as_of = datetime.now(timezone.utc)

            body = [campaign_numbers(c["campaign_id"], report_date, as_of, SEED) for c in CAMPAIGNS]
            payload = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (KeyError, ValueError) as e:
            error_msg = f"Invalid request: {str(e)}"
            error_body = json.dumps({"error": error_msg}).encode()
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(error_body)))
            self.end_headers()
            self.wfile.write(error_body)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
