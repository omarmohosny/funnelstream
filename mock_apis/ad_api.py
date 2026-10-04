# Run this module as: .venv\Scripts\python -m mock_apis.ad_api (from the repo root)
# Direct script invocation fails because it imports simulator.
import argparse
import copy
import json
import random
import threading
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from simulator.campaigns import CAMPAIGNS, campaign_numbers

SEED = 42

# optional noise (off by default); rates are per request
P_503 = 0.05
P_DUP_ROW = 0.05
P_CLICKS_GT_IMPRESSIONS = 0.03
P_NEGATIVE_SPEND = 0.02


def apply_api_noise(rows, rng):
    """Return a noised copy of the campaign rows (input untouched): sometimes a duplicated row,
    a row with clicks > impressions, a row with negative spend. Kept apart from campaign_numbers."""
    rows = copy.deepcopy(rows)
    if rows and rng.random() < P_DUP_ROW:
        rows.append(copy.deepcopy(rows[rng.randrange(len(rows))]))
    if rows and rng.random() < P_CLICKS_GT_IMPRESSIONS:
        r = rows[rng.randrange(len(rows))]
        r["clicks"] = r["impressions"] + rng.randint(1, 100)
    if rows and rng.random() < P_NEGATIVE_SPEND:
        r = rows[rng.randrange(len(rows))]
        r["spend"] = -abs(r["spend"]) - 1.0
    return rows


class Handler(BaseHTTPRequestHandler):
    noise = False
    noise_seed = 0
    _lock = threading.Lock()
    _requests = 0  # per-server counter of valid /campaigns requests (subclass gets its own)

    def _next_noise_rng(self):
        cls = type(self)
        with cls._lock:
            n = cls._requests
            cls._requests += 1
        return random.Random(f"{cls.noise_seed}-{n}")

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
            if self.noise:
                nrng = self._next_noise_rng()
                if nrng.random() < P_503:
                    err = json.dumps({"error": "service unavailable"}).encode()
                    self.send_response(503)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(err)))
                    self.end_headers()
                    self.wfile.write(err)
                    return
                body = apply_api_noise(body, nrng)
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


def make_handler(noise: bool = False, seed: int = 0):
    """Handler subclass with its own request counter, so noise is deterministic per server."""
    return type("NoisyHandler", (Handler,), {"noise": noise, "noise_seed": seed,
                                             "_lock": threading.Lock(), "_requests": 0})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--noise", action="store_true", help="inject 503s, duplicate rows, bad numbers")
    ap.add_argument("--noise-seed", type=int, default=0)
    ap.add_argument("--port", type=int, default=8000)
    a = ap.parse_args()
    HTTPServer(("0.0.0.0", a.port), make_handler(a.noise, a.noise_seed)).serve_forever()
