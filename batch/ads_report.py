"""Yesterday's ad report: pull once per day, store it, serve the stored copy on any rerun.

Rule: report_date is yesterday on the Cairo calendar (the ad API's report day). The pull is final, taken as of 00:00 Cairo today,
so every number is the full-day total and a rerun returns the same figures. If the store already holds a
snapshot for every campaign on that date, nothing is pulled and the stored report is returned
(status "cached"). Otherwise the API is pulled, validated, stored (status "pulled").

Writes go through get_store("databricks") into silver_campaign_snapshots; rows the validator rejects go to
silver_quarantine (NEGATIVE_SPEND, CLICKS_GT_IMPRESSIONS) and never into the snapshots.
Run by hand:  python -m batch.ads_report [--report-date YYYY-MM-DD] [--api-url URL] [--force]
"""
import argparse
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from simulator.campaigns import CAMPAIGNS

SNAPSHOT_KEYS = ["campaign_id", "report_date", "pulled_at"]
COLUMNS = ["campaign_id", "report_date", "pulled_at", "utm_campaign", "ad_platform", "impressions", "clicks",
           "platform_conversions", "spend", "currency"]
RETRY_STATUS = {503}
_BY_ID = {c["campaign_id"]: c for c in CAMPAIGNS}
CAIRO = ZoneInfo("Africa/Cairo")


def yesterday(now: datetime = None) -> date:
    now = now or datetime.now(timezone.utc)
    return now.astimezone(CAIRO).date() - timedelta(days=1)


def final_as_of(report_date: date) -> datetime:
    """The moment the day is complete: 00:00 Cairo of the next day. Fixed per date, so pulled_at never changes."""
    nxt = report_date + timedelta(days=1)
    return datetime(nxt.year, nxt.month, nxt.day, tzinfo=CAIRO)


def pull(api_url: str, report_date: date, as_of: datetime, tries: int = 3, backoff: float = 1.0, sleep=time.sleep):
    """GET /campaigns, retrying HTTP 503 with exponential backoff; other errors raise at once."""
    url = "%s/campaigns?report_date=%s&as_of=%s" % (api_url.rstrip("/"), report_date.isoformat(),
                                                    as_of.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code not in RETRY_STATUS or attempt == tries - 1:
                raise
            sleep(backoff * 2 ** attempt)


def validate(rows):
    """Split API rows into (good snapshot rows, rejected (row, error_code)); duplicates collapse on the key."""
    good, bad, seen = [], [], set()
    for r in rows:
        spend = Decimal(str(r["spend"]))
        if spend < 0:
            bad.append((r, "NEGATIVE_SPEND"))
        elif r["clicks"] > r["impressions"]:
            bad.append((r, "CLICKS_GT_IMPRESSIONS"))
        else:
            key = (r["campaign_id"], r["report_date"], r["pulled_at"])
            if key in seen:
                continue
            seen.add(key)
            camp = _BY_ID.get(r["campaign_id"], {})
            good.append({"campaign_id": r["campaign_id"], "report_date": date.fromisoformat(r["report_date"]),
                         "pulled_at": datetime.fromisoformat(r["pulled_at"]),
                         "utm_campaign": camp.get("utm_campaign"), "ad_platform": camp.get("platform"),
                         "impressions": r["impressions"], "clicks": r["clicks"],
                         "platform_conversions": r.get("platform_conversions"), "spend": spend,
                         "currency": r.get("currency", "EGP")})
    return good, bad


def _quarantine_rows(bad, received_at):
    out = []
    for r, code in bad:
        qid = hashlib.sha1(("ads|%s|%s|%s" % (r["campaign_id"], r["report_date"], code)).encode()).hexdigest()
        out.append({"quarantine_id": qid, "source_topic": "ads_api", "error_code": code,
                    "error_detail": "campaign %s report_date %s" % (r["campaign_id"], r["report_date"]),
                    "received_at": received_at})
    return out


def stored_report(store, report_date: date):
    """The stored snapshot per campaign for the date (latest pulled_at wins), as dicts."""
    sql = ("SELECT %s FROM %s WHERE report_date = DATE'%s' ORDER BY campaign_id, pulled_at"
           % (", ".join(COLUMNS), store.qualified("silver_campaign_snapshots"), report_date.isoformat()))
    latest = {}
    for row in store.query(sql):
        d = dict(zip(COLUMNS, row))
        latest[d["campaign_id"]] = d  # ordered by pulled_at, so the last one is the latest
    return [latest[k] for k in sorted(latest)]


def run(store, api_url: str, now: datetime = None, report_date: date = None, force: bool = False,
        pull_fn=pull, report_dir=None):
    """Return {"report_date", "status": "cached"|"pulled", "rows", "rejected", "path"}."""
    now = now or datetime.now(timezone.utc)
    report_date = report_date or yesterday(now)
    if report_date >= now.astimezone(CAIRO).date():
        raise ValueError("report_date %s is not a finished day" % report_date)
    rows, rejected, status = stored_report(store, report_date), 0, "cached"
    expected = {c["campaign_id"] for c in CAMPAIGNS}
    if force or {r["campaign_id"] for r in rows} != expected:
        raw = pull_fn(api_url, report_date, final_as_of(report_date))
        good, bad = validate(raw)
        store.upsert("silver_campaign_snapshots", good, SNAPSHOT_KEYS)
        if bad:
            store.upsert("silver_quarantine", _quarantine_rows(bad, now), ["quarantine_id"])
        rows, rejected, status = stored_report(store, report_date), len(bad), "pulled"
    return {"report_date": report_date, "status": status, "rows": rows, "rejected": rejected,
            "path": write_report(rows, report_date, report_dir)}


def write_report(rows, report_date, report_dir=None):
    folder = Path(report_dir or os.environ.get("ADS_REPORT_DIR") or "data/reports")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / ("ads_report_%s.json" % report_date.isoformat())
    path.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
    return str(path)


def format_report(result) -> str:
    lines = ["Ad report for %s (%s, %d rejected)" % (result["report_date"], result["status"], result["rejected"]),
             "%-10s %12s %8s %12s %10s %8s" % ("campaign", "impressions", "clicks", "conversions", "spend", "cur")]
    for r in result["rows"]:
        lines.append("%-10s %12s %8s %12s %10s %8s" % (r["campaign_id"], r["impressions"], r["clicks"],
                                                       r["platform_conversions"], r["spend"], r["currency"]))
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--report-date", type=date.fromisoformat, default=None, help="default: yesterday (Cairo)")
    ap.add_argument("--api-url", default=os.environ.get("AD_API_URL", "http://localhost:8000"))
    ap.add_argument("--force", action="store_true", help="pull again even if the report is stored")
    a = ap.parse_args(argv)
    from warehouse.store import get_store
    with get_store("databricks") as store:
        result = run(store, a.api_url, report_date=a.report_date, force=a.force)
    print(format_report(result))
    print("file:", result["path"])
    return result


if __name__ == "__main__":
    main()
