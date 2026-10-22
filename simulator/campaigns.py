import random
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

CAIRO = ZoneInfo("Africa/Cairo")

CAMPAIGNS = [
    {"campaign_id": "camp_201", "utm_campaign": "eid_sale_1", "utm_source": "google", "utm_medium": "cpc", "platform": "google", "channel": "paid_search", "daily_budget": 1500.0},
    {"campaign_id": "camp_202", "utm_campaign": "ramadan_offer_1", "utm_source": "meta", "utm_medium": "paid_social", "platform": "meta", "channel": "paid_social", "daily_budget": 2000.0},
    {"campaign_id": "camp_203", "utm_campaign": "retarget_cart_1", "utm_source": "meta", "utm_medium": "paid_social", "platform": "meta", "channel": "paid_social", "daily_budget": 600.0},
]


def campaign_numbers(campaign_id: str, report_date: date, as_of: datetime, seed: int) -> dict:
    camp = next(c for c in CAMPAIGNS if c["campaign_id"] == campaign_id)
    # the report day is the Cairo calendar day; its length is 23 or 25 hours on a daylight saving change
    day_start = datetime(report_date.year, report_date.month, report_date.day, tzinfo=CAIRO)
    nxt = report_date + timedelta(days=1)
    day_len = (datetime(nxt.year, nxt.month, nxt.day, tzinfo=CAIRO).astimezone(ZoneInfo("UTC"))
               - day_start.astimezone(ZoneInfo("UTC"))).total_seconds()
    frac = max(0.0, min(1.0, (as_of - day_start).total_seconds() / day_len))
    rng = random.Random(f"{seed}-{campaign_id}-{report_date.isoformat()}")
    cpm = rng.uniform(18, 35)
    ctr = rng.uniform(0.01, 0.04)
    full_spend = camp["daily_budget"] * rng.uniform(0.85, 1.05)
    conv_rate = rng.uniform(0.02, 0.06)  # drawn last so earlier values keep their seeds
    impressions = int(full_spend / cpm * 1000 * frac)
    clicks = int(impressions * ctr)
    return {
        "campaign_id": campaign_id,
        "report_date": report_date.isoformat(),
        "pulled_at": as_of.isoformat(),
        "impressions": impressions,
        "clicks": clicks,
        "spend": round(full_spend * frac, 4),
        "platform_conversions": int(clicks * conv_rate),
        "currency": "EGP",
    }
