import random
from datetime import timedelta

from simulator.catalog import weighted_district

SHOP_URL = "https://shop.example.eg/"


def tracking_url(path: str, campaign=None) -> str:
    """Landing/product page URL; campaign sessions carry the three UTM parameters (spec 18.4)."""
    url = SHOP_URL + path
    if campaign:
        url += "?utm_source=%s&utm_medium=%s&utm_campaign=%s" % (
            campaign["utm_source"], campaign["utm_medium"], campaign["utm_campaign"])
    return url


def _event(event_type, t, session_id, customer_id, device, channel, country, city, payload, rng=None, platform=None,
           district=None):
    if rng is None:
        rng = random.Random()
    event_id = f"evt_{rng.getrandbits(64):016x}"
    e = {
        "event_id": event_id,
        "event_type": event_type,
        "event_version": 1,
        "event_time": t.isoformat(),
        "session_id": session_id,
        "device": device,
        "channel": channel,
        "country": country,
        "city": city,
        "district": district,
        "payload": payload,
    }
    if platform is not None:
        e["platform"] = platform
    if customer_id is not None:
        e["customer_id"] = customer_id
    return e


def journey(rng, customer, catalog, campaign, start, hot=None):
    """Build one session of app events. Funnel drop-off happens at each step."""
    session_id = f"sess_{rng.getrandbits(64):016x}"
    customer_id = customer["customer_id"] if customer else None
    device = customer["device_primary"] if customer else rng.choice(["mobile", "desktop"])
    platform = ("app" if rng.random() < 0.7 else "web") if device == "mobile" else "web"
    city = customer["city"] if customer else "Cairo"
    district = customer.get("district") if customer else None  # anonymous: drawn after the journey
    country = "EG"
    channel = campaign["channel"] if campaign else rng.choice(["organic", "direct", "email"])
    t = start
    events = []

    # Filter to only active products
    active_products = [p for p in catalog if p["is_active"]]
    if not active_products:
        raise ValueError("No active products in catalog")

    def add(etype, payload, dt=5):
        nonlocal t
        t = t + timedelta(seconds=rng.randint(dt, dt * 4))
        events.append(_event(etype, t, session_id, customer_id, device, channel, country, city, payload, rng, platform, district))

    add("app_open", {})
    add("page_view", {"page": "home", "page_url": tracking_url("", campaign)})

    n_views = rng.randint(1, 6)
    viewed = rng.sample(active_products, n_views)
    if hot:  # inventory scenario: a hero product is the first thing the session looks at
        hero = rng.choice(hot)
        viewed = [hero] + [p for p in viewed if p is not hero][: n_views - 1]
    for i, product in enumerate(viewed):
        pv = {"product_id": product["product_id"], "price": product["price"], "category": product["category"],
              "page_url": tracking_url("p/" + product["product_id"], campaign if i == 0 else None)}
        add("product_view", pv)
        if rng.random() < 0.5:
            add("product_click", {"product_id": product["product_id"], "element": rng.choice(["image", "size", "color", "reviews"])})
        add("product_view_end", {"product_id": product["product_id"], "dwell_seconds": rng.randint(8, 240)})

    if rng.random() < (0.6 if hot else 0.35):
        item = viewed[0] if hot else rng.choice(viewed)
        cart_id = f"cart_{session_id[5:]}"
        add("add_to_cart", {"product_id": item["product_id"], "quantity": 1, "cart_id": cart_id})
        if rng.random() < 0.45:
            add("checkout_started", {"cart_id": cart_id})
            if rng.random() < 0.7:
                order_id = f"ord_{rng.getrandbits(64):016x}"
                add("purchase", {"order_id": order_id, "total": item["price"]})
    if district is None:  # anonymous session: Cairo district from a side rng, so the main stream is untouched
        district = weighted_district(random.Random(session_id), city)
        for e in events:
            e["district"] = district
    return events
