from datetime import timedelta


def _event(event_type, t, session_id, customer_id, device, channel, country, city, payload, rng=None):
    event_id = f"evt_{rng.getrandbits(48):012x}" if rng else "evt_placeholder"
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
        "payload": payload,
    }
    if customer_id is not None:
        e["customer_id"] = customer_id
    return e


def journey(rng, customer, catalog, campaign, start):
    """Build one session of app events. Funnel drop-off happens at each step."""
    session_id = f"sess_{rng.getrandbits(40):010x}"
    customer_id = customer["customer_id"] if customer else None
    device = customer["device_primary"] if customer else rng.choice(["mobile", "desktop"])
    city = customer["city"] if customer else "Cairo"
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
        events.append(_event(etype, t, session_id, customer_id, device, channel, country, city, payload, rng))

    add("app_open", {})
    first_page = {"page": "home"}
    if campaign:
        first_page.update({"utm_campaign": campaign["utm_campaign"], "utm_source": campaign["utm_source"]})
    add("page_view", first_page)

    n_views = rng.randint(1, 6)
    viewed = rng.sample(active_products, n_views)
    for i, product in enumerate(viewed):
        pv = {"product_id": product["product_id"], "price": product["price"], "category": product["category"]}
        if i == 0 and campaign:
            pv.update({"utm_campaign": campaign["utm_campaign"], "utm_source": campaign["utm_source"]})
        add("product_view", pv)
        if rng.random() < 0.5:
            add("product_click", {"product_id": product["product_id"], "element": rng.choice(["image", "size", "color", "reviews"])})
        add("product_view_end", {"product_id": product["product_id"], "dwell_seconds": rng.randint(8, 240)})

    if rng.random() < 0.35:
        item = rng.choice(viewed)
        cart_id = f"cart_{session_id[5:]}"
        add("add_to_cart", {"product_id": item["product_id"], "quantity": 1, "cart_id": cart_id})
        if rng.random() < 0.45:
            add("checkout_started", {"cart_id": cart_id})
            if rng.random() < 0.7:
                order_id = f"ord_{rng.getrandbits(32):08x}"
                add("purchase", {"order_id": order_id, "total": item["price"]})
    return events
