from datetime import timedelta


def _ev(rng, etype, t, order_id, customer, payload):
    return {
        "event_id": f"evt_{rng.getrandbits(64):016x}",
        "event_type": etype,
        "event_version": 1,
        "event_time": t.isoformat(),
        "session_id": None,
        "order_id": order_id,
        "customer_id": customer["customer_id"],
        "country": customer.get("country", "EG"),
        "city": customer["city"],
        "district": customer.get("district"),
        "payload": payload,
    }


def _basket(rng, catalog, first_item):
    """(lines, total, payment, carrier) of one order; the draw order is fixed (the seeded streams depend on it)."""
    active = [p for p in catalog if p["is_active"]]
    if first_item is not None:
        rest = [p for p in active if p is not first_item]
        items = [first_item] + rng.sample(rest, rng.randint(0, 2))
    else:
        items = rng.sample(active, rng.randint(1, 3))
    lines = []
    for p in items:
        q = rng.randint(1, 2)
        lines.append({"product_id": p["product_id"], "quantity": q, "unit_price": p["price"],
                      "line_total": round(p["price"] * q, 2)})
    total = round(sum(l["line_total"] for l in lines), 2)
    payment = rng.choices(["cod", "fawry", "vodafone_cash", "instapay", "card"],
                          weights=[55, 12, 10, 8, 15])[0]
    carrier = rng.choice(["bosta", "aramex", "jt_express"])
    return lines, total, payment, carrier


def make_order(rng, customer, catalog, campaign, t, order_id, first_item=None):
    """Return a list of (topic, event) tuples for one order's lifecycle."""
    lines, total, payment, carrier = _basket(rng, catalog, first_item)
    created = {"order_id": order_id, "payment_method": payment, "items": lines, "order_total": total,
               "channel": campaign["channel"] if campaign else "organic",
               "utm_source": campaign["utm_source"] if campaign else None,
               "utm_medium": campaign["utm_medium"] if campaign else None,
               "utm_campaign": campaign["utm_campaign"] if campaign else None}
    out = [("orders", _ev(rng, "order_created", t, order_id, customer, created))]

    if rng.random() < 0.05:  # cancel before shipment
        t2 = t + timedelta(minutes=rng.randint(5, 180))
        out.append(("orders", _ev(rng, "order_cancelled", t2, order_id, customer, {"reason": "customer_request"})))
        return out

    def add(topic, etype, payload):
        out.append((topic, _ev(rng, etype, t, order_id, customer, payload)))

    t += timedelta(minutes=rng.randint(10, 60))
    if payment != "cod":  # cash on delivery skips payment_authorised (spec 17.7)
        add("orders", "payment_authorised", {"payment_method": payment})
    t += timedelta(hours=rng.randint(2, 12))
    add("orders", "order_packed", {})
    t += timedelta(hours=rng.randint(2, 10))
    add("orders", "order_shipped", {"carrier": carrier})

    attempts = 1 if rng.random() < 0.8 else rng.randint(2, 3)
    for a in range(1, attempts + 1):
        t += timedelta(hours=rng.randint(4, 24))
        add("delivery.events", "out_for_delivery", {"carrier": carrier, "attempt": a})
        if a < attempts:
            t += timedelta(hours=1)
            add("delivery.events", "delivery_failed", {"reason": "no_answer", "attempt": a})
            continue
        if rng.random() < 0.9:
            t += timedelta(hours=rng.randint(1, 6))
            add("delivery.events", "delivered", {"carrier": carrier})
            t += timedelta(hours=1)
            add("orders", "order_completed", {})
        else:
            t += timedelta(hours=1)
            add("delivery.events", "delivery_failed", {"reason": "refused", "attempt": a})
            t += timedelta(hours=rng.randint(2, 6))
            add("orders", "order_returned", {"reason": "all_attempts_failed", "attempts": attempts})
    return out


def make_express_order(rng, customer, catalog, campaign, t, order_id, first_item=None, promised_minutes=20,
                       late_minutes=None):
    """One express order (the live-inventory service) as a list of (topic, event) tuples, in minutes.

    Normal case after order_created: payment authorised after 10-40 s (not cash on delivery), packed after 2-4
    minutes, shipped about a minute later, out for delivery 30-60 s after that, delivered after 8 to 17.5 minutes in
    total and never later than promised_minutes - 0.5, completed 30 s after delivery. No cancellation, no failed
    attempt, no return. The timeline is built for a 20 minute promise and scaled by promised_minutes / 20 (a rehearsal
    with a faster clock). late_minutes (a number of minutes) makes it a late order: delivered after that time.
    """
    lines, total, payment, carrier = _basket(rng, catalog, first_item)
    scale = promised_minutes / 20.0
    created = {"order_id": order_id, "payment_method": payment, "items": lines, "order_total": total,
               "channel": campaign["channel"] if campaign else "organic",
               "utm_source": campaign["utm_source"] if campaign else None,
               "utm_medium": campaign["utm_medium"] if campaign else None,
               "utm_campaign": campaign["utm_campaign"] if campaign else None,
               "service": "express", "promised_minutes": promised_minutes}
    out = [("orders", _ev(rng, "order_created", t, order_id, customer, created))]

    def add(topic, etype, minutes, payload):
        out.append((topic, _ev(rng, etype, t + timedelta(minutes=minutes), order_id, customer, payload)))

    if payment != "cod":  # cash on delivery skips payment_authorised
        add("orders", "payment_authorised", rng.uniform(10, 40) / 60 * scale, {"payment_method": payment})
    packed = rng.uniform(2, 4) * scale
    shipped = packed + rng.uniform(0.9, 1.1) * scale
    out_for = shipped + rng.uniform(30, 60) / 60 * scale
    if late_minutes is not None:
        delivered = max(float(late_minutes), out_for)
    else:
        delivered = min(rng.uniform(8, 17.5) * scale, promised_minutes - 0.5)
    add("orders", "order_packed", packed, {})
    add("orders", "order_shipped", shipped, {"carrier": carrier})
    add("delivery.events", "out_for_delivery", out_for, {"carrier": carrier, "attempt": 1})
    add("delivery.events", "delivered", delivered, {"carrier": carrier})
    add("orders", "order_completed", delivered + 0.5 * scale, {})
    return out
