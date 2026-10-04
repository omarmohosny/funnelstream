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
        "payload": payload,
    }


def make_order(rng, customer, catalog, campaign, t, order_id):
    """Return a list of (topic, event) tuples for one order's lifecycle."""
    active = [p for p in catalog if p["is_active"]]
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
    created = {"order_id": order_id, "payment_method": payment, "items": lines, "order_total": total,
               "channel": campaign["channel"] if campaign else "organic",
               "utm_source": campaign["utm_source"] if campaign else None,
               "utm_campaign": campaign["utm_campaign"] if campaign else None}
    out = [("orders", _ev(rng, "order_created", t, order_id, customer, created))]

    if rng.random() < 0.05:  # cancel before shipment
        t2 = t + timedelta(minutes=rng.randint(5, 180))
        out.append(("orders", _ev(rng, "order_cancelled", t2, order_id, customer, {"reason": "customer_request"})))
        return out

    def add(topic, etype, payload):
        out.append((topic, _ev(rng, etype, t, order_id, customer, payload)))

    t += timedelta(minutes=rng.randint(10, 60))
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
    return out
