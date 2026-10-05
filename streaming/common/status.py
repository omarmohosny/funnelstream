STATUS_RANK = {
    "order_created": 1,
    "payment_authorised": 2,
    "order_packed": 3,
    "order_shipped": 4,
    "out_for_delivery": 5,
    "delivery_failed": 5,
    "delivered": 6,
    "order_completed": 7,
    "order_returned": 7,  # terminal, mutually exclusive with order_completed (tie: latest status_time)
    "order_cancelled": 2,  # valid only before order_shipped; see resolve_current_status
}


def resolve_current_status(statuses: list[str]) -> str:
    """Highest-rank status. A cancel counts only if the order never shipped."""
    shipped = "order_shipped" in statuses
    candidates = [s for s in statuses if not (s == "order_cancelled" and shipped)]
    return max(candidates, key=lambda s: STATUS_RANK[s])


def resolve_current_status_timed(rows):
    """rows: (status, status_time) tuples. Spec 17.6.

    A cancel counts only if no order_shipped row has status_time earlier than the cancel's.
    A counted cancel is terminal. Otherwise the highest rank wins; ties (ranks 5 and 7) go to the
    latest status_time. Arrival order never matters. Raises ValueError on an empty list.
    """
    rows = list(rows)
    if not rows:
        raise ValueError("no status rows")
    ship_times = [t for s, t in rows if s == "order_shipped"]
    counted = [(s, t) for s, t in rows
               if s == "order_cancelled" and not any(st < t for st in ship_times)]
    if counted:
        return max(counted, key=lambda r: r[1])[0]
    rest = [(s, t) for s, t in rows if s != "order_cancelled"]
    if not rest:  # only cancels after shipment cannot happen (shipment row would be in rest)
        raise ValueError("no usable status rows")
    # exact ties on rank and time: prefer order_returned over order_completed, then alphabetical name
    return max(rest, key=lambda r: (STATUS_RANK[r[0]], r[1], r[0] == "order_returned", _neg_name(r[0])))[0]


def _neg_name(name: str) -> tuple:
    """Sort key that makes max() pick the alphabetically first name."""
    return tuple(-ord(c) for c in name)
