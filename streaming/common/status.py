STATUS_RANK = {
    "order_created": 1,
    "payment_authorised": 2,
    "order_packed": 3,
    "order_shipped": 4,
    "out_for_delivery": 5,
    "delivery_failed": 5,
    "delivered": 6,
    "order_completed": 7,
    "order_cancelled": 2,  # valid only before order_shipped; see resolve_current_status
}


def resolve_current_status(statuses: list[str]) -> str:
    """Highest-rank status. A cancel counts only if the order never shipped."""
    shipped = "order_shipped" in statuses
    candidates = [s for s in statuses if not (s == "order_cancelled" and shipped)]
    return max(candidates, key=lambda s: STATUS_RANK[s])
