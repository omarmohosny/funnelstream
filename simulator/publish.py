import json

DELIVERY_TYPES = ("out_for_delivery", "delivered", "delivery_failed")


UNPARSABLE_KEY = "unparsable"


def route(source: str, event) -> tuple[str, str]:
    """source is 'app' for app/web events, 'order' for order and delivery events, 'inventory' for stock events.

    Noised messages are routed by their hint: a str (malformed JSON) or an event missing its
    key field gets the key "unparsable"; an event with a missing/unknown event_type on an
    order hint goes to "orders".
    """
    if source == "inventory":
        if isinstance(event, str):  # not produced today (no noise on inventory), kept for symmetry
            return "inventory.events", UNPARSABLE_KEY
        return "inventory.events", event.get("store_id") or UNPARSABLE_KEY
    if isinstance(event, str):
        return ("app.events" if source == "app" else "orders"), UNPARSABLE_KEY
    if source == "app":
        return "app.events", event.get("session_id") or UNPARSABLE_KEY
    if event.get("event_type") in DELIVERY_TYPES:
        return "delivery.events", event.get("order_id") or UNPARSABLE_KEY
    return "orders", event.get("order_id") or UNPARSABLE_KEY


def build_producer_config(bootstrap: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500000,
    }


class Publisher:
    def __init__(self, bootstrap: str, producer=None, max_retries: int = 100):
        if producer is None:
            from confluent_kafka import Producer  # lazy: package only needed when publishing
            producer = Producer(build_producer_config(bootstrap))
        self._p = producer
        self._max_retries = max_retries

    def send(self, topic: str, key: str, event) -> None:
        """event is a dict (json.dumps'd) or a str (published unchanged, utf-8)."""
        value = event.encode("utf-8") if isinstance(event, str) else json.dumps(event).encode()
        for _ in range(self._max_retries):
            try:
                self._p.produce(topic, key=key.encode(), value=value)
                break
            except BufferError:  # local queue full: serve delivery callbacks, then retry
                self._p.poll(1)
        else:
            raise BufferError(f"producer queue still full after {self._max_retries} retries")
        self._p.poll(0)

    def flush(self) -> None:
        self._p.flush()
