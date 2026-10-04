import json

DELIVERY_TYPES = ("out_for_delivery", "delivered", "delivery_failed")


def route(source: str, event: dict) -> tuple[str, str]:
    """source is 'app' for app/web events, 'order' for order and delivery events."""
    if source == "app":
        return "app.events", event["session_id"]
    if event["event_type"] in DELIVERY_TYPES:
        return "delivery.events", event["order_id"]
    return "orders", event["order_id"]


def build_producer_config(bootstrap: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
    }


class Publisher:
    def __init__(self, bootstrap: str):
        from confluent_kafka import Producer  # lazy: package only needed when publishing
        self._p = Producer(build_producer_config(bootstrap))

    def send(self, topic: str, key: str, event: dict) -> None:
        self._p.produce(topic, key=key.encode(), value=json.dumps(event).encode())
        self._p.poll(0)

    def flush(self) -> None:
        self._p.flush()
