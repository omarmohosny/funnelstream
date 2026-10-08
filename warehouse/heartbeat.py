"""Best-effort pipeline heartbeat (spec 17.17): beat("publish") writes pipeline_heartbeat(component, now, details).

Used by scripts/publish_silver.py and warehouse/gold_build.py after a successful run. It builds its own Postgres store
from PG_DSN with a short connect timeout and swallows every error: a heartbeat must never fail or slow the job.
Without PG_DSN it does nothing.
"""
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("funnelstream.heartbeat")

CONNECT_TIMEOUT = 3  # seconds


def beat(component, details=None, env=None, store_factory=None) -> bool:
    """True if the heartbeat was written. Never raises."""
    env = os.environ if env is None else env
    dsn = env.get("PG_DSN")
    if not dsn and store_factory is None:
        return False
    store = None
    try:
        if store_factory is not None:
            store = store_factory()
        else:
            from warehouse.store import PostgresStore
            store = PostgresStore.bounded(dsn)
        store.upsert("pipeline_heartbeat", [dict(component=component, last_seen_at=datetime.now(timezone.utc),
                                                 details=details)], ["component"])
        return True
    except Exception as e:
        log.warning("heartbeat %s not written: %s", component, type(e).__name__)
        return False
    finally:
        if store is not None:
            try:
                store.close()
            except Exception:
                pass
