from collections import defaultdict
from datetime import datetime, timezone


def _ts(value) -> float:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def find_bot_sessions(events, max_events: int = 100, window_seconds: float = 10) -> set[str]:
    """Session ids with more than max_events events inside any window of window_seconds.

    O(n log n): group per session, sort times, two-pointer sliding window. Events without a
    session_id (or with an unparsable event_time) are ignored.
    """
    times = defaultdict(list)
    for e in events:
        sid = e.get("session_id")
        if not sid:
            continue
        try:
            times[sid].append(_ts(e["event_time"]))
        except (KeyError, ValueError, TypeError):
            continue
    bots = set()
    for sid, ts in times.items():
        if len(ts) <= max_events:
            continue
        ts.sort()
        lo = 0
        for hi, t in enumerate(ts):
            while t - ts[lo] > window_seconds:
                lo += 1
            if hi - lo + 1 > max_events:
                bots.add(sid)
                break
    return bots
