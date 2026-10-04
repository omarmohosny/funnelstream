from datetime import datetime, timedelta, timezone

from streaming.common.bots import find_bot_sessions

T0 = datetime(2026, 10, 4, tzinfo=timezone.utc)


def evs(sid, n, span_seconds, start=T0):
    return [{"session_id": sid, "event_time": (start + timedelta(seconds=span_seconds * i / n)).isoformat()}
            for i in range(n)]


def test_burst_flagged():
    assert find_bot_sessions(evs("bot", 150, 5)) == {"bot"}


def test_normal_not_flagged():
    assert find_bot_sessions(evs("ok", 12, 60)) == set()


def test_spread_not_flagged():
    assert find_bot_sessions(evs("slow", 150, 600)) == set()


def test_no_session_ignored_and_mixed():
    noise = [{"session_id": None, "event_time": T0.isoformat()} for _ in range(500)]
    noise += [{"event_time": T0.isoformat()} for _ in range(500)]
    assert find_bot_sessions(noise + evs("bot", 150, 5) + evs("ok", 5, 5)) == {"bot"}


def test_threshold_is_strictly_more_than():
    assert find_bot_sessions(evs("s", 100, 5)) == set()
    assert find_bot_sessions(evs("s", 101, 5)) == {"s"}


def test_unsorted_input_and_z_suffix():
    e = evs("bot", 150, 5)
    e.reverse()
    e[0]["event_time"] = e[0]["event_time"].replace("+00:00", "Z")
    assert find_bot_sessions(e) == {"bot"}
