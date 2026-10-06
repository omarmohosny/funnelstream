"""Egyptian calendar facts for dim_date, rendered into warehouse/gold/04_dim_date.sql.

APPROXIMATE: Ramadan and the two Eids follow the moon sighting, so the real dates can differ by a day or two
from the ranges below. The ranges are fixed for the project (2025 to 2027) and are not fetched from anywhere.
Weekend in Egypt is Friday and Saturday.
"""
from datetime import date, timedelta

DIM_DATE_START = date(2025, 1, 1)
DIM_DATE_END = date(2027, 12, 31)

# (kind, first day, last day), both inclusive. kind: ramadan, eid (Eid al-Fitr and Eid al-Adha). APPROXIMATE.
CALENDAR_RANGES = [
    ("ramadan", date(2025, 3, 1), date(2025, 3, 29)),
    ("eid", date(2025, 3, 30), date(2025, 4, 1)),    # Eid al-Fitr
    ("eid", date(2025, 6, 6), date(2025, 6, 9)),     # Eid al-Adha
    ("ramadan", date(2026, 2, 18), date(2026, 3, 19)),
    ("eid", date(2026, 3, 20), date(2026, 3, 22)),   # Eid al-Fitr
    ("eid", date(2026, 5, 26), date(2026, 5, 29)),   # Eid al-Adha
    ("ramadan", date(2027, 2, 8), date(2027, 3, 9)),
    ("eid", date(2027, 3, 10), date(2027, 3, 12)),   # Eid al-Fitr
    ("eid", date(2027, 5, 16), date(2027, 5, 19)),   # Eid al-Adha
]

# (month, day, name): fixed-date public holidays, every year. is_holiday = Eid day or one of these.
FIXED_HOLIDAYS = [
    (1, 7, "Coptic Christmas"),
    (1, 25, "Revolution Day and Police Day"),
    (4, 25, "Sinai Liberation Day"),
    (5, 1, "Labour Day"),
    (7, 23, "July 23 Revolution Day"),
    (10, 6, "Armed Forces Day"),
]

WEEKEND_WEEKDAYS = (4, 5)  # Python weekday(): Friday=4, Saturday=5


def _in(kind, d):
    return any(k == kind and a <= d <= b for k, a, b in CALENDAR_RANGES)


def is_weekend(d) -> bool:
    return d.weekday() in WEEKEND_WEEKDAYS


def is_ramadan(d) -> bool:
    return _in("ramadan", d)


def is_eid(d) -> bool:
    return _in("eid", d)


def is_holiday(d) -> bool:
    return is_eid(d) or any((m, dd) == (d.month, d.day) for m, dd, _ in FIXED_HOLIDAYS)


def date_range(start=DIM_DATE_START, end=DIM_DATE_END):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def date_key(d) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def calendar_ranges_values() -> str:
    """VALUES rows (kind, DATE start, DATE end) for the {calendar_ranges} placeholder."""
    return ", ".join("('%s', DATE'%s', DATE'%s')" % (k, a.isoformat(), b.isoformat()) for k, a, b in CALENDAR_RANGES)


def fixed_holidays_values() -> str:
    """VALUES rows (month, day) for the {fixed_holidays} placeholder."""
    return ", ".join("(%d, %d)" % (m, d) for m, d, _ in FIXED_HOLIDAYS)
