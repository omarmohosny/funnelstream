"""Pure normalisation helpers (used by the Silver job)."""
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

CANONICAL_CITIES = ("Cairo", "Giza", "Alexandria", "Mansoura", "Tanta", "Assiut", "Other")

_CITY_ALIASES = {
    "cairo": "Cairo", "el cairo": "Cairo", "al cairo": "Cairo", "al qahira": "Cairo", "el qahira": "Cairo",
    "giza": "Giza", "el giza": "Giza", "al giza": "Giza", "gizah": "Giza",
    "alexandria": "Alexandria", "alex": "Alexandria", "alexandra": "Alexandria", "el iskandariyah": "Alexandria",
    "al iskandariyah": "Alexandria", "iskandariya": "Alexandria",
    "mansoura": "Mansoura", "el mansoura": "Mansoura", "al mansoura": "Mansoura",
    "tanta": "Tanta", "assiut": "Assiut", "asyut": "Assiut", "assyut": "Assiut",
    "other": "Other",
}

_DEVICE_ALIASES = {
    "mobile": "mobile", "phone": "mobile", "smartphone": "mobile", "android": "mobile", "ios": "mobile",
    "desktop": "desktop", "pc": "desktop", "computer": "desktop", "laptop": "desktop",
    "tablet": "tablet", "ipad": "tablet",
}


def _clean(value) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[-_]", " ", value)).strip().lower()


def normalize_city(value) -> str:
    return _CITY_ALIASES.get(_clean(value), "unknown")


def normalize_device(value) -> str:
    return _DEVICE_ALIASES.get(_clean(value), "unknown")


def to_utc_iso(value) -> str:
    """ISO string (Z, offset or naive=UTC) to a UTC ISO string ending in +00:00. ValueError on garbage."""
    if not isinstance(value, str):
        raise ValueError("not a string: %r" % (value,))
    try:
        ts = datetime.fromisoformat(value.strip())
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc).isoformat()
    except OverflowError as e:
        raise ValueError(str(e))


def to_decimal(value) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("not numeric: %r" % (value,))
    try:
        d = Decimal(str(value).strip())
    except InvalidOperation:
        raise ValueError("not numeric: %r" % (value,))
    if not d.is_finite():
        raise ValueError("not finite: %r" % (value,))
    return d
