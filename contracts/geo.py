"""Address hierarchy reference: country > city > district (spec section 18.1).

DISTRICTS is the source of truth. normalize_district() is pure and never raises; it maps a raw
district string to the canonical spelling for an already normalised canonical city, or "unknown".
"""
import re

COUNTRY = "EG"

DISTRICTS: dict[str, tuple[str, ...]] = {
    "Cairo": ("Nasr City", "Heliopolis", "Maadi", "New Cairo", "Shubra", "Zamalek", "Downtown",
              "Garden City", "El Moqattam", "Helwan", "El Matareya", "El Marg", "El Zaytoun",
              "Sayeda Zeinab", "El Shorouk"),
    "Giza": ("Dokki", "Agouza", "Haram", "Imbaba", "Omraniya", "Bulaq El Dakrour", "El Warraq",
             "Giza (centre)"),
    "Alexandria": ("Montaza", "Raml", "Sidi Gaber", "Moharam Bek", "Karmouz", "Amreya", "Dekhela",
                   "Attarin", "Manshiya", "Labban"),
    "Mansoura": ("Mansoura First", "Mansoura Second"),
    "Tanta": ("Tanta First", "Tanta Second"),
    "Assiut": ("Assiut First", "Assiut Second"),
    "Other": ("Other",),
}

# Same order as DISTRICTS; every tuple sums to 1.0 (the last entry absorbs rounding).
DISTRICT_WEIGHTS: dict[str, tuple[float, ...]] = {
    "Cairo": (0.17, 0.12, 0.11, 0.12, 0.10, 0.04, 0.04, 0.03, 0.04, 0.05, 0.05, 0.03, 0.03, 0.03, 0.04),
    "Giza": (0.20, 0.08, 0.18, 0.16, 0.16, 0.08, 0.04, 0.10),
    "Alexandria": (0.14, 0.14, 0.12, 0.10, 0.10, 0.08, 0.08, 0.08, 0.08, 0.08),
    "Mansoura": (0.55, 0.45),
    "Tanta": (0.55, 0.45),
    "Assiut": (0.55, 0.45),
    "Other": (1.0,),
}

# Extra spellings, keyed by the cleaned form (see _key), value = canonical district.
_ALIASES = {
    "madinet nasr": "Nasr City",
    "masr el gedida": "Heliopolis",
    "misr el gedida": "Heliopolis",
    "tagamoa": "New Cairo",
    "el tagamoa": "New Cairo",
    "fifth settlement": "New Cairo",
    "5th settlement": "New Cairo",
    "el maadi": "Maadi",
    "pyramids": "Haram",
    "el haram": "Haram",
    "al haram": "Haram",
    "el dokki": "Dokki",
    "al dokki": "Dokki",
    "el agouza": "Agouza",
    "sidi gabr": "Sidi Gaber",
    "el raml": "Raml",
    "el montaza": "Montaza",
    "giza": "Giza (centre)",
    "giza centre": "Giza (centre)",
    "giza center": "Giza (centre)",
    "mansoura 1": "Mansoura First",
    "mansoura 2": "Mansoura Second",
    "tanta 1": "Tanta First",
    "tanta 2": "Tanta Second",
    "assiut 1": "Assiut First",
    "assiut 2": "Assiut Second",
}


def _key(value: str) -> str:
    """Case, spacing, hyphen, underscore and punctuation insensitive form."""
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _build_lookup() -> dict[str, str]:
    lookup: dict[str, str] = {}
    for districts in DISTRICTS.values():
        for d in districts:
            k = _key(d)
            bare = re.sub(r"^(el|al) ", "", k)
            for variant in (k, bare, "el " + bare, "al " + bare):
                lookup.setdefault(variant, d)
    for alias, d in _ALIASES.items():
        lookup[_key(alias)] = d
    return lookup


_LOOKUP = _build_lookup()


def normalize_district(city, value) -> str:
    """Canonical district of `city` for `value`, else "unknown".

    `city` is the already normalised canonical city (or "unknown")."""
    if not isinstance(city, str) or city not in DISTRICTS or not isinstance(value, str):
        return "unknown"
    district = _LOOKUP.get(_key(value))
    if district is None or district not in DISTRICTS[city]:
        return "unknown"
    return district


def district_violation(city, value) -> bool:
    """True when a non-empty district string does not belong to a known city."""
    if not isinstance(value, str) or not value.strip() or value.strip().lower() == "unknown":
        return False
    if not isinstance(city, str) or city not in DISTRICTS:
        return False
    return normalize_district(city, value) == "unknown"


def alias_variants(district: str) -> list[str]:
    """All alias spellings that map to `district` (used by the noise generator)."""
    return sorted(a for a, d in _ALIASES.items() if d == district)
