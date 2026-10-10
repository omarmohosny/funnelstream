"""The eight simulated dark stores and the district to store map (spec 2026-10-10 inventory design, section 5).

One source for everything: the simulator picks the store of a sale from it, and the same rows are written to the
Postgres tables dim_store and dim_store_district, where the live queries join demand to stock in SQL. A real
system uses delivery zones; here each district has exactly one primary store. Assiut and "Other" have no store
("not served") and are left out of every out-of-stock metric.
"""

STORES = {
    "store_cairo_nasr_city": ("Cairo", "Nasr City dark store",
                              ("Nasr City", "Heliopolis", "Shubra", "El Matareya", "El Marg", "El Zaytoun")),
    "store_cairo_maadi": ("Cairo", "Maadi dark store",
                          ("Maadi", "Zamalek", "Downtown", "Garden City", "Helwan", "Sayeda Zeinab")),
    "store_cairo_new_cairo": ("Cairo", "New Cairo dark store", ("New Cairo", "El Shorouk", "El Moqattam")),
    "store_giza_dokki": ("Giza", "Dokki dark store",
                         ("Dokki", "Agouza", "Haram", "Imbaba", "Omraniya", "Bulaq El Dakrour", "El Warraq",
                          "Giza (centre)")),
    "store_alex_raml": ("Alexandria", "Raml dark store",
                        ("Raml", "Sidi Gaber", "Moharam Bek", "Karmouz", "Attarin", "Manshiya")),
    "store_alex_montaza": ("Alexandria", "Montaza dark store", ("Montaza", "Amreya", "Dekhela", "Labban")),
    "store_mansoura": ("Mansoura", "Mansoura dark store", ("Mansoura First", "Mansoura Second")),
    "store_tanta": ("Tanta", "Tanta dark store", ("Tanta First", "Tanta Second")),
}

_STORE_OF = {(city, district): store_id
             for store_id, (city, _, districts) in STORES.items() for district in districts}


def store_for(city, district):
    """store_id that serves the (canonical city, canonical district), or None when it is not served."""
    return _STORE_OF.get((city, district))


def store_rows():
    return [{"store_id": store_id, "store_name": name, "city": city} for store_id, (city, name, _) in STORES.items()]


def district_rows():
    return [{"city": city, "district": district, "store_id": store_id}
            for (city, district), store_id in sorted(_STORE_OF.items())]
