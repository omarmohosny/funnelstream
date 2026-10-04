import random
from datetime import date, timedelta

CATEGORIES = {
    "fashion": ["Galabeya", "Abaya", "Denim jacket", "Sneakers", "Hijab set"],
    "beauty": ["Argan oil", "Face serum", "Lipstick", "Kohl pencil"],
    "electronics": ["Earbuds", "Power bank", "Phone case", "Smart watch"],
    "home": ["Coffee set", "Bed sheets", "Towel set", "Water filter"],
}
FIRST_NAMES = ["Mohamed", "Ahmed", "Mariam", "Nour", "Youssef", "Hagar", "Omar", "Salma", "Karim", "Dina"]
LAST_NAMES = ["Hassan", "Mahmoud", "Ibrahim", "Abdelaziz", "Fathy", "Sayed", "Gamal", "Ragab"]
CITIES = [("Cairo", 0.30), ("Giza", 0.18), ("Alexandria", 0.14), ("Mansoura", 0.06), ("Tanta", 0.05), ("Assiut", 0.04), ("Other", 0.23)]
COUNTRY = "EG"


def make_catalog(seed: int, n_products: int = 200) -> list[dict]:
    rng = random.Random(seed)
    products = []
    cats = list(CATEGORIES)
    for i in range(n_products):
        cat = cats[i % len(cats)]
        base = {"fashion": 350, "beauty": 180, "electronics": 900, "home": 260}[cat]
        products.append({
            "product_id": f"prod_{i + 1:04d}",
            "name": f"{rng.choice(CATEGORIES[cat])} {i + 1}",
            "category": cat,
            "price": round(base * rng.uniform(0.6, 1.8), 2),
            "is_active": rng.random() > 0.05,
        })
    return products


def _weighted_city(rng: random.Random) -> str:
    r, acc = rng.random(), 0.0
    for city, w in CITIES:
        acc += w
        if r <= acc:
            return city
    return "Other"


def make_customers(seed: int, n: int) -> list[dict]:
    rng = random.Random(seed)
    out = []
    for i in range(n):
        prefix = rng.choice(["10", "11", "12", "15"])
        phone = "+20" + prefix + "".join(str(rng.randint(0, 9)) for _ in range(8))
        out.append({
            "customer_id": f"cust_{i + 1:06d}",
            "name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
            "phone": phone,
            "country": COUNTRY,
            "city": _weighted_city(rng),
            "device_primary": rng.choice(["mobile", "mobile", "mobile", "desktop"]),
            "signup_date": (date(2025, 1, 1) + timedelta(days=rng.randint(0, 600))).isoformat(),
            "marketing_consent": rng.random() < 0.6,
        })
    return out
