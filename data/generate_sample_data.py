"""Generate messy raw retail order files from two "source systems".

The data is synthetic and deliberately dirty, so the pipeline has real work to do:
  * two sources with different column names and date formats (schema drift)
  * amounts stored as text, some with currency symbols or garbage
  * missing values, duplicate rows, negative quantities

Usage:  python data/generate_sample_data.py [rows_per_source]
Writes: data/raw/source_a/orders_a.csv and data/raw/source_b/orders_b.csv
"""
import csv
import random
import sys
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
OUT = Path(__file__).parent / "raw"

COUNTRIES = ["DE", "NL", "IE", "PL", "FR", "IN", "de", " nl "]
CATEGORIES = ["Electronics", "Grocery", "Fashion", "Home", "Sports", None]
START = date(2025, 1, 1)


def amount_text(value: float) -> str:
    roll = random.random()
    if roll < 0.03:
        return "N/A"            # unparseable
    if roll < 0.06:
        return ""               # empty
    if roll < 0.15:
        return f"EUR {value:.2f}"  # prefixed text
    return f"{value:.2f}"


def rows(prefix: str, n: int):
    for i in range(n):
        d = START + timedelta(days=random.randint(0, 364))
        qty = -1 if random.random() < 0.03 else random.choice([1, 1, 2, 3, 5])  # -1 = bad record
        yield {
            "order_id": f"{prefix}-{i:06d}",
            "customer_id": random.choice([f"C{random.randint(1, 900):04d}", None]),
            "order_date": d,
            "country": None if random.random() < 0.03 else random.choice(COUNTRIES),
            "category": random.choice(CATEGORIES),
            "quantity": qty,
            "unit_price": amount_text(random.uniform(2, 900)),
        }


def write_source_a(n: int):
    path = OUT / "source_a" / "orders_a.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    data = list(rows("A", n))
    data += random.sample(data, k=n // 50)  # ~2% exact duplicates
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "order_date", "country", "category", "quantity", "unit_price"])
        for r in data:
            w.writerow([r["order_id"], r["customer_id"] or "", r["order_date"].isoformat(),
                        r["country"] or "", r["category"] or "", r["quantity"], r["unit_price"]])
    return len(data)


def write_source_b(n: int):
    """Same business data, but a different legacy schema and date format."""
    path = OUT / "source_b" / "orders_b.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    data = list(rows("B", n))
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ORDER_NO", "CUST_ID", "ORDER_DT", "CTRY_CD", "PRODUCT_CATEGORY", "QTY", "PRICE"])
        for r in data:
            w.writerow([r["order_id"], r["customer_id"] or "", r["order_date"].strftime("%d/%m/%Y"),
                        r["country"] or "", r["category"] or "", r["quantity"], r["unit_price"]])
    return len(data)


if __name__ == "__main__":
    a = write_source_a(ROWS)
    b = write_source_b(ROWS)
    print(f"Wrote {a} rows to source_a and {b} rows to source_b under {OUT}")
