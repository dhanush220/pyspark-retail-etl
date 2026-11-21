"""Spark tests (skipped automatically if PySpark/Java are not installed)."""
from datetime import date
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from etl import transforms as T  # noqa: E402


def test_harmonize_maps_legacy_columns(spark):
    df = spark.createDataFrame(
        [("B-1", "C1", "05/03/2025", "nl", "home", "2", "EUR 10.50")],
        ["ORDER_NO", "CUST_ID", "ORDER_DT", "CTRY_CD", "PRODUCT_CATEGORY", "QTY", "PRICE"],
    )
    out = T.harmonize(df, "source_b")
    assert out.columns == ["order_id", "customer_id", "order_date", "country", "category",
                           "quantity", "unit_price", "source_system"]
    assert out.first()["source_system"] == "source_b"


def test_safe_casts_turn_bad_values_into_null(spark):
    df = spark.createDataFrame(
        [("2025-03-05", "3", "EUR 12.50"), ("05/03/2025", "x", "N/A"), ("not a date", "1", "")],
        ["order_date", "quantity", "unit_price"],
    )
    rows = T.apply_types(df).collect()
    assert rows[0]["order_date"] == date(2025, 3, 5)
    assert rows[0]["unit_price"] == Decimal("12.50")
    assert rows[1]["order_date"] == date(2025, 3, 5)   # dd/MM/yyyy format
    assert rows[1]["quantity"] is None
    assert rows[1]["unit_price"] is None
    assert rows[2]["order_date"] is None


def test_split_and_dedupe(spark):
    raw = spark.createDataFrame(
        [
            ("A-1", "C1", "2025-01-01", " de ", "grocery", "2", "5.00", "a"),
            ("A-1", "C1", "2025-01-01", " de ", "grocery", "2", "5.00", "a"),   # duplicate
            ("A-2", "", "2025-01-02", "NL", "", "-1", "5.00", "a"),             # bad quantity
            ("A-3", "C3", "2025-01-03", "", "home", "1", "N/A", "a"),           # no country, no price
        ],
        ["order_id", "customer_id", "order_date", "country", "category", "quantity", "unit_price",
         "source_system"],
    )
    typed = T.clean_values(T.apply_types(raw))
    valid, rejected = T.split_valid_rejected(typed)
    silver = T.deduplicate(valid).collect()

    assert len(silver) == 1
    assert silver[0]["country"] == "DE" and silver[0]["category"] == "Grocery"
    reasons = {r["order_id"]: r["reject_reason"] for r in rejected.collect()}
    assert reasons == {"A-2": "bad_quantity", "A-3": "missing_country,bad_unit_price"}


def test_daily_sales(spark):
    df = spark.createDataFrame(
        [("A-1", date(2025, 1, 1), "DE", "Home", 2, Decimal("10.00")),
         ("A-2", date(2025, 1, 1), "DE", "Home", 1, Decimal("5.50"))],
        "order_id string, order_date date, country string, category string, quantity int, "
        "unit_price decimal(12,2)",
    )
    row = T.daily_sales(df).first()
    assert float(row["revenue"]) == 25.50
    assert row["orders"] == 2 and row["units"] == 3
