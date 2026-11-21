"""Pure-Python tests (no Spark needed)."""
from etl.quality import all_passed, evaluate, reconcile
from etl.schema import build_rename_map, missing_target_columns, normalize_column_name


def test_normalize_column_name():
    assert normalize_column_name("  Order DT ") == "order_dt"
    assert normalize_column_name("CTRY-CD") == "ctry_cd"


def test_rename_map_handles_legacy_schema():
    rename, unknown = build_rename_map(["ORDER_NO", "CUST_ID", "ORDER_DT", "QTY", "PRICE", "EXTRA_COL"])
    assert rename == {
        "ORDER_NO": "order_id",
        "CUST_ID": "customer_id",
        "ORDER_DT": "order_date",
        "QTY": "quantity",
        "PRICE": "unit_price",
    }
    assert unknown == ["EXTRA_COL"]


def test_missing_target_columns():
    assert missing_target_columns(["order_id", "order_date"]) == [
        "customer_id", "country", "category", "quantity", "unit_price"
    ]


def _metrics(**over):
    m = dict(raw_rows=100, valid_rows=85, rejected_rows=10, duplicates_removed=5,
             distinct_orders=85, null_order_id=0, non_positive_qty=0)
    m.update(over)
    return m


def test_reconciliation_passes_when_all_rows_accounted():
    assert reconcile(_metrics()).passed


def test_reconciliation_fails_when_rows_lost():
    assert not reconcile(_metrics(valid_rows=80, distinct_orders=80)).passed


def test_quality_checks_fail_on_duplicates_and_high_reject_ratio():
    results = {r.name: r for r in evaluate(_metrics(distinct_orders=84, rejected_rows=40, raw_rows=130))}
    assert not results["order_id_unique"].passed
    assert not results["reject_ratio_ok"].passed


def test_clean_metrics_pass():
    assert all_passed(evaluate(_metrics()))
