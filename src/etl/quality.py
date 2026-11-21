"""Data quality checks and row-count reconciliation.

`evaluate` and `reconcile` are pure Python (unit-testable without Spark);
`collect_metrics` is the only function that touches Spark.
"""
from dataclasses import dataclass


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def collect_metrics(valid_df, rejected_df, raw_count: int, dedup_removed: int) -> dict:
    """Gather all numbers the checks need in as few Spark actions as possible."""
    from pyspark.sql import functions as F

    stats = valid_df.agg(
        F.count("*").alias("valid_rows"),
        F.countDistinct("order_id").alias("distinct_orders"),
        F.sum(F.col("order_id").isNull().cast("int")).alias("null_order_id"),
        F.sum((F.col("quantity") <= 0).cast("int")).alias("non_positive_qty"),
        F.min("order_date").alias("min_date"),
        F.max("order_date").alias("max_date"),
    ).first().asDict()
    stats["rejected_rows"] = rejected_df.count()
    stats["raw_rows"] = raw_count
    stats["duplicates_removed"] = dedup_removed
    return stats


def evaluate(m: dict, max_reject_ratio: float = 0.25) -> list:
    """Turn metrics into pass/fail checks."""
    reject_ratio = m["rejected_rows"] / m["raw_rows"] if m["raw_rows"] else 1.0
    return [
        CheckResult("not_empty", m["valid_rows"] > 0, f"{m['valid_rows']} valid rows"),
        CheckResult("order_id_unique", m["valid_rows"] == m["distinct_orders"],
                    f"{m['valid_rows']} rows / {m['distinct_orders']} distinct order_id"),
        CheckResult("order_id_not_null", (m["null_order_id"] or 0) == 0, f"{m['null_order_id'] or 0} nulls"),
        CheckResult("quantity_positive", (m["non_positive_qty"] or 0) == 0,
                    f"{m['non_positive_qty'] or 0} rows <= 0"),
        CheckResult("reject_ratio_ok", reject_ratio <= max_reject_ratio,
                    f"{reject_ratio:.1%} rejected (limit {max_reject_ratio:.0%})"),
        reconcile(m),
    ]


def reconcile(m: dict) -> CheckResult:
    """Every raw row must be accounted for: raw = valid + rejected + duplicates removed."""
    accounted = m["valid_rows"] + m["rejected_rows"] + m["duplicates_removed"]
    return CheckResult(
        "row_reconciliation",
        accounted == m["raw_rows"],
        f"raw {m['raw_rows']} = valid {m['valid_rows']} + rejected {m['rejected_rows']}"
        f" + duplicates {m['duplicates_removed']} (accounted {accounted})",
    )


def format_report(results: list) -> str:
    lines = [f"[{'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}" for r in results]
    return "\n".join(lines)


def all_passed(results: list) -> bool:
    return all(r.passed for r in results)
