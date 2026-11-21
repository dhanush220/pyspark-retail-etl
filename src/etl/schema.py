"""Target schema and source-to-target column mapping (schema harmonization).

Pure Python, no Spark import, so it can be unit-tested anywhere.
"""
import re

# Final column order and Spark SQL type of the harmonized (silver) table.
TARGET_SCHEMA = {
    "order_id": "string",
    "customer_id": "string",
    "order_date": "date",
    "country": "string",
    "category": "string",
    "quantity": "int",
    "unit_price": "decimal(12,2)",
    "source_system": "string",
}

# Known aliases from every source system -> target column name.
COLUMN_ALIASES = {
    "order_no": "order_id",
    "cust_id": "customer_id",
    "order_dt": "order_date",
    "ctry_cd": "country",
    "product_category": "category",
    "qty": "quantity",
    "price": "unit_price",
}

# Date formats seen across sources, tried in order.
DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "yyyyMMdd"]


def normalize_column_name(name: str) -> str:
    """'  Order DT ' -> 'order_dt'. Lower-case, trim, non-alphanumerics to '_'."""
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_")
    return cleaned.lower()


def build_rename_map(columns):
    """Map raw column names to target names.

    Returns (rename_map, unknown_columns). Unknown columns are reported,
    not silently dropped, so new upstream fields are noticed.
    """
    rename, unknown = {}, []
    for col in columns:
        norm = normalize_column_name(col)
        target = COLUMN_ALIASES.get(norm, norm)
        if target in TARGET_SCHEMA:
            rename[col] = target
        else:
            unknown.append(col)
    return rename, unknown


def missing_target_columns(mapped_columns):
    """Target columns (except source_system, added later) not provided by a source."""
    provided = set(mapped_columns)
    return [c for c in TARGET_SCHEMA if c != "source_system" and c not in provided]
