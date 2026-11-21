"""PySpark transformations: harmonize -> safe-cast -> clean -> validate -> dedupe.

All casts use try_cast / try_to_timestamp so a bad value becomes NULL instead of
failing the whole job. This works with ANSI mode on (Spark 4 / Databricks
serverless) and off (Spark 3.5).
"""
import logging

from pyspark.sql import DataFrame, functions as F

from .schema import DATE_FORMATS, TARGET_SCHEMA, build_rename_map, missing_target_columns

log = logging.getLogger(__name__)


def harmonize(df: DataFrame, source_system: str) -> DataFrame:
    """Rename source columns to the target names, add missing ones as NULL,
    keep target columns only (as strings, typed later)."""
    rename, unknown = build_rename_map(df.columns)
    if unknown:
        log.warning("%s: ignoring unknown columns %s", source_system, unknown)
    df = df.select([F.col(f"`{src}`").cast("string").alias(dst) for src, dst in rename.items()])
    for col in missing_target_columns(df.columns):
        log.warning("%s: missing column %s, filling NULL", source_system, col)
        df = df.withColumn(col, F.lit(None).cast("string"))
    df = df.withColumn("source_system", F.lit(source_system))
    return df.select(*TARGET_SCHEMA.keys())


def parse_date(col: str):
    """Try every known date format; first one that parses wins, else NULL."""
    attempts = [F.expr(f"try_to_timestamp(`{col}`, '{fmt}')") for fmt in DATE_FORMATS]
    return F.to_date(F.coalesce(*attempts))


def safe_amount(col: str):
    """'EUR 12.50' -> '12.50', 'N/A' or '' -> NULL (still a string; typed in apply_types)."""
    number = F.regexp_extract(F.coalesce(F.col(col), F.lit("")), r"(-?\d+(?:\.\d+)?)", 1)
    return F.when(number == "", None).otherwise(number)


def apply_types(df: DataFrame) -> DataFrame:
    """Cast harmonized string columns to the target types without failing on bad data."""
    df = df.withColumn("unit_price", safe_amount("unit_price"))
    return (
        df.withColumn("order_date", parse_date("order_date"))
        .withColumn("quantity", F.expr("try_cast(trim(quantity) AS INT)"))
        .withColumn("unit_price", F.expr("try_cast(unit_price AS DECIMAL(12,2))"))
    )


def clean_values(df: DataFrame) -> DataFrame:
    """Standardize text values and handle NULLs with explicit defaults."""
    empty_to_null = lambda c: F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))  # noqa: E731
    return (
        df.withColumn("order_id", empty_to_null("order_id"))
        .withColumn("customer_id", F.coalesce(empty_to_null("customer_id"), F.lit("UNKNOWN")))
        .withColumn("country", F.upper(empty_to_null("country")))
        .withColumn("category", F.coalesce(F.initcap(empty_to_null("category")), F.lit("Unknown")))
    )


def split_valid_rejected(df: DataFrame):
    """Return (valid, rejected). Rejected rows keep a reason for later analysis."""
    reasons = F.concat_ws(
        ",",
        F.when(F.col("order_id").isNull(), F.lit("missing_order_id")),
        F.when(F.col("order_date").isNull(), F.lit("bad_order_date")),
        F.when(F.col("country").isNull(), F.lit("missing_country")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("bad_quantity")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("bad_unit_price")),
    )
    tagged = df.withColumn("reject_reason", reasons)
    valid = tagged.filter(F.col("reject_reason") == "").drop("reject_reason")
    rejected = tagged.filter(F.col("reject_reason") != "")
    return valid, rejected


def deduplicate(df: DataFrame) -> DataFrame:
    """One row per order_id (exact re-deliveries from the source are dropped)."""
    return df.dropDuplicates(["order_id"])


def add_partitions(df: DataFrame) -> DataFrame:
    """Partition columns used for dynamic partition overwrite."""
    return df.withColumn("year", F.year("order_date")).withColumn("month", F.month("order_date"))


def daily_sales(df: DataFrame) -> DataFrame:
    """Gold table: revenue and order count per day, country and category."""
    return (
        df.withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .groupBy("order_date", "country", "category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
        )
    )
