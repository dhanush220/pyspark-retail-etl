"""End-to-end pipeline used by run_local.py, the Databricks notebook and the Glue job.

Layers:
  raw CSV (two source systems)
    -> silver   : harmonized, typed, cleaned, deduplicated; Parquet partitioned by year/month
    -> rejected : bad rows + reason (quarantine for review)
    -> gold     : daily revenue by country and category
"""
import logging
from functools import reduce

from pyspark.sql import DataFrame, SparkSession

from . import quality, transforms as T

log = logging.getLogger(__name__)

# source system name -> sub-folder under the raw input path
SOURCES = {"source_a": "source_a", "source_b": "source_b"}


def read_source(spark: SparkSession, path: str) -> DataFrame:
    # Everything is read as string; typing happens in apply_types with safe casts.
    return spark.read.option("header", True).option("inferSchema", False).csv(path)


def build(spark: SparkSession, raw_path: str):
    """Pure transformation step (no writes). Returns (silver, rejected, gold, metrics)."""
    harmonized = [
        T.harmonize(read_source(spark, f"{raw_path.rstrip('/')}/{folder}"), name)
        for name, folder in SOURCES.items()
    ]
    raw = reduce(DataFrame.unionByName, harmonized)
    raw_count = raw.count()

    typed = T.clean_values(T.apply_types(raw))
    valid, rejected = T.split_valid_rejected(typed)
    valid = valid.cache()
    valid_before = valid.count()
    silver = T.add_partitions(T.deduplicate(valid)).cache()
    dedup_removed = valid_before - silver.count()

    metrics = quality.collect_metrics(silver, rejected, raw_count, dedup_removed)
    gold = T.daily_sales(silver)
    return silver, rejected, gold, metrics


def run(spark: SparkSession, raw_path: str, out_path: str, fail_on_dq: bool = True) -> dict:
    # Only overwrite the year/month partitions present in this run.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    silver, rejected, gold, metrics = build(spark, raw_path)
    results = quality.evaluate(metrics)
    report = quality.format_report(results)
    log.info("Data quality report:\n%s", report)
    print(report)

    if fail_on_dq and not quality.all_passed(results):
        raise RuntimeError("Data quality checks failed, nothing was written.\n" + report)

    out = out_path.rstrip("/")
    silver.write.mode("overwrite").partitionBy("year", "month").parquet(f"{out}/silver/orders")
    rejected.write.mode("overwrite").parquet(f"{out}/quarantine/orders_rejected")
    gold.write.mode("overwrite").parquet(f"{out}/gold/daily_sales")
    return metrics
