"""Run the pipeline on your laptop.

    python data/generate_sample_data.py
    python run_local.py --raw data/raw --out output
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyspark.sql import SparkSession  # noqa: E402

from etl.pipeline import run  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--out", default="output")
    p.add_argument("--no-fail-on-dq", action="store_true", help="write outputs even if checks fail")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    spark = (
        SparkSession.builder.appName("retail-orders-etl")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    try:
        metrics = run(spark, args.raw, args.out, fail_on_dq=not args.no_fail_on_dq)
        print("\nMetrics:", metrics)
        spark.read.parquet(f"{args.out}/gold/daily_sales").orderBy("revenue", ascending=False).show(10)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
