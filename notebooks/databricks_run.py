# Databricks notebook source
# MAGIC %md
# MAGIC # Retail orders ETL on Databricks
# MAGIC 1. Import this repo with **Workspace → Create → Git folder** (paste your GitHub URL).
# MAGIC 2. Run `data/generate_sample_data.py` once (cell below), or upload your own CSVs to a Volume.
# MAGIC 3. Run all cells. Outputs go to a Unity Catalog Volume.

# COMMAND ----------

import os, subprocess, sys

repo_root = os.path.dirname(os.getcwd())          # notebook lives in <repo>/notebooks
sys.path.insert(0, os.path.join(repo_root, "src"))

# Change these to a Volume you own, e.g. /Volumes/workspace/default/etl
BASE = "/Volumes/workspace/default/etl"
RAW, OUT = f"{BASE}/raw", f"{BASE}/output"

# COMMAND ----------

# Generate sample data into the repo, then copy it to the Volume.
subprocess.run([sys.executable, os.path.join(repo_root, "data", "generate_sample_data.py")], check=True)
dbutils.fs.cp(f"file:{repo_root}/data/raw", RAW, recurse=True)  # noqa: F821  (dbutils exists in Databricks)

# COMMAND ----------

from etl.pipeline import run

metrics = run(spark, RAW, OUT, fail_on_dq=True)  # noqa: F821  (spark exists in Databricks)
metrics

# COMMAND ----------

display(spark.read.parquet(f"{OUT}/gold/daily_sales").orderBy("revenue", ascending=False))  # noqa: F821

# COMMAND ----------

display(spark.read.parquet(f"{OUT}/quarantine/orders_rejected").groupBy("reject_reason").count())  # noqa: F821
