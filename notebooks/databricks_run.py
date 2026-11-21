# Databricks notebook source
# MAGIC %md
# MAGIC # Retail orders ETL on Databricks
# MAGIC 1. Import this repo with **Workspace → Create → Git folder** (paste your GitHub URL).
# MAGIC 2. Click **Run all**. The notebook creates the Volume `workspace.default.etl`,
# MAGIC    generates sample data, runs the pipeline and shows the results.

# COMMAND ----------

import os, subprocess, sys

repo_root = os.path.dirname(os.getcwd())          # notebook lives in <repo>/notebooks
sys.path.insert(0, os.path.join(repo_root, "src"))

# Free Edition has a "workspace" catalog with a "default" schema; create a Volume for files.
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.etl")  # noqa: F821
BASE = "/Volumes/workspace/default/etl"
RAW, OUT = f"{BASE}/raw", f"{BASE}/output"

# COMMAND ----------

# Generate sample data straight into the Volume (Volumes are mounted at /Volumes/...).
subprocess.run(
    [sys.executable, os.path.join(repo_root, "data", "generate_sample_data.py"), "5000", RAW],
    check=True,
)

# COMMAND ----------

import importlib

import etl.pipeline, etl.quality, etl.schema, etl.transforms

# Reload so a `git pull` takes effect without restarting the notebook's Python process.
for m in (etl.schema, etl.transforms, etl.quality, etl.pipeline):
    importlib.reload(m)

metrics = etl.pipeline.run(spark, RAW, OUT, fail_on_dq=True)  # noqa: F821  (spark exists in Databricks)
metrics

# COMMAND ----------

display(spark.read.parquet(f"{OUT}/gold/daily_sales").orderBy("revenue", ascending=False))  # noqa: F821

# COMMAND ----------

display(spark.read.parquet(f"{OUT}/quarantine/orders_rejected").groupBy("reject_reason").count())  # noqa: F821
