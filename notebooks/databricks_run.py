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
