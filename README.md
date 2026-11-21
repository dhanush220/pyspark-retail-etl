# Retail Orders ETL with PySpark (Databricks + AWS Glue)

A batch data pipeline that ingests messy order data from two source systems with different schemas, cleans and validates it, and publishes analytics-ready tables. The same code runs locally, on **Databricks** and as an **AWS Glue** job.

> Personal portfolio project built with synthetic data. It shows patterns I use in production data engineering (schema harmonization, safe type casting, data quality and reconciliation). It contains no employer or client code.

## Architecture

```
 source_a/*.csv  (order_id, order_date yyyy-MM-dd, ...)
 source_b/*.csv  (ORDER_NO, ORDER_DT dd/MM/yyyy, PRICE "EUR 12.50", ...)
        │
        ▼  harmonize: rename legacy columns, add missing ones, tag source_system
        ▼  safe casts: try_cast / try_to_timestamp, so bad values become NULL instead of failing the job
        ▼  clean: trim, upper-case country, default category, NULL handling
        ▼  validate: rows with bad data → quarantine with reject_reason
        ▼  deduplicate on order_id
        │
        ├── silver/orders              Parquet, partitioned by year/month (dynamic partition overwrite)
        ├── quarantine/orders_rejected rejected rows + reason
        └── gold/daily_sales           revenue, orders, units per day / country / category
        │
        ▼  data quality gate: uniqueness, not-null, positive quantity, reject ratio,
           row reconciliation (raw = valid + rejected + duplicates). Nothing is written if a check fails.
```

## Tech stack
Python · PySpark (Spark 3.5 / 4.x) · Spark SQL · Databricks · AWS Glue · Amazon S3 · Amazon Athena · Parquet · pytest

## Project layout
```
src/etl/schema.py      target schema, column aliases, harmonization helpers (pure Python)
src/etl/transforms.py  PySpark transformations
src/etl/quality.py     data quality checks + reconciliation
src/etl/pipeline.py    orchestration: read → transform → DQ gate → write
run_local.py           run on a laptop
notebooks/databricks_run.py  run on Databricks
glue/glue_job.py       run as an AWS Glue job
data/generate_sample_data.py  creates dirty sample data
tests/                 pytest (pure-Python tests + Spark tests)
```

## Run it

### Option 1: Databricks Free Edition (no installation)
1. Sign up at Databricks Free Edition.
2. **Workspace → Create → Git folder**, paste this repo's GitHub URL.
3. Open `notebooks/databricks_run.py` and **Run all** (it creates the Volume `workspace.default.etl` itself).

### Option 2: Local
Requires Python 3.10+ and Java 17.
```bash
pip install -r requirements.txt
python data/generate_sample_data.py
python run_local.py --raw data/raw --out output
pytest -q
```

### Option 3: AWS Glue
See the steps at the top of `glue/glue_job.py`: zip `src/etl`, upload to S3, create a Glue job with `--extra-py-files`, then crawl `silver/` and `gold/` and query with Athena.

## Example data quality report
```
[PASS] not_empty: ...
[PASS] order_id_unique: ...
[PASS] order_id_not_null: ...
[PASS] quantity_positive: ...
[PASS] reject_ratio_ok: ...
[PASS] row_reconciliation: raw = valid + rejected + duplicates
```
*(Replace this with the real output from your run.)*

## What I would add next
- Delta Lake MERGE for incremental (upsert) loads instead of full overwrite
- Orchestration with Databricks Workflows or AWS Step Functions
- CI with GitHub Actions running `pytest`
