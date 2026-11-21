"""AWS Glue (4.0 / 5.0) entry point for the same pipeline.

Deploy:
  1. Zip the package:          cd src && zip -r ../etl.zip etl
  2. Upload etl.zip and this file to S3.
  3. Create a Glue Spark job with:
       Script path            s3://<bucket>/scripts/glue_job.py
       --extra-py-files       s3://<bucket>/scripts/etl.zip
       --RAW_PATH             s3://<bucket>/raw
       --OUT_PATH             s3://<bucket>/curated
  4. Point a Glue Crawler at OUT_PATH/silver and OUT_PATH/gold, then query with Athena.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from etl.pipeline import run

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "OUT_PATH"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

metrics = run(spark, args["RAW_PATH"], args["OUT_PATH"], fail_on_dq=True)
print("Run metrics:", metrics)

job.commit()
