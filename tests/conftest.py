import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession. Spark tests are skipped if PySpark or Java is missing."""
    pytest.importorskip("pyspark")
    if not shutil.which("java"):
        pytest.skip("Java not found; install a JDK (17) to run Spark tests")
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("etl-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()
