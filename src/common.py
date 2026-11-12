"""Shared configuration for the CS 236 Phase 1 pipeline.

Holds file paths, the Spark session factory, and the CSV reader options so
that eda.py and process.py read the raw data in exactly the same way.
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "CS236-Fall26-Project-Dataset"
NYC_RAW = DATA_DIR / "nyc_311_800k.csv"
LA_RAW = DATA_DIR / "la_311_800k.csv"

OUTPUT_DIR = PROJECT_ROOT / "output"
EDA_DIR = OUTPUT_DIR / "eda"
FIG_DIR = EDA_DIR / "figures"
PROCESSED_DIR = OUTPUT_DIR / "processed"

# Both files are RFC-4180 style CSVs: every field is double-quoted and some
# free-text fields (e.g. NYC resolution_description) contain commas/newlines.
# multiLine + escape='"' makes Spark parse those correctly. Everything is read
# as string first (inferSchema=False) so that we control type conversion and
# can count values that fail to parse instead of silently getting nulls.
CSV_OPTIONS = {"header": True, "multiLine": True, "escape": '"', "inferSchema": False}


def get_spark(app_name: str) -> SparkSession:
    """Create a local Spark session.

    The driver is bound to 127.0.0.1 because on macOS Spark otherwise tries to
    bind to the machine's hostname and can fail with a BindException.
    """
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "8g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_raw(spark: SparkSession, path: Path):
    return spark.read.options(**CSV_OPTIONS).csv(str(path))
