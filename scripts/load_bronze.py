import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from scripts.spark_session import create_spark

# Create Spark session
spark = create_spark()

# Get project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

RAW_PATH = os.path.join(PROJECT_ROOT, "data", "raw")
BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")

os.makedirs(BRONZE_PATH, exist_ok=True)

tables = [
    "customers",
    "products",
    "orders",
    "payments",
    "order_items",
    "shipments",
    "returns"
]

for table in tables:
    print(f"\n=== Loading raw {table}.csv into Bronze ===")

    df = spark.read.csv(
        os.path.join(RAW_PATH, f"{table}.csv"),
        header=True,
        inferSchema=True
    )

    df = df.withColumn("ingest_time", current_timestamp())  # add bronze metadata

    df.write.mode("overwrite").parquet(
        os.path.join(BRONZE_PATH, f"{table}.parquet")
    )

    print(f" Bronze written → data/bronze/{table}.parquet")
