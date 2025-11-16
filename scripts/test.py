import os
from scripts.spark_session import create_spark

spark = create_spark()

# Resolve project root (one level above scripts/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

# Build correct path
parquet_path = os.path.join(PROJECT_ROOT, "data", "bronze", "payments.parquet")

df = spark.read.parquet(parquet_path)
df.show()