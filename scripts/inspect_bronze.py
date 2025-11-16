import os
from scripts.spark_session import create_spark

spark = create_spark()

# Project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")

# Choose a table to inspect
table = "customers"   # change to products/orders etc.

df = spark.read.parquet(os.path.join(BRONZE_PATH, f"{table}.parquet"))

print("\n===== Schema =====")
df.printSchema()

print("\n===== Sample Data =====")
df.show(20, truncate=False)

print("\n===== Row Count =====")
print(df.count())


