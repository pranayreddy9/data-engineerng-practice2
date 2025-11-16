import os
from pyspark.sql.functions import col, trim, lower, initcap, current_timestamp
from scripts.spark_session import create_spark

spark = create_spark()

# Path setup
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
BRONZE = os.path.join(PROJECT_ROOT, "data", "bronze")
SILVER = os.path.join(PROJECT_ROOT, "data", "silver")

# Load bronze customers
df = spark.read.parquet(os.path.join(BRONZE, "customers.parquet"))

# 🟦 Silver transformations
df_clean = (
    df.dropDuplicates(["customer_id"])
      .withColumn("customer_id", col("customer_id").cast("int"))
      .withColumn("first_name", initcap(trim(col("first_name"))))
      .withColumn("last_name", initcap(trim(col("last_name"))))
      .withColumn("email", lower(trim(col("email"))))
      .withColumn("country", initcap(trim(col("country"))))
      .withColumn("processed_at", current_timestamp())
)

# Write to Silver
df_clean.write.mode("overwrite").parquet(os.path.join(SILVER, "customers.parquet"))

print("Silver customers created at:", os.path.join(SILVER, "customers.parquet"))
