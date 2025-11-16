import os
from pyspark.sql.functions import col, trim, lower, to_timestamp
from scripts.spark_session import create_spark
from scripts.silver_config import silver_config

spark = create_spark()

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")
SILVER_PATH = os.path.join(PROJECT_ROOT, "data", "silver")
os.makedirs(SILVER_PATH, exist_ok=True)


def apply_transforms(df, config):
    # 1. Deduplicate based on primary key
    df = df.dropDuplicates(config.get("pk", []))

    # 2. Cast columns
    for column, dtype in config.get("cast", {}).items():
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(dtype))

    # 3. Trim string columns
    for column in config.get("trim", []):
        if column in df.columns:
            df = df.withColumn(column, trim(col(column)))

    # 4. Lowercase columns
    for column in config.get("lower", []):
        if column in df.columns:
            df = df.withColumn(column, lower(col(column)))

    # 5. Convert timestamps
    for column in config.get("timestamp", []):
        if column in df.columns:
            df = df.withColumn(column, to_timestamp(col(column)))

    # 6. Final cleanup: replace nulls
    df = df.fillna("unknown")

    return df


def process_table(table_name):
    print(f"\n=== Processing Silver table: {table_name} ===")

    bronze_file = os.path.join(BRONZE_PATH, f"{table_name}.parquet")
    silver_file = os.path.join(SILVER_PATH, f"{table_name}.parquet")

    df = spark.read.parquet(bronze_file)

    config = silver_config.get(table_name, {})
    df_clean = apply_transforms(df, config)

    df_clean.write.mode("overwrite").parquet(silver_file)

    print(f"✓ Silver table created: {silver_file}")


if __name__ == "__main__":
    for table in silver_config.keys():
        process_table(table)
