from pyspark.sql import SparkSession

def create_spark():
    spark = (
        SparkSession.builder
            .appName("local-pipeline")
            .master("local[*]")         # runs Spark locally using all CPU cores
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
