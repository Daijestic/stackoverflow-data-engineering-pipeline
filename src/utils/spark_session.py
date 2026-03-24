from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "StackOverflowSilverJob") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
        .config("spark.sql.warehouse.dir", "D:/spark-warehouse")
        .getOrCreate()
    )
    return spark