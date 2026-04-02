from pyspark.sql import functions as F
from glob import glob
from src.utils.spark_session import create_spark_session
from pyspark.sql.window import Window
from src.utils.data_quality import *


output_path = "data/silver/answers"

def main():
    spark = create_spark_session("BronzeToSilverAnswers")

    bronze_files = glob("data/bronze/answers/*.json")
    df_raw = (
        spark.read
        .option("multiline", True)
        .json(bronze_files)
    )

    df_silver = df_raw.select(
        "answer_id",
        "question_id",
        F.from_unixtime("creation_date").cast("timestamp").alias("creation_date"),
        "score",
        "is_accepted",
        F.col("owner.user_id").alias("owner_user_id"),
        F.col("owner.display_name").alias("owner_display_name"),
        F.col("owner.reputation").alias("owner_reputation"),
        F.to_date(
            F.from_unixtime("creation_date")
        ).alias("date"),
        F.from_unixtime("last_activity_date").cast("timestamp").alias("last_activity_date")
    )

    window_spec = Window.partitionBy("answer_id").orderBy(
        F.col("last_activity_date").desc_nulls_last(),
        F.col("creation_date").desc_nulls_last()
    )
    
    df_silver = (
        df_silver
        .dropna(subset=["answer_id"])
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    df_silver.printSchema()
    df_silver.show(5, truncate=False)

    check_not_null(df_silver, "answer_id")
    check_duplicates(df_silver, "answer_id")
    check_not_null(df_silver, "question_id")
    check_row_count(df_silver, min_rows=1)
    check_valid_timestamp(df_silver, "creation_date")

    df_silver.write.mode("overwrite").partitionBy("date").parquet(output_path)
    print(f"Saved silver answers to {output_path}")


if __name__ == "__main__":
    main()