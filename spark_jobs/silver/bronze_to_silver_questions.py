from pyspark.sql import functions as F
from pyspark.sql.window import Window
from glob import glob
from src.utils.spark_session import create_spark_session
from src.utils.data_quality import *

output_path = "data/silver/questions"

def main():
    spark = create_spark_session("BronzeToSilverQuestions")

    bronze_files = glob("data/bronze/questions/*.json")
    df_raw = (
        spark.read
        .option("multiline", True)
        .json(bronze_files)
    )

    df_silver = df_raw.select(
        "question_id",
        "title",
        "link",
        "content_license",
        "is_answered",
        "view_count",
        "answer_count",
        "score",
        "tags",
        F.from_unixtime("creation_date").cast("timestamp").alias("creation_date"),
        F.from_unixtime("last_activity_date").cast("timestamp").alias("last_activity_date"),
        F.from_unixtime("last_edit_date").cast("timestamp").alias("last_edit_date"),
        F.from_unixtime("closed_date").cast("timestamp").alias("closed_date"),
        F.to_date(F.from_unixtime("creation_date")).alias("date"),
        "closed_reason",
        "accepted_answer_id",
        F.col("owner.account_id").alias("owner_account_id"),
        F.col("owner.user_id").alias("owner_user_id"),
        F.col("owner.reputation").alias("owner_reputation"),
        F.col("owner.user_type").alias("owner_user_type"),
        F.col("owner.accept_rate").alias("owner_accept_rate"),
        F.col("owner.display_name").alias("owner_display_name"),
        F.col("owner.profile_image").alias("owner_profile_image"),
        F.col("owner.link").alias("owner_link"),
    )

    window_spec = Window.partitionBy("question_id").orderBy(
        F.col("last_activity_date").desc_nulls_last(),
        F.col("creation_date").desc_nulls_last()
    )


    df_silver = (
        df_silver
        .dropna(subset=["question_id"])
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    df_silver.printSchema()
    df_silver.show(5, truncate=False)

    check_not_null(df_silver, "question_id")
    check_duplicates(df_silver, "question_id")
    check_row_count(df_silver, min_rows=1)
    check_valid_timestamp(df_silver, "creation_date")

    df_silver.write.mode("overwrite").partitionBy("date").parquet(output_path)    
    print(f"Saved silver questions to {output_path}")


if __name__ == "__main__":
    main()