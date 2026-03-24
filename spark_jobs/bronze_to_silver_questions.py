from pyspark.sql import functions as F
from glob import glob
from src.utils.spark_session import create_spark_session

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

    df_silver = (
        df_silver
        .dropna(subset=["question_id"])
        .dropDuplicates(["question_id"])
    )

    df_silver.printSchema()
    df_silver.show(5, truncate=False)

    df_silver.write.mode("overwrite").parquet(output_path)    
    print(f"Saved silver questions to {output_path}")


if __name__ == "__main__":
    main()