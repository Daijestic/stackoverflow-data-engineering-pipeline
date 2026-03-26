from pyspark.sql import functions as F
from src.utils.spark_session import create_spark_session

output_path = "data/gold/fact_answers"

def main():
    spark = create_spark_session("SilverToGoldFactAnswers")

    df_silver = spark.read.parquet("data/silver/answers")

    df_fact = df_silver.select(
        F.col("answer_id"),
        F.col("question_id"),
        F.col("creation_date"),
        F.col("last_activity_date"),
        F.col("date"),
        F.col("score"),
        F.col("is_accepted"),
        F.col("owner_user_id"),
        F.col("owner_display_name"),
        F.col("owner_reputation"),
    )

    df_fact.printSchema()
    df_fact.show(5, truncate=False)

    df_fact.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(output_path)

    print(f"Saved fact answers to {output_path}")

if __name__ == "__main__":
    main()