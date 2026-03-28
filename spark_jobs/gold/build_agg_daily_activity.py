from pyspark.sql import functions as F
from src.utils.spark_session import create_spark_session

output_path = "data/gold/agg_daily_activity"

def main():
    spark = create_spark_session("BuildAggDailyActivity")

    df_answers_fact = spark.read.parquet("data/gold/fact_answers")
    df_questions_fact = spark.read.parquet("data/gold/fact_questions")

    df_answers_daily = df_answers_fact.groupBy("date").agg(
        F.count("*").alias("total_answers"),
        F.sum(
            F.when(F.col("is_accepted"), 1).otherwise(0)
        ).alias("accepted_answers")
    )

    df_questions_daily = df_questions_fact.groupBy("date").agg(
        F.count("*").alias("total_questions")
    )

    df_result = df_answers_daily.alias("a").join(
        df_questions_daily.alias("q"),
        F.col("a.date") == F.col("q.date"),
        how="full_outer"
    ).select(
        F.coalesce(F.col("a.date"), F.col("q.date")).alias("date"),
        F.col("total_answers"),
        F.col("accepted_answers"),
        F.col("total_questions")
    ).fillna({
        "total_answers": 0,
        "accepted_answers": 0,
        "total_questions": 0
    }).withColumn(
        "accepted_answer_rate_daily",
        F.when(
            F.col("total_answers") > 0,
            F.col("accepted_answers") / F.col("total_answers")
        ).otherwise(F.lit(0.0))
    )

    df_result.printSchema()
    df_result.show(5, truncate=False)

    df_result.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"Saved agg daily activity to {output_path}")

if __name__ == "__main__":
    main()