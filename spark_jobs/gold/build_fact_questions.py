from pyspark.sql import functions as F
from src.utils.spark_session import create_spark_session

output_path = "data/gold/fact_questions"

def main():
    spark = create_spark_session("BuildFactQuestions")

    # 1. Read Silver
    df_silver = spark.read.parquet("data/silver/questions")

    # 2. Select schema for Gold
    df_fact = df_silver.select(
        F.col("question_id"),
        F.col("title"),
        F.col("tags"),
        F.col("creation_date"),
        F.col("date"),
        F.col("score"),
        F.col("view_count"),
        F.col("answer_count"),
        F.col("is_answered"),
        F.col("accepted_answer_id")
    )

    df_fact.printSchema()
    df_fact.show(5, truncate=False)

    # 3. Write Gold
    df_fact.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(output_path)

    print(f"Saved fact questions to {output_path}")

if __name__ == "__main__":
    main()