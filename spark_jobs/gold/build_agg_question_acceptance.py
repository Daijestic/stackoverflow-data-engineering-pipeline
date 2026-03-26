from pyspark.sql import functions as F
from src.utils.spark_session import create_spark_session

output_path = "data/gold/agg_question_acceptance"

def main():
    spark = create_spark_session("BuildAggQuestionAcceptance")

    df_fact = spark.read.parquet("data/gold/fact_answers")

    df_agg = df_fact.groupBy("question_id").agg(
        F.count("*").alias("total_answers"),
        F.sum(
            F.when(F.col("is_accepted"), 1).otherwise(0)
        ).alias("accepted_answers")
    )
    
    df_result = df_agg.withColumn(
        "accepted_answer_rate",
        F.col("accepted_answers") / F.col("total_answers")
    )

    df_result.printSchema()
    df_result.show(5, truncate=False)

    df_result.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"Saved agg question acceptance to {output_path}")


if __name__ == "__main__":
    main()