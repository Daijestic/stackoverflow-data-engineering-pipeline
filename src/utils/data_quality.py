from pyspark.sql import functions as F

def check_not_null(df, column_name):
    null_count = df.filter(F.col(column_name).isNull()).count()
    if null_count > 0:
        raise ValueError(f"column '{column_name}' contains {null_count} NULL values")
    return True

def check_duplicates(df, key_col):
    duplicates_count = df.groupBy(key_col) \
                            .count() \
                            .filter(F.col("count") > 1) \
                            .count()
    if duplicates_count:
        raise ValueError(f"column '{key_col}' contains {duplicates_count} duplicate keys")
    return True

def check_row_count(df, min_rows=1):
    sample_count = df.limit(min_rows).count()

    if sample_count < min_rows:
        raise ValueError(
            f"row count is less than expected minimum ({min_rows})"
        )
    return True

def check_valid_timestamp(df, column_name):
    null_count = df.filter(F.col(column_name).isNull()).count()
    future_count = df.filter(F.col(column_name) > F.current_timestamp()).count()
    if null_count > 0 or future_count > 0:
        raise ValueError(f"column '{column_name}' has {null_count} NULL values and {future_count} future timestamps")
    return True
