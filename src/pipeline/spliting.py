from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def split_valid_rejected(df: DataFrame):
    rejection_reason = (
        F.when(
            F.col("is_valid_ip") == False,
            F.lit("invalid_ip")
        )
        .when(
            F.col("is_valid_timestamp") == False,
            F.lit("invalid_timestamp")
        )
        .when(
            F.col("is_valid_request") == False,
            F.lit("malformed_request")
        )
        .when(
            F.col("is_valid_status_code") == False,
            F.lit("invalid_status_code")
        )
        .when(
            F.col("is_valid_response_size") == False,
            F.lit("invalid_response_size")
        )
    )

    rejected_df = (
        df
        .filter(
            ~(
                F.col("is_valid_ip")
                & F.col("is_valid_timestamp")
                & F.col("is_valid_request")
                & F.col("is_valid_status_code")
                & F.col("is_valid_response_size")
            )
        )
        .select(
            F.col("value").alias("raw_log"),
            rejection_reason.alias("rejection_reason"),
            F.current_timestamp().alias("processing_timestamp")
        )
    )

    valid_df = (
        df
        .filter(
            F.col("is_valid_ip")
            & F.col("is_valid_timestamp")
            & F.col("is_valid_request")
            & F.col("is_valid_status_code")
            & F.col("is_valid_response_size")
        )
    )

    return valid_df, rejected_df
