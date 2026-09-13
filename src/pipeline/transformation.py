from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform_valid_records(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "timestamp",
            F.to_timestamp(
                F.col("timestamp"),
                "dd/MMM/yyyy:HH:mm:ss Z"
            )
        )
        .withColumn(
            "status_code",
            F.col("status_code").cast("int")
        )
        .withColumn(
            "response_size",
            F.col("response_size").cast("long")
        )
        .withColumn(
            "year", 
            F.year("timestamp")
        )
        .withColumn(
            "month", 
            F.month("timestamp")
        )
        .select(
            "ip",
            "timestamp",
            "request_method",
            "request_url",
            "http_protocol",
            "status_code",
            "response_size",
            "referrer",
            "user_agent",
            "year",
            "month"
        )
    )