from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def validate_ip(df: DataFrame) -> DataFrame:

    ip_pattern = (
        r"^(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\."
        r"(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\."
        r"(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\."
        r"(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])$"
    )
    

    return df.withColumn(
        "is_valid_ip",
        F.col("ip").rlike(ip_pattern)
    )

def validate_timestamp(df: DataFrame) -> DataFrame:

    return df.withColumn(
        "is_valid_timestamp",
        F.expr(
            """
            try_to_timestamp(
                timestamp,
                'dd/MMM/yyyy:HH:mm:ss Z'
            ) IS NOT NULL
            """
        )
    )

def validate_request(df:DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "is_valid_request",
            (
                F.col("request_method").isin(
                    "GET",
                    "POST",
                    "PUT",
                    "DELETE",
                    "PATCH",
                    "HEAD",
                    "OPTIONS"
                )
                & F.col("request_url").isNotNull()
                & (F.col("request_url") != "")
                & F.col("http_protocol").rlike(r"^HTTP/\d+\.\d+$")
            )
        )
    )

def validate_status_code(df: DataFrame) -> DataFrame:

    return df.withColumn(
        "is_valid_status_code",
        F.col("status_code").cast("int").between(200, 599)
    )


def validate_response_size(df: DataFrame) -> DataFrame:

    return df.withColumn(
        "is_valid_response_size",
        F.col("response_size").cast("long") >= 0
    )

def validate_records(df: DataFrame) -> DataFrame:

    df = validate_ip(df)
    df = validate_timestamp(df)
    df = validate_request(df)
    df = validate_status_code(df)
    df = validate_response_size(df)

    return df