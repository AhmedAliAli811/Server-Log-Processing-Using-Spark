from pyspark.sql import SparkSession, Row
from pyspark.testing.utils import assertDataFrameEqual
from pathlib import Path
from src.pipeline.access_log_pipeline import run_pipeline

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    LongType,
)

from src.pipeline.parsing import parse_access_logs
from src.pipeline.spliting import split_valid_rejected
from src.pipeline.transformation import transform_valid_records
from src.pipeline.validation import validate_records



def test_parse_validate_split_transform():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_access_log_pipeline")
        .getOrCreate()
    )

    try:
        test_rows = [
            Row(
                value='54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /test HTTP/1.1" 200 100 "-" "Mozilla" "-"'
            ),
            Row(
                value='999.999.999.999 - - [22/Jan/2019:03:56:14 +0330] "GET /test HTTP/1.1" 200 100 "-" "Mozilla" "-"'
            ),
            Row(
                value='54.36.149.41 - - [invalid_timestamp] "GET /test HTTP/1.1" 200 100 "-" "Mozilla" "-"'
            ),
            Row(
                value='54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "INVALID /test HTTP/1.1" 200 100 "-" "Mozilla" "-"'
            ),
            Row(
                value='54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /test HTTP/1.1" 700 100 "-" "Mozilla" "-"'
            ),
            Row(
                value='54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /test HTTP/1.1" 200 -100 "-" "Mozilla" "-"'
            ),
        ]

        test_df = spark.createDataFrame(test_rows)

        parsed_df = parse_access_logs(test_df)
        validated_df = validate_records(parsed_df)
        valid_df, rejected_df = split_valid_rejected(validated_df)
        transformed_df = transform_valid_records(valid_df)


        expected_valid_df = spark.createDataFrame(
            [
                Row(
                    value='54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /test HTTP/1.1" 200 100 "-" "Mozilla" "-"',
                    ip="54.36.149.41",
                    timestamp="22/Jan/2019:03:56:14 +0330",
                    request_method="GET",
                    request_url="/test",
                    http_protocol="HTTP/1.1",
                    status_code="200",
                    response_size="100",
                    referrer="-",
                    user_agent="Mozilla",
                    is_valid_ip=True,
                    is_valid_timestamp=True,
                    is_valid_request=True,
                    is_valid_status_code=True,
                    is_valid_response_size=True,
                )
            ]
        )

        actual_valid_df = valid_df.select(
            "value",
            "ip",
            "timestamp",
            "request_method",
            "request_url",
            "http_protocol",
            "status_code",
            "response_size",
            "referrer",
            "user_agent",
            "is_valid_ip",
            "is_valid_timestamp",
            "is_valid_request",
            "is_valid_status_code",
            "is_valid_response_size",
        )

        assertDataFrameEqual(
            actual_valid_df,
            expected_valid_df,
        )

        expected_rejected_df = spark.createDataFrame(
            [
                Row(
                    raw_log='999.999.999.999 - - [22/Jan/2019:03:56:14 +0330] "GET /test HTTP/1.1" 200 100 "-" "Mozilla" "-"',
                    rejection_reason="invalid_ip",
                ),
                Row(
                    raw_log='54.36.149.41 - - [invalid_timestamp] "GET /test HTTP/1.1" 200 100 "-" "Mozilla" "-"',
                    rejection_reason="invalid_timestamp",
                ),
                Row(
                    raw_log='54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "INVALID /test HTTP/1.1" 200 100 "-" "Mozilla" "-"',
                    rejection_reason="malformed_request",
                ),
                Row(
                    raw_log='54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /test HTTP/1.1" 700 100 "-" "Mozilla" "-"',
                    rejection_reason="invalid_status_code",
                ),
                Row(
                    raw_log='54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /test HTTP/1.1" 200 -100 "-" "Mozilla" "-"',
                    rejection_reason="invalid_response_size",
                ),
            ]
        )

        actual_rejected_df = rejected_df.select(
            "raw_log",
            "rejection_reason",
        )

        assertDataFrameEqual(
            actual_rejected_df,
            expected_rejected_df,
        )

        expected_transformed_schema = StructType([
            StructField("ip", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("request_method", StringType(), True),
            StructField("request_url", StringType(), True),
            StructField("http_protocol", StringType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("response_size", LongType(), True),
            StructField("referrer", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
        ])

        expected_transformed_df = spark.createDataFrame(
            [
                (
                    "54.36.149.41",
                    transformed_df.first()["timestamp"],
                    "GET",
                    "/test",
                    "HTTP/1.1",
                    200,
                    100,
                    "-",
                    "Mozilla",
                    2019,
                    1,
                )
            ],
            schema=expected_transformed_schema,
        )

        assertDataFrameEqual(
            transformed_df,
            expected_transformed_df,
        )

    finally:
        spark.stop()

def test_run_pipeline_writes_parquet(tmp_path):
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_run_pipeline")
        .getOrCreate()
    )

    try:
        raw_path = str(tmp_path / "raw")
        processed_path = str(tmp_path / "processed")
        rejected_path = str(tmp_path / "rejected")

        test_rows = [
            Row(
                value='54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /test HTTP/1.1" 200 100 "-" "Mozilla" "-"'
            ),
            Row(
                value='999.999.999.999 - - [22/Jan/2019:03:56:14 +0330] "GET /test HTTP/1.1" 200 100 "-" "Mozilla" "-"'
            ),
        ]

        spark.createDataFrame(test_rows).write.mode("overwrite").text(raw_path)

        run_pipeline(
            spark,
            raw_path=raw_path,
            processed_path=processed_path,
            rejected_path=rejected_path,
        )

        processed_df = spark.read.parquet(processed_path)
        rejected_df = spark.read.parquet(rejected_path)

        assert processed_df.count() == 1
        assert rejected_df.count() == 1

        assert processed_df.first()["ip"] == "54.36.149.41"
        assert processed_df.first()["status_code"] == 200

        assert rejected_df.first()["rejection_reason"] == "invalid_ip"

        processed_path_obj = Path(processed_path)

        partition_dirs = [
            path.name
            for path in processed_path_obj.iterdir()
            if path.is_dir()
        ]

        assert "year=2019" in partition_dirs

        month_dir = processed_path_obj / "year=2019"

        assert any(
            path.name == "month=1"
            for path in month_dir.iterdir()
            if path.is_dir()
        )

    finally:
        spark.stop()