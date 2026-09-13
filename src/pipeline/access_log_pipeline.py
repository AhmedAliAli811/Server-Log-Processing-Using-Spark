from pyspark.sql import SparkSession

from src.pipeline.parsing import parse_access_logs
from src.pipeline.validation import validate_records
from src.pipeline.spliting import split_valid_rejected
from src.pipeline.transformation import transform_valid_records


RAW_PATH = "hdfs://namenode:9000/log-processing/raw/access_logs/access.log"
PROCESSED_PATH = "hdfs://namenode:9000/log-processing/processed/access_logs"
REJECTED_PATH = "hdfs://namenode:9000/log-processing/rejected"


def run_pipeline(
    spark: SparkSession,
    raw_path: str = RAW_PATH,
    processed_path: str = PROCESSED_PATH,
    rejected_path: str = REJECTED_PATH,
):   
    raw_df = spark.read.text(raw_path)

    parsed_df = parse_access_logs(raw_df)

    validated_df = validate_records(parsed_df)

    valid_df , rejected_df = split_valid_rejected(validated_df)

    transformed_df = transform_valid_records(valid_df)

    (
        transformed_df.write
        .mode("overwrite")
        .partitionBy("year" , "month")
        .parquet(processed_path)
    ) 

    (
        rejected_df.write
        .mode("overwrite")
        .parquet(rejected_path)
    ) 

if __name__ == "__main__":

    spark = (
    SparkSession.builder
    .appName("WebServerLogProcessing")
    .master("local[4]")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
    )
    run_pipeline(spark)

    spark.stop()
