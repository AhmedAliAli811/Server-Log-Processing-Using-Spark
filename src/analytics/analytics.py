from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

import pyspark.sql.functions as F


VALID_INPUT_PATH = "hdfs://namenode:9000/log-processing/processed/access_logs"
REJECTED_INPUT_PATH = "hdfs://namenode:9000/log-processing/rejected"

OUTPUT_PATH = "hdfs://namenode:9000/log-processing/analytics"


def hourly_requests_traffic (spark:SparkSession, df:DataFrame):

    output_path = OUTPUT_PATH + "/request_volume_hourly"

    hourly_traffic = (
        df.groupBy(F.date_trunc("hour" , df.timestamp).alias("hour"))
        .count()
        .orderBy("hour")
    )

    hourly_traffic.coalesce(1).write.mode("overwrite").parquet(output_path)

def http_Status_count(spark:SparkSession, df:DataFrame):
    status_code_output_path = OUTPUT_PATH + "/status_code_counts"
    status_class_output_path = OUTPUT_PATH + "/status_class_counts"

    status_counts = (
        df.groupBy("status_code")
        .agg(F.count("*").alias("request_count"))
        .orderBy(F.col("request_count").desc())
    )

    status_counts.coalesce(1).write.mode("overwrite").parquet(status_code_output_path)

    status_class = (
        df.withColumn(
            "status_class" , 
            F.when((F.col("status_code") >= 200) & (F.col("status_code") < 300), "2xx")
            .when((F.col("status_code") >= 300) & (F.col("status_code") < 400), "3xx")
            .when((F.col("status_code") >= 400) & (F.col("status_code") < 500), "4xx")
            .when((F.col("status_code") >= 500) & (F.col("status_code") < 600), "5xx")
            .otherwise("other")
        )
    )
    class_counts = (
        status_class.groupBy("status_class")
          .agg(F.count("*").alias("request_count"))
          .orderBy(F.col("request_count").desc())
    )
    class_counts.coalesce(1).write.mode("overwrite").parquet(status_class_output_path)


def top_endpoints (spark:SparkSession, df:DataFrame):

    output_path = OUTPUT_PATH + "/top_endpoints"

    endpoint_counts = (
        df.groupBy("request_url")
          .agg(F.count("*").alias("request_count"))
          .orderBy(F.col("request_count").desc())
    )

    endpoint_counts.coalesce(1).write.mode("overwrite").parquet(output_path)

def most_rejected_reasons (spark:SparkSession , df:DataFrame):
    output_path = OUTPUT_PATH + "/top_rejected_reasons"

    reason_counts = (
        df.groupBy("rejection_reason")
          .agg(F.count("*").alias("reason_count"))
          .orderBy(F.col("reason_count").desc())
    )

    reason_counts.coalesce(1).write.mode("overwrite").parquet(output_path)

if __name__== "__main__":
    spark = (
            SparkSession.builder
            .appName("Analytics")
            .getOrCreate()
    )

    valid_df = spark.read.parquet(VALID_INPUT_PATH)
    hourly_requests_traffic(spark , valid_df)
    http_Status_count(spark , valid_df) 
    top_endpoints(spark , valid_df)
    

    rejected_df = spark.read.parquet(REJECTED_INPUT_PATH)
    most_rejected_reasons(spark , rejected_df)
    
    spark.stop()