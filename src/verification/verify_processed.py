from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VerifyProcessedOutput")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

df = spark.read.parquet(
    "hdfs://namenode:9000/log-processing/processed/access_logs"
)

df.printSchema()

spark.stop()
