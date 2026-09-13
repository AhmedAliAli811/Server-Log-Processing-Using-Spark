from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def parse_access_logs(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn("ip" , F.regexp_extract("value"  , r"^(\S+)" , 1))
        .withColumn("timestamp" , F.regexp_extract("value"  , r'\[([^\]]+)\]' , 1))
        .withColumn("request_method" , F.regexp_extract("value" , r'"(\S+)\s' , 1))
        .withColumn("request_url" , F.regexp_extract("value" , r'"\S+\s(.*?)\sHTTP/\S+"' , 1))
        .withColumn("http_protocol" , F.regexp_extract("value" , r'"[^"]*\s(HTTP/\S+)"' , 1))
        .withColumn("status_code" , F.regexp_extract("value" , r'"\s+(\S+)\s+\S+\s+"' , 1))
        .withColumn("response_size" , F.regexp_extract("value" , r'"\s+\S+\s+(\S+)\s+"' , 1))
        .withColumn("referrer" , F.regexp_extract("value" , r'"\s+\S+\s+\S+\s+"([^"]*)"\s+"' , 1))
        .withColumn("user_agent" , F.regexp_extract("value" , r'"\s+"([^"]*)"\s+"[^"]*"$' , 1))
    )