# Web Server Log Processing Pipeline

## 1. Project Overview

This project implements a data processing pipeline for large-scale web server access logs.

The pipeline reads raw web server log files, stores them in HDFS, processes and validates the data using Apache Spark, and produces analytics-ready Parquet datasets partitioned by year and month.

The main goal is to build a practical Big Data processing pipeline that demonstrates:

* Distributed data processing using Apache Spark
* Working with semi-structured log data
* Parsing unstructured log records
* Data quality and validation
* Handling rejected records
* Data transformation
* Parquet-based data storage
* Partitioning strategies
* Spark performance considerations
* Basic analytical workloads

The pipeline intentionally ends at the Parquet layer. Data warehouses, Kafka, Airflow, and cloud infrastructure are considered possible future extensions rather than part of the current project scope.

---

## 2. Business Requirements

The processed web server logs should support the following analytical requirements:

### 2.1 Request Volume Over Time

Determine how many HTTP requests were received over time.

This can be analyzed at different time granularities, such as:

* Hour
* Day
* Month

---

### 2.2 HTTP Status Code Distribution

Analyze the distribution of HTTP response status codes.

Examples:

* `200` — Successful requests
* `301` — Redirects
* `404` — Not Found
* `500` — Server Error

This can help identify application errors, failed requests, and general server behavior.

---

### 2.3 Top Requested Endpoints

Identify the most frequently requested URLs/endpoints.

This can help determine:

* Most popular resources
* Frequently accessed endpoints
* Potentially problematic endpoints
* Traffic patterns

---

## 3. Data Source

The project uses a **Web Server Logs** dataset containing raw HTTP access log records.

Each record represents an HTTP request received by a web server.

Example raw record:

```text
54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /filter/... HTTP/1.1" 200 30577 "-" "Mozilla/5.0 ..." "-"
```

The original records are semi-structured and need to be parsed before they can be used for analytical processing.

---

## 4. Input Data Format

The raw data follows a web server access log format containing information such as:

* Client IP address
* Timestamp
* HTTP request
* HTTP method
* Requested URL
* HTTP protocol
* Response status code
* Response size
* Referrer
* User agent

A typical request section has the following structure:

```text
"GET /path HTTP/1.1"
```

This needs to be decomposed into:

```text
request_method = GET
request_url    = /path
http_protocol  = HTTP/1.1
```

---

## 5. Target Data Schema

After parsing, the main processed dataset will contain the following columns:

| Column           | Description                |
| ---------------- | -------------------------- |
| `ip`             | Client IP address          |
| `timestamp`      | Request timestamp          |
| `request_method` | HTTP request method        |
| `request_url`    | Requested URL/endpoint     |
| `http_protocol`  | HTTP protocol version      |
| `status_code`    | HTTP response status code  |
| `response_size`  | Size of the HTTP response  |
| `referrer`       | Referring URL              |
| `user_agent`     | Client/browser information |

Additional derived columns will be created for partitioning and analytics:

| Column  | Description                    |
| ------- | ------------------------------ |
| `year`  | Year extracted from timestamp  |
| `month` | Month extracted from timestamp |

---

## 6. Pipeline Architecture

The pipeline follows this high-level architecture:

```text
                    Raw Web Server Logs
                            │
                            ▼
                           HDFS
                            │
                            ▼
                         PySpark
                            │
                    ┌───────┴───────┐
                    ▼               ▼
                  Parse          Raw Data
                    │
                    ▼
              Data Validation
                    │
             ┌──────┴──────┐
             ▼             ▼
        Valid Records   Rejected Records
             │             │
             ▼             ▼
       Transformations   Rejection Data
             │             │
             ▼             ▼
      Partition by       Parquet
      year/month
             │
             ▼
          Parquet
```

### Pipeline Stages

1. Read raw log files from HDFS.
2. Parse each raw log record.
3. Extract structured fields.
4. Validate the parsed records.
5. Separate valid and rejected records.
6. Transform valid records into the target schema.
7. Add derived date fields.
8. Write valid records to partitioned Parquet.
9. Write rejected records to a separate Parquet dataset.
10. Use the processed data for analytical queries.

---

## 7. Data Quality Rules

Invalid records should not simply be discarded. They should be captured separately so that they can be investigated later.

### 7.1 Invalid Timestamp

A record is rejected if its timestamp cannot be parsed into a valid timestamp.

**Action:**

```text
Reject record
Reason: invalid_timestamp
```

---

### 7.2 Missing or Invalid IP

A record is rejected if the client IP address is missing or does not follow a valid IP format.

**Action:**

```text
Reject record
Reason: invalid_ip
```

---

### 7.3 Malformed HTTP Request

A record is rejected if the HTTP request cannot be correctly parsed.

For example, a request should contain:

```text
HTTP_METHOD URL HTTP_PROTOCOL
```

**Action:**

```text
Reject record
Reason: malformed_request
```

---

### 7.4 Invalid Status Code

The HTTP status code must be numeric.

For example:

```text
200
404
500
```

are valid numeric status codes.

A non-numeric value should be rejected.

**Action:**

```text
Reject record
Reason: invalid_status_code
```

---

### 7.5 Negative Response Size

Response size cannot be negative.

For example:

```text
30577
1024
0
```

are valid values.

A value such as:

```text
-100
```

is invalid.

**Action:**

```text
Reject record
Reason: negative_response_size
```

---

## 8. Rejected Records

Rejected records will be stored separately instead of being silently dropped.

The rejected dataset should preserve enough information to investigate the original record.

Target schema:

| Column                 | Description                             |
| ---------------------- | --------------------------------------- |
| `raw_log`              | Original unprocessed log record         |
| `rejection_reason`     | Reason why the record failed validation |
| `processing_timestamp` | Time when the record was processed      |

Example:

```text
raw_log:
54.36.149.41 - - [...]

rejection_reason:
invalid_status_code

processing_timestamp:
2026-09-08 18:30:00
```

This approach provides traceability and makes data quality problems observable.

---

## 9. Data Transformation

After validation, the pipeline transforms the raw records into a structured dataset.

### Parsing

The raw log line is parsed into individual fields.

Example:

```text
Raw:
54.36.149.41 - - [22/Jan/2019:03:56:14 +0330]
"GET /filter/... HTTP/1.1" 200 30577 "-" "Mozilla/5.0 ..." "-"
```

Becomes:

```text
ip              = 54.36.149.41
timestamp       = 2019-01-22 03:56:14
request_method  = GET
request_url     = /filter/...
http_protocol   = HTTP/1.1
status_code     = 200
response_size   = 30577
```

### Type Conversion

String values extracted from the raw log should be converted to appropriate data types.

Examples:

```text
timestamp      → Timestamp
status_code    → Integer
response_size  → Integer
```

### Derived Columns

The following fields are extracted from the timestamp:

```text
year
month
```

These fields will be used for partitioning.

---

## 10. Output Design

The primary output is a cleaned and analytics-ready Parquet dataset.

Conceptually:

```text
processed/
└── access_logs/
    ├── year=2019/
    │   ├── month=01/
    │   │   ├── part-*.parquet
    │   │   └── ...
    │   ├── month=02/
    │   │   └── ...
    │   └── ...
    └── ...
```

Rejected records are stored separately:

```text
processed/
└── rejected/
    └── part-*.parquet
```

---

## 11. Partitioning Strategy

The processed access logs will be partitioned by:

```text
year
month
```

In PySpark:

```python
df.write \
    .partitionBy("year", "month") \
    .parquet(output_path)
```

The resulting directory structure will look like:

```text
year=2019/
    month=01/
    month=02/
    month=03/
```

### Why Partition by Year and Month?

The dataset is time-based, and many analytical queries will likely filter records by date.

Partitioning by year and month allows Spark to perform partition pruning when a query filters on these columns.

For example:

```sql
SELECT *
FROM access_logs
WHERE year = 2019
  AND month = 1;
```

Spark can avoid scanning unrelated partitions.

### Why Not Partition by Day?

Partitioning by year/month/day could create a much larger number of directories and files, especially for high-volume datasets.

For this project, year/month provides a reasonable balance between:

* Query filtering
* Number of partitions
* File-system overhead
* Dataset size

---

## 12. Analytics Requirements

The processed Parquet dataset should support the following analytical queries.

### Request Volume Over Time

Example analytical question:

```text
How many requests were received each day?
```

Possible output:

```text
date        request_count
----------  -------------
2019-01-22  125430
2019-01-23  138210
...
```

---

### HTTP Status Distribution

Example:

```text
status_code    request_count
-----------    -------------
200            ...
404            ...
301            ...
500            ...
```

---

### Top Endpoints

Example:

```text
request_url          request_count
------------------   -------------
/products            ...
/filter              ...
/category            ...
```

These analytical queries can be implemented using Spark SQL or the PySpark DataFrame API.

---

## 13. Technology Stack

| Technology      | Purpose                           |
| --------------- | --------------------------------- |
| Python          | Pipeline implementation           |
| PySpark         | Distributed data processing       |
| Apache Spark    | Big Data processing engine        |
| HDFS            | Distributed storage for raw data  |
| Parquet         | Processed columnar storage format |
| SQL / Spark SQL | Analytical queries                |
| Git             | Version control                   |

---

## 14. Project Structure

The project is expected to follow a structure similar to:

```text
web-server-log-pipeline/
│
├── data/
│   └── sample/
│
├── src/
│   ├── ingestion/
│   ├── parsing/
│   ├── validation/
│   ├── transformation/
│   └── analytics/
│
├── tests/
│
├── notebooks/
│
├── config/
│
├── README.md
│
└── requirements.txt
```

The exact structure may be adjusted during implementation.

---

## 15. Execution Flow

The complete execution flow is:

```text
1. Obtain raw web server logs
          ↓
2. Upload raw logs to HDFS
          ↓
3. Start Spark application
          ↓
4. Read raw logs from HDFS
          ↓
5. Parse raw log records
          ↓
6. Apply data quality rules
          ↓
7. Separate valid/rejected records
          ↓
8. Transform valid records
          ↓
9. Extract year/month
          ↓
10. Write valid data as Parquet
          ↓
11. Write rejected data as Parquet
          ↓
12. Run analytical queries
```

---

## 16. Performance Optimization

Performance optimization will be considered during implementation.

Areas to investigate include:

### Repartitioning and Coalescing

The pipeline should avoid creating unnecessarily large numbers of small files.

Spark operations such as:

```python
repartition()
```

and:

```python
coalesce()
```

may be used where appropriate.

---

### Shuffle Awareness

Operations such as:

* `groupBy`
* `join`
* `distinct`
* `orderBy`
* `repartition`

can introduce shuffles.

The Spark UI should be used to inspect expensive stages and understand where shuffling occurs.

---

### Parquet

Parquet provides columnar storage and allows Spark to read only the required columns for analytical queries.

---

### Partition Pruning

Partitioning by:

```text
year / month
```

allows Spark to avoid scanning unrelated partitions when filters are applied to partition columns.

---

## 17. Testing and Validation

The pipeline should be tested against different types of input records.

### Valid Record

A correctly formatted web server log should be successfully parsed and written to the processed dataset.

### Invalid Timestamp

Expected result:

```text
Rejected
Reason: invalid_timestamp
```

### Invalid IP

Expected result:

```text
Rejected
Reason: invalid_ip
```

### Malformed Request

Expected result:

```text
Rejected
Reason: malformed_request
```

### Invalid Status Code

Expected result:

```text
Rejected
Reason: invalid_status_code
```

### Negative Response Size

Expected result:

```text
Rejected
Reason: negative_response_size
```

---

## 18. Pipeline Metrics

The pipeline should track basic processing metrics.

Examples:

```text
Total records read
Valid records
Rejected records
Records written
```

A useful summary could look like:

```text
Records Read:       10,000,000
Valid Records:       9,850,000
Rejected Records:      150,000
```

The rejection rate can also be calculated:

```text
rejection_rate =
    rejected_records / total_records
```

These metrics help evaluate data quality and pipeline behavior.

---

## 19. Future Improvements

The current project intentionally focuses on the Spark processing layer and ends at Parquet.

Possible future extensions include:

* Apache Kafka for real-time log ingestion
* Apache Airflow for workflow orchestration
* AWS S3 for cloud object storage
* Data Lake architecture
* Data Warehouse integration
* dbt transformations
* Lakehouse architecture
* Real-time Spark Structured Streaming
* Monitoring and alerting
* Automated data-quality checks

These technologies are outside the current project's core scope.

---

## 20. Project Goals

By completing this project, the following Data Engineering concepts should be demonstrated:

* Handling raw semi-structured data
* Distributed storage with HDFS
* Distributed processing with Apache Spark
* PySpark DataFrame operations
* Parsing complex strings
* Data validation
* Error/rejection handling
* Data transformation
* Date-based partitioning
* Parquet storage
* Spark SQL
* Aggregations
* Window functions where applicable
* Shuffle analysis
* Spark performance optimization
* Data quality monitoring
