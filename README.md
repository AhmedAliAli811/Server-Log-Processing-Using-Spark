# Web Server Log Processing Pipeline

A local Big Data Engineering project for processing a large Nginx web server access-log dataset, applying data-quality validation, storing clean and rejected records in HDFS as Parquet, building an analytics layer with Apache Spark, and consuming the resulting datasets in Power BI.

## 1. Project Overview

The project demonstrates an end-to-end batch data pipeline for large-scale web server logs.

The pipeline takes raw Nginx access logs and performs:

- Raw-data ingestion into HDFS
- Log parsing and schema extraction
- Data-quality validation
- Separation of valid and rejected records
- Data type conversion and derived-column creation
- Parquet storage with Snappy compression
- Year/month partitioning
- Spark-based analytics
- Power BI visualization directly from HDFS through WebHDFS



## 2. Business Requirements

The analytics layer is designed to answer questions such as:

### 2.1 Request Volume Over Time

Understand how request traffic changes by hour and identify periods of higher or lower activity.

### 2.2 HTTP Status Code Distribution

Analyze HTTP response behavior, including the distribution of successful, redirected, client-error, and server-error responses.

### 2.3 Top Requested Endpoints

Identify the most frequently requested URLs/endpoints in the web-server logs.

### 2.4 Rejected Data Analysis

Understand why records were rejected during processing and monitor the main data-quality problems.

---

## 3. Solution Architecture

![Project Architecture](docs/images/Architecture.jpg)

The architecture follows a batch-processing flow:

**Raw Logs → HDFS → Spark Parsing & Validation → Clean/Rejected Parquet → Spark Analytics → HDFS Analytics → Power BI**

The main components are:

- **HDFS** — distributed storage for raw, processed, rejected, and analytics data
- **Apache Spark** — parsing, validation, transformation, and aggregation
- **Parquet + Snappy** — columnar analytics storage and compression
- **Docker** — local execution environment
- **WebHDFS** — HTTP interface used by Power BI to access HDFS files
- **Power BI** — analytics and dashboard layer

See [Architecture](docs/architecture.md) for more details.

---

## 4. Data Source

The input is a large Nginx web-server access-log dataset of approximately **3.3 GB**.

The source contains raw HTTP request information such as:

- Client IP address
- Timestamp
- HTTP request
- HTTP status code
- Response size
- Referrer
- User agent

The raw data is intentionally kept in log format until the Spark processing stage.

---

## 5. Input Data Format

The source records are raw Nginx-style access-log lines.

A typical record contains:

- IP address
- Timestamp
- Request method
- Requested URL
- HTTP protocol
- Status code
- Response size
- Referrer
- User agent

The parsing stage converts these unstructured log lines into structured Spark records.

---

## 6. Target Data Schema

Valid records are transformed into the following analytical schema:

| Column | Description |
|---|---|
| `ip` | Client IP address |
| `timestamp` | Request timestamp |
| `request_method` | HTTP method |
| `request_url` | Requested URL |
| `request_protocol` | HTTP protocol |
| `status_code` | HTTP response status code |
| `response_size` | Response size |
| `referrer` | HTTP referrer |
| `user_agent` | Client user-agent string |
| `year` | Derived year from timestamp |
| `month` | Derived month from timestamp |

The schema is designed to support both data-quality checks and downstream analytics.

---

## 7. Pipeline Architecture and Stages

### Stage 1 — Raw Data Ingestion

The raw log dataset is uploaded to HDFS and becomes the input layer of the pipeline.

### Stage 2 — Parsing

Spark reads the raw log records and extracts the individual fields from each log line.

### Stage 3 — Data Quality Validation

Each parsed record is checked against the project's validation rules.

Records that pass validation continue to the clean-data pipeline.

Records that fail validation are written to a separate rejected-data location.

### Stage 4 — Transformation

Valid records are converted to the required data types and enriched with derived `year` and `month` columns.

### Stage 5 — Processed Data Storage

Clean records are written to HDFS as Snappy-compressed Parquet files and partitioned by year and month.

### Stage 6 — Analytics

Spark reads the processed data and creates aggregated datasets for the required business questions.

### Stage 7 — BI Consumption

The analytics datasets are stored in HDFS and loaded into Power BI through the Hadoop File (HDFS) connector and WebHDFS.

---

## 8. Data Quality Rules

The pipeline validates the following conditions.

### 8.1 Invalid Timestamp

A record is rejected when its timestamp cannot be parsed into the expected datetime representation.

### 8.2 Missing or Invalid IP

A record is rejected when the IP field is missing or does not represent a valid IP address.

### 8.3 Malformed HTTP Request

A record is rejected when the request portion cannot be correctly separated into the expected HTTP request components.

### 8.4 Invalid Status Code

A record is rejected when the HTTP status code is missing or cannot be interpreted as a numeric status code.

### 8.5 Negative Response Size

A record is rejected when the response size is negative.

For the implementation details and validation flow, see [Data Quality](docs/data_quality.md).

---

## 9. Rejected Records

Rejected records are stored separately from the clean dataset.

This prevents invalid records from contaminating downstream analytics while preserving them for data-quality investigation.

The rejected dataset is also stored in Parquet format.

The project therefore maintains two distinct processing outcomes:

```text
Valid Records
    ↓
Processed Parquet
    ↓
Analytics

Rejected Records
    ↓
Rejected Parquet
    ↓
Quality Analysis
```

---

## 10. Data Transformation

The transformation stage performs:

### Parsing

Raw log strings are converted into structured fields.

### Type Conversion

Fields are converted to appropriate analytical types, including timestamps and numeric values.

### Derived Columns

The processing pipeline derives:

- `year`
- `month`

These fields are used for partitioning and time-based analysis.

---

## 11. Output Design

The main processed dataset is stored under:

```text
/log-processing/processed/access_logs
```

The processed data is written as:

- **Format:** Apache Parquet
- **Compression:** Snappy
- **Partitioning:** `year/month`

Rejected records are stored separately under:

```text
/log-processing/rejected
```

The analytics layer is stored under:

```text
/log-processing/analytics
```

---

## 12. Partitioning Strategy

The processed dataset is partitioned using:

```text
year
month
```

This creates a structure similar to:

```text
access_logs/
├── year=2019/
│   ├── month=1/
│   ├── month=2/
│   └── ...
```

Partitioning by time provides a practical layout for time-based workloads and demonstrates a common data-lake optimization technique.

---

## 13. Analytics Layer

Spark produces five analytics datasets.

### 13.1 `request_volume_hourly`

Hourly request volume used to analyze traffic over time.

### 13.2 `status_class_counts`

Counts grouped by HTTP status class, such as:

- 2xx
- 3xx
- 4xx
- 5xx

### 13.3 `status_code_counts`

Counts grouped by individual HTTP status code.

### 13.4 `top_endpoints`

The most frequently requested endpoints.

### 13.5 `top_rejected_reasons`

Counts of rejected records grouped by their validation/rejection reason.

All five analytics outputs are stored in HDFS as Parquet.

See [Analytics](docs/analytics.md).

---

## 14. Power BI Dashboard

![Power BI Dashboard](docs/images/Dashboard.png)

The final analytics layer is consumed in Power BI.

Power BI connects to the HDFS WebHDFS endpoint rather than copying the analytics datasets manually to the local machine.

The dashboard focuses on:

- Request traffic over time
- HTTP status behavior
- Status-code distribution
- Top requested endpoints
- Data-quality rejection reasons

See [Power BI Integration](docs/power_bi.md).

---

## 15. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Project development and supporting logic |
| Apache Spark | Distributed processing and analytics |
| Hadoop HDFS | Data storage |
| Parquet | Columnar storage format |
| Snappy | Compression |
| Docker | Local infrastructure |
| Power BI | Visualization and BI |
| WebHDFS | HTTP-based HDFS access |

---

## 16. Project Structure

```text
Log Processing/
├── config/
├── data/
│   ├── raw/
├── src/
│   ├── analytics/
│   ├── pipeline/
│   └── verification/
├── test/
├── docs/
│   ├── images/
│   ├── analytics.md
│   ├── architecture.md
│   ├── data_pipeline.md
│   ├── data_quality.md
│   ├── design_decisions.md
│   └── power_bi.md
└── README.md
```

---

## 17. Execution Flow

The high-level execution flow is:

```text
1. Start the local Hadoop/Spark environment
2. Upload raw logs to HDFS
3. Read raw logs with Spark
4. Parse log records
5. Validate records
6. Separate valid and rejected records
7. Transform valid records
8. Write processed Parquet to HDFS
9. Run Spark analytics
10. Write analytics Parquet to HDFS
11. Connect Power BI through WebHDFS
12. Build the dashboard
```

---

## 18. Performance and Resource Optimization

The project was developed on a machine with limited RAM, so resource management was an important part of the implementation.

The final successful large-dataset execution used a controlled local Spark configuration instead of allowing Spark to use every available CPU core.

Key settings included:

```text
Spark master: local[4]
spark.sql.shuffle.partitions: 16
Driver memory: 1500m
```

These settings reduced resource pressure during the 3.3 GB processing job.

The project also uses:

- Parquet instead of raw text for processed data
- Snappy compression
- Year/month partitioning
- Separate clean and rejected outputs

See [Design Decisions](docs/design_decisions.md) for the reasoning behind the implementation choices.

---

## 19. Testing and Validation

The final pipeline was validated by checking that:

- The raw dataset could be processed successfully.
- Valid records were written to HDFS.
- Processed output was partitioned by year and month.
- Rejected records were written separately.
- All five analytics datasets were generated.
- HDFS reported no missing or corrupt blocks after processing.
- Power BI could access the analytics layer through WebHDFS.
- The dashboard could consume the resulting Parquet data.

---

## 20. Challenges and Solutions

### Spark Resource Exhaustion

Processing the complete dataset with unrestricted local parallelism caused Spark driver/RPC and heartbeat failures.

**Solution:** reduce local parallelism and shuffle partitions and use a constrained driver-memory configuration.

### Docker/WSL2 Memory Pressure

The local environment had limited memory shared between Docker, Hadoop, and Spark.

**Solution:** keep the Spark configuration conservative and avoid unnecessarily large driver/executor memory allocations.

### HDFS DataNode Hostname Resolution

Power BI could reach the NameNode WebHDFS endpoint through `localhost:9870`, but WebHDFS redirected file reads to the Docker DataNode hostname.

Windows could not resolve that Docker hostname directly.

**Solution:** configure local hostname resolution so the Windows client could resolve the DataNode endpoint while keeping the internal Docker hostname available for container-to-container communication.

This was a local development workaround, not a production deployment design.

---

## 21. Results

The completed project successfully processes the full approximately 3.3 GB log dataset and produces:

- Clean, structured Parquet data in HDFS
- Time-partitioned processed data
- A separate rejected-record dataset
- Five Spark analytics datasets
- A Power BI dashboard consuming the analytics layer through WebHDFS

The project demonstrates the complete flow from raw web-server logs to an analytical BI layer.

---

## 22. Future Improvements

Possible extensions include:

- Introduce Apache Kafka for streaming ingestion.
- Add cloud storage and processing on AWS.
- Add orchestration for scheduled production workflows.
- Add stronger automated data-quality testing.
- Add incremental processing instead of processing the full dataset every run.
- Introduce a more production-oriented cluster deployment.
- Add monitoring and pipeline observability.
- Extend the analytics layer with additional web-traffic KPIs.

Kafka, AWS, and production-scale orchestration are future extensions and are not part of the current implementation.

---

## 23. Skills Demonstrated

This project demonstrates practical experience with:

- Big-data batch processing
- Apache Spark
- Hadoop HDFS
- Distributed file storage concepts
- Log parsing
- Data-quality validation
- Data transformation
- Parquet and columnar storage
- Compression
- Data partitioning
- Spark aggregations
- Docker-based data-engineering environments
- WebHDFS
- Power BI integration
- Performance troubleshooting
- Resource-aware pipeline design

---
## 24. How to Run

The project runs locally using Docker, Hadoop HDFS, and Apache Spark.

### Prerequisites

* Docker Desktop
* Python environment used by the project
* Power BI Desktop (optional, for the dashboard)
* Sufficient free RAM for the local Hadoop/Spark environment

> The project was developed on a machine with approximately 8 GB RAM, so the Spark configuration is intentionally conservative.

### 1. Start the Docker Environment

From the project directory:

```powershell
docker compose up -d
```

Check the running containers:

```powershell
docker ps
```

Allow the Hadoop services a short time to initialize before running HDFS commands.

### 2. Verify HDFS

Check the DataNode status:

```powershell
docker exec hadoop-namenode hdfs dfsadmin -report
```

The report should show a live DataNode.

### 3. Prepare the Raw Data Directory

Create the raw-data directory in HDFS:

```powershell
docker exec hadoop-namenode hdfs dfs -mkdir -p /log-processing/raw
```

### 4. Copy the Raw CSV into the NameNode Container

The raw CSV is first copied from the local Windows machine into the NameNode container.

```powershell
docker cp "<LOCAL_CSV_PATH>" hadoop-namenode:/tmp/raw_data.csv
```

For example:

```powershell
docker cp "D:\Data Track\Data Projects\Log Processing\data\raw\access_logs.csv" hadoop-namenode:/tmp/raw_data.csv
```

### 5. Put the Raw CSV into HDFS

Upload the CSV from the NameNode container into the HDFS raw-data directory:

```powershell
docker exec hadoop-namenode hdfs dfs -put /tmp/raw_data.csv /log-processing/raw/
```

Verify that the file was uploaded successfully:

```powershell
docker exec hadoop-namenode hdfs dfs -ls /log-processing/raw
```

The HDFS input location is:

```text
/log-processing/raw
```

### 6. Run the Spark Pipeline

Run the Spark processing pipeline using the project's Spark entry point.

The successful configuration used for processing the full dataset was:

```text
Spark master: local[4]
spark.sql.shuffle.partitions: 16
Driver memory: 1500m
```

If running through `spark-submit`, the resource configuration is:

```powershell
spark-submit `
  --master local[4] `
  --driver-memory 1500m `
  --conf spark.sql.shuffle.partitions=16 `
  <spark-entry-point>
```

Replace `<spark-entry-point>` with the project's actual Spark entry script.

### 7. Verify the Processed Output

Check the processed HDFS dataset:

```powershell
docker exec hadoop-namenode hdfs dfs -ls -R /log-processing/processed/access_logs
```

The output should contain the year/month partition structure, for example:

```text
/log-processing/processed/access_logs/year=2019/
/log-processing/processed/access_logs/year=2019/month=1/
```

### 8. Verify Rejected Records

Check the rejected-record dataset:

```powershell
docker exec hadoop-namenode hdfs dfs -ls /log-processing/rejected
```

### 9. Verify Analytics Outputs

Check the complete analytics directory:

```powershell
docker exec hadoop-namenode hdfs dfs -ls -R /log-processing/analytics
```

The following datasets should be present:

```text
request_volume_hourly/
status_class_counts/
status_code_counts/
top_endpoints/
top_rejected_reasons/
```

### 10. Connect Power BI

Open Power BI Desktop and select:

```text
Get Data
→ Other
→ Hadoop File (HDFS)
```

Use:

```text
localhost:9870
```

Then navigate to:

```text
/log-processing/analytics
```

Select the required Parquet files and load them into Power BI.

See [Power BI Integration](docs/power_bi.md) for the connection details.

---

## 25. Documentation

Detailed project documentation:

* [Architecture](docs/architecture.md)
* [Data Pipeline](docs/data_pipeline.md)
* [Data Quality](docs/data_quality.md)
* [Analytics](docs/analytics.md)
* [Power BI Integration](docs/power_bi.md)
* [Design Decisions](docs/design_decisions.md)
* [Project Commands](docs/commands.md)


