# Data Pipeline

## Pipeline Goal

The pipeline converts raw Nginx access logs into structured, validated, partitioned Parquet datasets and then produces analytical aggregates.

## Processing Stages

### 1. Raw Ingestion

The raw log dataset is placed in HDFS.

The raw layer preserves the source records before structured processing.

### 2. Reading

Spark reads the raw log files from HDFS.

### 3. Parsing

Each raw log line is parsed into structured fields:

```text
ip
timestamp
request_method
request_url
request_protocol
status_code
response_size
referrer
user_agent
```

### 4. Validation

Each parsed record is checked against the project's data-quality rules.

Invalid records are not allowed into the clean dataset.

### 5. Split

The records are divided into:

```text
Valid Records
Rejected Records
```

### 6. Transformation

Valid records receive the required data types and derived fields:

```text
year
month
```

### 7. Processed Storage

Valid records are written to:

```text
/log-processing/processed/access_logs
```

using:

- Parquet
- Snappy compression
- `year/month` partitioning

### 8. Rejected Storage

Rejected records are written separately under:

```text
/log-processing/rejected
```

This preserves invalid records for quality analysis.

### 9. Analytics

Spark reads the processed dataset and generates:

```text
request_volume_hourly
status_class_counts
status_code_counts
top_endpoints
top_rejected_reasons
```

### 10. Analytics Storage

The resulting datasets are stored under:

```text
/log-processing/analytics
```

### 11. BI Consumption

Power BI accesses the analytics Parquet files through the Hadoop File (HDFS) connector and WebHDFS.

## Final Flow

```text
Raw Logs
   ↓
HDFS
   ↓
Spark
   ↓
Parse
   ↓
Validate
   ↓
┌──────────────┬──────────────┐
│ Valid        │ Rejected     │
↓              ↓
Transform      Store
↓
Parquet
↓
Analytics
↓
Power BI
```
