# Analytics Layer

## Purpose

The analytics layer converts the processed event-level dataset into aggregated datasets that can be consumed efficiently by Power BI.

All analytics are generated with Apache Spark and stored as Parquet in HDFS.

## Analytics Outputs

### 1. Request Volume by Hour

**Dataset:**

```text
request_volume_hourly
```

Purpose:

- Measure request traffic over time.
- Identify high-traffic and low-traffic periods.
- Support time-series visualization.

### 2. Status Class Counts

**Dataset:**

```text
status_class_counts
```

HTTP status codes are grouped into broad classes:

```text
2xx
3xx
4xx
5xx
```

Purpose:

- Understand the overall behavior of HTTP responses.
- Compare successful and unsuccessful request classes.

### 3. Status Code Counts

**Dataset:**

```text
status_code_counts
```

Purpose:

- Analyze individual HTTP status codes.
- Identify the most frequent response codes.
- Detect unusual response patterns.

### 4. Top Endpoints

**Dataset:**

```text
top_endpoints
```

Purpose:

- Identify the most requested URLs/endpoints.
- Understand which resources receive the most traffic.

### 5. Top Rejection Reasons

**Dataset:**

```text
top_rejected_reasons
```

Purpose:

- Identify the most common validation failures.
- Provide visibility into data quality.

## Analytics Storage

The datasets are stored under:

```text
/log-processing/analytics
```

with one directory per analytical output.

Example:

```text
analytics/
├── request_volume_hourly/
├── status_class_counts/
├── status_code_counts/
├── top_endpoints/
└── top_rejected_reasons/
```

## Why an Analytics Layer?

Power BI does not need to process the complete raw event dataset for every visualization.

Instead, Spark performs the heavy aggregation first.

This creates a simple separation:

```text
Spark
↓
Heavy Processing & Aggregation
↓
Analytics Parquet
↓
Power BI
↓
Visualization
```

This approach keeps the BI layer focused on visualization and exploration.
