# Design Decisions

This document records the main implementation decisions made during the project.

## 1. HDFS for Storage

HDFS was selected as the storage layer to demonstrate Hadoop distributed-storage concepts and provide a clear separation between storage and processing.

The project stores:

- Raw logs
- Processed data
- Rejected data
- Analytics outputs

## 2. Apache Spark for Processing

Spark was selected for parsing, validation, transformation, and aggregation.

The full dataset is large enough to demonstrate practical distributed-processing concerns while still being executable in the local development environment.

## 3. Parquet Instead of Raw Text for Processed Data

Processed records are stored as Parquet because the downstream workload is analytical.

Advantages demonstrated by the project include:

- Columnar storage
- Efficient analytical reads
- Schema-aware data
- Compression support

## 4. Snappy Compression

Snappy is used for the processed Parquet output to reduce storage size while remaining appropriate for analytical workloads.

## 5. Year/Month Partitioning

The processed dataset is partitioned by:

```text
year
month
```

This creates a time-oriented directory structure and demonstrates a common data-lake partitioning strategy.

## 6. Separate Rejected Dataset

Invalid records are stored separately rather than being dropped.

This decision provides:

- Traceability
- Data-quality visibility
- Rejection analysis
- Easier debugging

## 7. Separate Analytics Layer

The project does not connect Power BI directly to the complete event-level processed dataset for the main dashboard.

Instead:

```text
Processed Data
      ↓
Spark Aggregation
      ↓
Analytics Data
      ↓
Power BI
```

This keeps the heavy aggregation work in Spark.

## 8. Resource-Constrained Spark Configuration

The project was developed on a machine with approximately 8 GB of RAM.

Unrestricted local parallelism caused resource pressure and Spark communication/heartbeat failures during full-dataset processing.

The successful configuration used:

```text
local[4]
spark.sql.shuffle.partitions = 16
driver memory = 1500m
```

The configuration was chosen to balance processing capability with the limited local resources.

## 9. Local Docker Environment

Docker was used to run the Hadoop/Spark environment locally.

This made it possible to reproduce the project components without requiring a cloud account or external cluster.

The local setup is intended for learning and portfolio demonstration rather than production deployment.

## 10. WebHDFS for Power BI

Power BI requires an HTTP-accessible HDFS interface in this setup.

WebHDFS provides that interface through the NameNode HTTP endpoint.

The final local flow is:

```text
Power BI
   ↓
WebHDFS
   ↓
HDFS
```

## 11. Local Windows Hostname Resolution

The Docker DataNode hostname was not directly resolvable from Windows.

Host-level hostname resolution was used to allow the Power BI client to follow the WebHDFS file-read endpoint.

This was chosen because it solved the local client-to-container networking problem without changing the internal Hadoop hostname used by Spark and the other Docker containers.

## 12. Current Scope vs Future Scope

The current project is a batch-processing pipeline.

The following are future extensions rather than current components:

- Kafka
- AWS/cloud deployment
- Production orchestration
- Streaming ingestion
- Production monitoring

