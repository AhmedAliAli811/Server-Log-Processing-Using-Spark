# Project Commands

This document contains the main Docker, Hadoop HDFS, Spark, and Power BI-related commands used during the project.

---

## 1. Docker

### Start the environment

```powershell
docker compose up -d
```

### Check running containers

```powershell
docker ps
```

### Check all containers, including stopped containers

```powershell
docker ps -a
```

### Check the DataNode hostname

```powershell
docker exec hadoop-datanode hostname
```

---

## 2. HDFS Initialization

### Create the raw-data directory

```powershell
docker exec hadoop-namenode hdfs dfs -mkdir -p /log-processing/raw
```

### Create the main project directories

```powershell
docker exec hadoop-namenode hdfs dfs -mkdir -p /log-processing/raw
docker exec hadoop-namenode hdfs dfs -mkdir -p /log-processing/processed
docker exec hadoop-namenode hdfs dfs -mkdir -p /log-processing/rejected
docker exec hadoop-namenode hdfs dfs -mkdir -p /log-processing/analytics
```

### List the HDFS root directory

```powershell
docker exec hadoop-namenode hdfs dfs -ls /
```

---

## 3. Upload Raw CSV to HDFS

The raw CSV is first copied from the Windows host into the NameNode container and then uploaded into HDFS.

### Copy the CSV into the NameNode container

```powershell
docker cp "<LOCAL_CSV_PATH>" hadoop-namenode:/tmp/raw_data.csv
```

For example:

```powershell
docker cp "D:\Data Track\Data Projects\Log Processing\data\raw\access_logs.csv" hadoop-namenode:/tmp/raw_data.csv
```

### Upload the CSV from the container into HDFS

```powershell
docker exec hadoop-namenode hdfs dfs -put /tmp/raw_data.csv /log-processing/raw/
```

### Verify the uploaded file

```powershell
docker exec hadoop-namenode hdfs dfs -ls /log-processing/raw
```

### Check the file size in HDFS

```powershell
docker exec hadoop-namenode hdfs dfs -du -h /log-processing/raw
```

---

## 4. HDFS Verification

### Check the HDFS cluster report

```powershell
docker exec hadoop-namenode hdfs dfsadmin -report
```

This was used to verify:

* DataNode availability
* HDFS capacity
* Remaining space
* Missing blocks
* Corrupt replicas
* DataNode health

### Check the raw data

```powershell
docker exec hadoop-namenode hdfs dfs -ls /log-processing/raw
```

### Recursively list the processed data

```powershell
docker exec hadoop-namenode hdfs dfs -ls -R /log-processing/processed/access_logs
```

### List rejected records

```powershell
docker exec hadoop-namenode hdfs dfs -ls /log-processing/rejected
```

### Recursively list all analytics outputs

```powershell
docker exec hadoop-namenode hdfs dfs -ls -R /log-processing/analytics
```

---

## 5. Spark Processing

The full dataset was processed using a resource-constrained local Spark configuration.

### Spark configuration

```text
Master: local[4]
Shuffle partitions: 16
Driver memory: 1500m
```

### Example spark-submit configuration

```powershell
spark-submit `
  --master local[4] `
  --driver-memory 1500m `
  --conf spark.sql.shuffle.partitions=16 `
  <spark-entry-point
```
