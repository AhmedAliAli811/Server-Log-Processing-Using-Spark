# Data Quality

## Purpose

The data-quality layer prevents malformed or invalid web-server records from entering the clean analytical dataset.

Rejected records are preserved separately so that data problems remain observable.

## Validation Rules

### 1. Invalid Timestamp

The timestamp must be parseable into the expected datetime representation.

Records with an invalid timestamp are rejected.

### 2. Missing or Invalid IP

The IP field must exist and represent a valid IP address.

Records that fail the IP validation are rejected.

### 3. Malformed HTTP Request

The request section must contain the expected request components.

The pipeline expects:

```text
request_method
request_url
request_protocol
```

Malformed request strings are rejected.

### 4. Invalid Status Code

The status code must be present and numeric.

Records with a non-numeric or invalid status-code value are rejected.

### 5. Negative Response Size

Response size must not be negative.

Records with negative response sizes are rejected.

## Valid vs Rejected Data

The pipeline deliberately separates data-quality outcomes:

```text
Parsed Records
      ↓
   Validation
      ↓
 ┌──────────────┐
 │              │
Valid        Rejected
 │              │
 ↓              ↓
Processed     Rejected
Parquet       Parquet
```

## Rejection Analysis

Rejected records are not silently discarded.

The analytics layer includes:

```text
top_rejected_reasons
```

This allows the project to identify the most common data-quality problems.

