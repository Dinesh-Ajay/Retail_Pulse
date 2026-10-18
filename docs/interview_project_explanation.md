# RetailPulse — Project Explanation Guide for micro1 Interview

This document provides word-for-word spoken talking points tailored for the **micro1 AI conversational interview**.

---

## 1. The 60–90 Second Project Explanation (Recommended Pitch)

> "Hi! I recently designed and built **RetailPulse**, an end-to-end retail data engineering pipeline and analytical warehouse that transforms raw, messy e-commerce transactions into clean, verified datasets.
>
> In real-world e-commerce, transaction feeds are plagued by dirty data—duplicate transactions, missing customer keys, invalid calendar dates, and negative prices. To solve this, I designed a pipeline based on the **Medallion Architecture**.
>
> 1. In the **Bronze layer**, we ingest raw data verbatim with audit timestamps to maintain a historical record of incoming payloads.
> 2. In the **Silver layer**, I use **PySpark** with explicit schemas to trim strings, cast types, compute financial amounts with high precision, and evaluate data quality rules. What's crucial here is that malformed records aren't silently dropped—they're routed into a quarantined rejected dataset with explicit diagnostic error codes like `MISSING_CUSTOMER_ID` or `INVALID_QUANTITY`. We also resolve duplicate order IDs deterministically using Spark window functions.
> 3. In the **Gold layer**, we compute business KPIs—such as daily sales, product velocity, and regional revenue—persisting them as columnar **Parquet** files with Snappy compression.
> 4. Finally, I implemented a **Kimball Star Schema Data Warehouse in MySQL** with `dim_product`, `dim_date`, and `fact_sales`. I ensured the loading stage is strictly **idempotent** and transactional, so repeated runs update existing keys rather than duplicating facts.
>
> For reliability, I wrote an automated **pytest suite** covering 18 unit and integration tests, implemented structured logging, and generated machine-readable execution reports.
>
> While this runs locally on PySpark, I designed the transformations so they can seamlessly scale to cloud platforms like AWS EMR, Glue, or Databricks as data volumes grow."

---

## 2. The 30-Second Elevator Pitch

> "I built **RetailPulse**, a PySpark and MySQL data engineering pipeline that cleanses raw retail transactions and serves a Star Schema Data Warehouse.
>
> It implements the Medallion architecture: raw Bronze preservation, Silver cleansing with bad-data quarantine and deterministic deduplication, and Gold Parquet aggregations.
>
> It features idempotent MySQL loading, comprehensive logging, execution reports, and 18 automated tests. It demonstrates how to build robust, observable ETL pipelines that handle dirty data gracefully."

---

## 3. Five Likely Follow-Up Questions & Grounded Answers

### Q1: How did you handle duplicate records in your Spark pipeline?
**Answer**:
> "I treated `order_id` as the unique transaction grain. Using PySpark's `Window.partitionBy("order_id_clean").orderBy(col("_ingestion_timestamp").asc())`, I assigned a `row_number()`. The first occurrence was retained for the Silver dataset, while any subsequent occurrence (`row_num > 1`) was tagged with the rejection code `DUPLICATE_ORDER_ID` and written to our quarantine layer. This prevented duplicate sales from skewing our financial metrics."

### Q2: Why did you choose Parquet over CSV for processed layers?
**Answer**:
> "CSV is row-oriented, uncompressed, and has no native schema. Parquet is a columnar storage format with Snappy compression. It offers two massive benefits for big data: **column projection pruning**—where queries only read the specific columns requested—and **predicate pushdown**—where row groups are skipped at the storage layer based on min/max statistics. In our benchmarks, this dramatically reduces disk I/O and query execution time."

### Q3: How do you ensure your MySQL warehouse loading is idempotent?
**Answer**:
> "I used a transactional upsert approach. For `fact_sales`, we use parameterized `INSERT INTO ... ON DUPLICATE KEY UPDATE` statements within an explicit database transaction (`autocommit=False`). If the same batch is loaded twice, existing rows are refreshed rather than duplicated. Furthermore, if any batch fails, we execute a complete `conn.rollback()`, ensuring no partial or corrupted data is committed."

### Q4: Why might local PySpark be slower than Pandas on small datasets?
**Answer**:
> "On small datasets—like a few hundred records—Pandas is faster because it runs purely in-memory in a single Python thread with zero initialization overhead. PySpark, on the other hand, has the JVM startup cost, Py4J gateway socket communication, and DAG compilation overhead. However, as data scales beyond memory limits (e.g., millions of rows), Pandas crashes with `OutOfMemoryError`, while Spark's distributed partitions and out-of-core execution scale horizontally."

### Q5: How would you monitor this pipeline in a production cloud environment?
**Answer**:
> "In AWS, I would run the PySpark job on AWS Glue or EMR, stream driver logs to **Amazon CloudWatch**, and configure CloudWatch Alarms to trigger **SNS** notifications to Slack or PagerDuty if any stage status is not `SUCCESS`. I would also push our JSON execution report metrics—like input counts vs. rejected counts—into CloudWatch Custom Metrics to monitor data quality trends over time."

---

## 4. Honest Engineering Limitations to Acknowledge

1. **Local Single-Node vs. Distributed Cluster**: The pipeline runs on local PySpark using `local[*]`. It does not demonstrate physical multi-node network shuffles, dynamic executor scaling, or cloud object store eventual consistency.
2. **Transaction Grain Assumption**: The current schema assumes one order represents one transaction line item. In a multi-item e-commerce system, the fact table grain would need to change to `order_item_id` with composite keys (`order_id`, `item_id`).
3. **Batch vs. Streaming**: RetailPulse is a scheduled batch pipeline. It does not process continuous real-time streams via Apache Kafka or Spark Structured Streaming.

---

## 5. Proposed Future Improvements

1. **Migrate to Delta Lake**: Implement Delta Lake or Apache Iceberg on top of Parquet to get ACID transactions, schema enforcement, and time-travel capability directly on the lake storage.
2. **Workflow Orchestration**: Integrate **Apache Airflow** or **Prefect** to schedule DAG execution, manage backfills, and handle automated retries.
3. **Data Quality Framework**: Introduce **Great Expectations** or AWS Deequ for declarative data quality assertions and automated data profiling documentation.
