# 40+ Big Data Engineering Interview Questions & Practical Exercises

Prepared for the **micro1 Big Data Engineer Technical & AI Interview**.

---

## Category 1: Python for Data Engineering

### 1. How does Python manage memory, and how do you prevent memory leaks in large data batch jobs?
**Answer**: Python uses private heap memory managed by reference counting and a cyclic garbage collector. In large data processing, memory issues occur if large collections (like lists of dictionaries) are held in memory. Mitigation:
- Use generators (`yield`) and iterative file reading (`csv.reader` chunks).
- Use generator expressions over list comprehensions for millions of items.
- Explicitly call `del obj` and `gc.collect()` when destroying intermediate in-memory structures.
- Offload large memory transformations to distributed engines like PySpark.

### 2. What is the GIL (Global Interpreter Lock), and how does it impact data engineering pipelines?
**Answer**: The GIL is a mutex in CPython that prevents multiple native threads from executing Python bytecodes simultaneously. While it limits multi-threaded CPU-bound Python tasks, PySpark bypasses GIL constraints because:
- Heavy computation runs inside JVM executors in Scala/Java threads.
- PySpark communicates with JVM via sockets/Py4J, parallelizing across separate OS worker processes.

### 3. What is the difference between shallow copy and deep copy in Python?
**Answer**:
- A **shallow copy** (`copy.copy()`) creates a new container object but inserts references to the original nested objects. Modifying nested mutable objects affects both copies.
- A **deep copy** (`copy.deepcopy()`) recursively duplicates the container and all nested objects, creating completely independent copies.

### 4. How do you handle schema drift in Python data ingestion?
**Answer**:
- Define explicit schema models using Pydantic, Marshmallow, or PySpark `StructType`.
- Compare incoming keys against expected schemas; log and quarantine records with unexpected columns or missing required fields.
- Use schema evolution features in modern lakehouse formats (like Delta Lake `.option("mergeSchema", "true")`).

---

## Category 2: SQL and Relational Databases

### 5. What is the difference between `RANK()`, `DENSE_RANK()`, and `ROW_NUMBER()`?
**Answer**:
- `ROW_NUMBER()` assigns sequential integers (1, 2, 3, 4) regardless of ties.
- `RANK()` assigns identical ranks to tied values, then skips subsequent ranks (e.g., 1, 2, 2, 4).
- `DENSE_RANK()` assigns identical ranks to tied values without skipping ranks (e.g., 1, 2, 2, 3).

### 6. Explain the ACID properties of relational databases.
**Answer**:
- **Atomicity**: All operations in a transaction succeed, or the entire transaction is rolled back.
- **Consistency**: The database transitions from one valid state to another, maintaining constraints.
- **Isolation**: Concurrent transactions execute without cross-contamination (e.g., Read Committed, Repeatable Read).
- **Durability**: Committed data survives system crashes or power failures.

### 7. How do indexes work, and when can an index degrade database performance?
**Answer**:
- Indexes (typically B-Trees) maintain sorted pointer structures to quickly locate rows by key without full table scans (`O(log N)` vs `O(N)`).
- Degradation occurs during write-heavy workloads (`INSERT`, `UPDATE`, `DELETE`) because every index on the table must be re-balanced upon each modification. Too many indexes cause write amplification.

### 8. What is the difference between a Clustered Index and a Non-Clustered Index?
**Answer**:
- **Clustered Index**: Determines the physical ordering of data pages on disk (only one per table; in MySQL InnoDB, this is the Primary Key).
- **Non-Clustered Index**: A separate structure containing index keys with pointers back to the clustered index/row locator.

---

## Category 3: ETL / ELT & Pipeline Engineering

### 9. What is the fundamental difference between ETL and ELT?
**Answer**:
- **ETL (Extract, Transform, Load)**: Data is transformed in a specialized compute cluster (like Spark) before being loaded into the target warehouse. Ideal for strict privacy compliance, masking PII, and legacy warehouses.
- **ELT (Extract, Load, Transform)**: Raw data is loaded directly into the cloud data warehouse/lakehouse (like Snowflake or BigQuery) and transformed inside the engine using SQL (e.g., dbt). Takes advantage of modern cloud MPP compute scalability.

### 10. How do you design an idempotent batch pipeline?
**Answer**:
- Idempotency ensures that executing a pipeline multiple times with the same input produces the exact same result without duplicate side-effects.
- *Techniques*:
  1. Overwrite partitions: `write.mode("overwrite")` on target date partitions.
  2. Staging-and-merge: Load into a temporary staging table and execute an atomic `MERGE` or `INSERT ... ON DUPLICATE KEY UPDATE`.
  3. Upserts keyed on unique business transaction identifiers.

### 11. What is the difference between full load and incremental load?
**Answer**:
- **Full Load**: Truncates and reloads the entire target dataset. Simple, but resource-intensive and unviable for multi-terabyte tables.
- **Incremental Load**: Ingests only new, modified, or deleted records since the last checkpoint, tracked via watermarks, modification timestamps (`updated_at`), or Change Data Capture (CDC).

### 12. What is Change Data Capture (CDC)?
**Answer**: CDC monitors and captures row-level changes (`INSERT`, `UPDATE`, `DELETE`) directly from a database's transaction log (e.g., MySQL binlog, PostgreSQL WAL) using tools like **Debezium** and streams them to Kafka or a lakehouse with zero query impact on the source database.

---

## Category 4: Data Warehousing & Dimensional Modeling

### 13. What is the Kimball Star Schema vs. Snowflake Schema?
**Answer**:
- **Star Schema**: A central fact table surrounded by denormalized dimension tables. Faster query performance due to fewer joins; easier for BI analysts to understand.
- **Snowflake Schema**: Dimensions are normalized into multiple related sub-tables (e.g., `dim_product` references `dim_subcategory` which references `dim_category`). Reduces data redundancy but requires complex multi-table joins.

### 14. What is a Surrogate Key, and why is it preferred over a Natural Key?
**Answer**:
- A **Natural Key** is a business identifier from the source system (e.g., `product_id = 'P101'`).
- A **Surrogate Key** is a synthetic integer primary key generated in the warehouse (e.g., `product_key = 14`).
- *Benefits*:
  1. Insulates warehouse history from source key reassignments or changes.
  2. Enables Slowly Changing Dimensions (SCD Type 2) where one natural key has multiple historical surrogate keys.
  3. Integer surrogate keys yield faster join performance and reduced index sizes.

### 15. What are Slowly Changing Dimensions (SCD), and how do types 1, 2, and 3 differ?
**Answer**:
- **SCD Type 1**: Overwrites the old attribute value with the new value (no history preserved).
- **SCD Type 2**: Creates a new row with a new surrogate key, marking the old row with `end_date` and `is_current = FALSE` (complete historical tracking).
- **SCD Type 3**: Adds a new column (e.g., `previous_region` and `current_region`) to track limited history.

---

## Category 5: Apache Spark & PySpark

### 16. What is the difference between RDD, DataFrame, and Dataset?
**Answer**:
- **RDD (Resilient Distributed Dataset)**: Low-level functional collection of JVM objects. No built-in schema awareness; lacks Catalyst optimization.
- **DataFrame**: Untyped distributed dataset organized into named columns. Optimized by the Catalyst Optimizer and Tungsten execution engine. Available in Python, Scala, R, Java.
- **Dataset**: Strongly-typed JVM collection combining DataFrame optimizations with compile-time type safety. Available only in Scala and Java.

### 17. What causes a Spark job to fail with an `OutOfMemoryError` (OOM)?
**Answer**:
- *Driver OOM*: Calling `.collect()`, `.toPandas()`, or broadcasting a table that exceeds driver memory.
- *Executor OOM*: High data skew (one partition receiving gigabytes of data), inadequate executor memory (`spark.executor.memory`), or too few shuffle partitions resulting in large in-memory hash aggregations.

### 18. How do you mitigate data skew in Spark?
**Answer**:
1. **Salting**: Append a random integer (`0..N`) to the join key of the skewed dataset, duplicate the dimension table rows with all salt values, join, and aggregate out the salt.
2. **Broadcast Join**: If one side is small, broadcast it so no partition hashing occurs.
3. **Adaptive Query Execution (AQE)**: Set `spark.sql.adaptive.skewJoin.enabled = true`, allowing Spark runtime to dynamically split oversized skewed partitions into smaller sub-partitions.

### 19. What is the difference between `.coalesce()` and `.repartition()`?
**Answer**:
- `.repartition(N)`: Performs a full shuffle across the cluster network to evenly distribute data into `N` partitions. Can increase or decrease partition counts.
- `.coalesce(N)`: Avoids a full shuffle by merging adjacent partitions on the same executor nodes. Can only decrease partition count. Much faster when reducing partitions before writing to disk.

### 20. What is Predicate Pushdown and Projection Pruning?
**Answer**:
- **Predicate Pushdown**: Spark pushes SQL `WHERE` filter clauses down to the underlying Parquet/ORC file reader so unmatching row groups are never read into memory.
- **Projection Pruning**: Spark only reads the columns explicitly referenced in the query, ignoring other columns stored in the columnar file format.

---

## Category 6: Hadoop and Apache Flink

### 21. How does HDFS achieve fault tolerance?
**Answer**: By dividing files into fixed-size blocks (default 128MB) and replicating each block across multiple distinct physical DataNodes (default replication factor: 3). The NameNode monitors heartbeats and block reports; if a DataNode fails, the NameNode initiates background re-replication.

### 22. Why is Apache Spark faster than Hadoop MapReduce?
**Answer**:
1. Spark executes intermediate DAG steps in memory (RAM), whereas MapReduce writes intermediate map results and shuffle output to physical disk.
2. Spark uses Whole-Stage Code Generation (Tungsten) and query graph optimization (Catalyst).
3. Spark avoids repetitive JVM startup overhead for multi-pass iterative computations.

### 23. What is the difference between Spark Structured Streaming and Apache Flink?
**Answer**:
- **Spark Structured Streaming**: Micro-batch engine (discretized streams). Collects incoming events over a tiny time window (e.g. 100ms) and runs mini-batch Spark jobs.
- **Apache Flink**: Native event-driven streaming. Processes records immediately on arrival with millisecond latency, true out-of-order event-time watermarking, and managed state backends (RocksDB).

---

## Category 7: Cloud Big Data Platforms (AWS / GCP / Azure)

### 24. How would you choose between AWS EMR and AWS Glue?
**Answer**:
- **AWS Glue**: Serverless Spark. Ideal for scheduled batch jobs, minimal maintenance, variable workloads, and tight integration with AWS Glue Data Catalog.
- **Amazon EMR**: Dedicated, long-running or transient EC2 clusters. Ideal when you need custom Spark configurations, deep JVM tuning, Spot instance cost optimizations, or multi-engine stacks (Spark + Presto + Trino).

### 25. How does Snowflake / BigQuery separate storage and compute?
**Answer**: Storage is decoupled into durable cloud object stores (S3, GCS, Azure Blob). Compute clusters (warehouses or query slots) are spun up dynamically on demand, read data over high-speed networks, cache active data locally, and can scale up, down, or suspend independently of stored data volume.

### 26. How do you protect sensitive credentials in a production cloud pipeline?
**Answer**:
- Never commit credentials to source control or container images.
- Use Cloud Secret Managers (AWS Secrets Manager, Azure Key Vault, GCP Secret Manager).
- Leverage IAM roles and instance profiles (e.g., AWS IAM Roles for Service Accounts in EKS, Managed Identities in Azure) to eliminate static passwords entirely.

---

## Category 8: Data Quality, Governance & Observability

### 27. What is the difference between hard validation and soft validation in ETL?
**Answer**:
- **Hard Validation**: Immediate pipeline termination if data violates critical business rules (e.g., missing primary keys, corrupted schema headers).
- **Soft Validation (Quarantine)**: Defective records are filtered into a dead-letter queue or quarantine dataset (like RetailPulse's `data/rejected/`), allowing clean records to continue processing while alerting data stewards.

### 28. What key metrics should be monitored in a data pipeline SLA?
**Answer**:
1. **Freshness / Latency**: Time elapsed between source event creation and warehouse availability.
2. **Completeness & Volume**: Ingested record counts vs. baseline thresholds (detecting 0-byte source drops).
3. **Data Quality Defect Rate**: Percentage of rejected/quarantined records over time.
4. **Execution Duration**: Pipeline runtime trends detecting resource starvation or shuffle degradation.

---

## Category 9: Pipeline Reliability, Troubleshooting & Scenario-Based

### 29. Scenario: A Spark job running fine for months suddenly takes 5x longer. How do you investigate?
**Answer**:
1. Check input data volume: Did a sudden volume surge occur (data explosion)?
2. Open the **Spark Web UI**:
   - Inspect Stage durations and the Event Timeline.
   - Look at Task metrics (min, median, max durations). If max task time is vastly higher than median, identify **Data Skew**.
   - Check **Shuffle Read/Write** volumes.
3. Check cluster resource utilization: Were worker nodes preempted (Spot loss) or sharing resources?

### 30. Scenario: A batch job fails halfway through writing to a database. How do you avoid corrupted or partial state?
**Answer**:
- Implement atomic transactions: Wrap operations in `BEGIN ... COMMIT` and execute `ROLLBACK` on error.
- Use the **Staging-Table pattern**: Load the batch into a temporary staging table, run validation checks on the staged data, and perform an atomic table swap or transactional merge into production.

---

## Category 10: RetailPulse Project-Specific Questions

### 31. What is the business purpose and transaction grain of RetailPulse?
**Answer**: RetailPulse is a batch e-commerce data pipeline designed to ingest messy retail transactions, cleanse records, quarantine defects, and serve a Star Schema Data Warehouse. The transaction grain is one row per atomic valid order (`order_id`).

### 32. Explain the schema reconciliation in RetailPulse between input, silver, and rejected records.
**Answer**:
$$\text{Input Records} = \text{Silver Records} + \text{Rejected Records}$$
In our verified 200-record execution:
- 200 input records
- 170 clean records written to Silver and loaded into MySQL `fact_sales`
- 30 defective records quarantined (e.g. 6 invalid quantities, 3 duplicate order IDs, 3 missing customer IDs, 3 invalid dates, etc.)
- Reconciles 100% mathematically.

### 33. Why did you implement window functions for duplicate detection instead of `.dropDuplicates()`?
**Answer**:
- `.dropDuplicates(["order_id"])` silently drops subsequent records with zero audit trail.
- In enterprise pipelines, auditors need to know which records were duplicated and why. By using `Window.partitionBy("order_id").orderBy("_ingestion_timestamp")` and evaluating `row_number() > 1`, we preserve the duplicate rows in `data/rejected/` with the explicit reason `DUPLICATE_ORDER_ID`.

---

## 5 Practical Coding Exercises with Solutions

### Exercise 1: Clean a CSV Dataset in Pure Python
```python
import csv
from datetime import datetime

def clean_csv_records(input_path, output_path):
    clean_records = []
    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # 1. Check non-empty keys
                if not row["order_id"] or not row["customer_id"]:
                    continue
                # 2. Parse quantity and price
                qty = int(row["quantity"])
                price = float(row["unit_price"])
                if qty <= 0 or price < 0:
                    continue
                # 3. Validate date format
                datetime.strptime(row["order_date"], "%Y-%m-%d")
                
                # Append cleansed record
                row["total_amount"] = f"{round(qty * price, 2):.2f}"
                clean_records.append(row)
            except (ValueError, KeyError):
                continue
                
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(clean_records[0].keys()))
        writer.writeheader()
        writer.writerows(clean_records)
```

### Exercise 2: SQL Window Function Query
*Problem: Calculate monthly cumulative running revenue and regional revenue rank.*
```sql
SELECT 
    order_date,
    region,
    total_amount,
    -- Running total partitioned by region ordered by date
    SUM(total_amount) OVER (
        PARTITION BY region 
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_regional_revenue,
    -- Rank regions by total revenue
    DENSE_RANK() OVER (ORDER BY total_amount DESC) AS transaction_rank
FROM fact_sales;
```

### Exercise 3: Transform a Spark DataFrame
*Problem: Filter active customers, parse dates, and compute category aggregations.*
```python
from pyspark.sql.functions import col, to_date, sum as _sum, round as _round

def process_sales(spark_df):
    return (
        spark_df
        .filter(col("quantity") > 0)
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("total_amount", _round(col("quantity") * col("unit_price"), 2))
        .groupBy("category")
        .agg(
            _sum("quantity").alias("total_units"),
            _round(_sum("total_amount"), 2).alias("total_revenue")
        )
        .orderBy(col("total_revenue").desc())
    )
```

### Exercise 4: Detect Duplicates with PySpark Window Functions
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

def isolate_duplicates(df, key_col="order_id", order_col="ingestion_timestamp"):
    window_spec = Window.partitionBy(key_col).orderBy(col(order_col).asc())
    ranked_df = df.withColumn("row_num", row_number().over(window_spec))
    
    valid_df = ranked_df.filter(col("row_num") == 1).drop("row_num")
    duplicates_df = ranked_df.filter(col("row_num") > 1).drop("row_num")
    return valid_df, duplicates_df
```

### Exercise 5: Debugging a Failing ETL Pipeline
*Problem Scenario*: Pipeline crashes with `NullPointerException` during date parsing and loses halfway committed database writes.
*Solution & Reasoning*:
```python
# 1. Use tolerant date parsing instead of strict casting
# Replace: col("order_date").cast("date") -> throws exception or silent null
# With: to_date(col("order_date"), "yyyy-MM-dd") and evaluate with when/otherwise

# 2. Wrap database writes in explicit atomic transactions:
conn = get_connection()
cursor = conn.cursor()
try:
    cursor.executemany(insert_dimensions, dim_data)
    cursor.executemany(insert_facts, fact_data)
    conn.commit()  # Single atomic commit point
except Exception as e:
    conn.rollback() # Guarantee zero partial state
    raise e
finally:
    cursor.close()
    conn.close()
```
