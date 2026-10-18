# Distributed Processing & Engine Comparison: Spark, Hadoop, and Flink

## 1. Apache Spark Deep Dive

### Core Architecture & Execution Model
- **Driver Node**: The central process coordinating execution. Runs the main program, maintains the `SparkContext`/`SparkSession`, translates DataFrame/SQL logic into an Execution DAG (Directed Acyclic Graph), schedules physical execution tasks, and coordinates with cluster managers (YARN, Kubernetes, Standalone).
- **Executors**: Distributed worker processes running on cluster worker nodes. Responsible for executing tasks, caching data blocks in memory/disk, and returning task completion metrics to the driver.
- **Partitions**: Atomic chunks of data distributed across executors. The level of parallelism in Spark is governed by the number of partitions.

### Transformations vs. Actions & Lazy Evaluation
- **Transformations (Lazy)**: Operations that produce a new DataFrame from an existing one without immediate execution (e.g., `.select()`, `.filter()`, `.groupBy()`, `.withColumn()`). Spark merely records these operations into a logical plan.
- **Catalyst Optimizer**: Transforms the unresolved logical plan -> resolved logical plan -> optimized logical plan -> physical plans -> best physical plan (generating bytecode via Whole-Stage Java Code Generation).
- **Actions (Eager)**: Operations that trigger physical DAG execution and materialize or export data (e.g., `.count()`, `.write.parquet()`, `.show()`, `.collect()`, `.take()`).
- **Why Lazy Evaluation Matters**: Allows Spark to optimize the entire pipeline as a holistic query plan, combining filters, eliminating unused columns (projection pushdown), and reorganizing join orders before moving a single byte of data.

### Narrow vs. Wide Transformations & The Shuffle
- **Narrow Transformations**: Each partition of the parent RDD/DataFrame contributes to at most one partition of the child DataFrame (e.g., `map`, `filter`, `select`). Execution occurs entirely in-memory within the local executor without network data movement.
- **Wide Transformations**: Multiple child partitions depend on data from multiple parent partitions (e.g., `groupByKey`, `distinct`, `join`, `orderBy`, `Window.partitionBy`).
- **The Shuffle**: Wide transformations require data reorganization across the cluster network based on hash keys. Shuffle is the most resource-intensive operation in distributed computing due to disk serialization, network I/O, and deserialization.

### Advanced Optimization Techniques
1. **Predicate Pushdown & Column Projection**:
   - In columnar formats like Apache Parquet, query filters (e.g. `WHERE region = 'South'`) are pushed directly to the file reader, skipping entire row groups and unrequested columns before loading data into memory.
2. **Broadcast Hash Join (BHJ)**:
   - When joining a large dataset with a small dimension table (typically < 10MB default, configurable via `spark.sql.autoBroadcastJoinThreshold`), Spark broadcasts the small table to all executor memories, eliminating the shuffle of the large table.
3. **Data Skew & Mitigation**:
   - Skew occurs when a disproportionate amount of data hashes to a single partition (e.g., 80% of sales originating from one region).
   - *Mitigation*: Key Salting (appending random integers `key_0`, `key_1` to disperse hash buckets), Adaptive Query Execution (`spark.sql.adaptive.skewJoin.enabled=true`), or isolated broadcast joins.
4. **Caching Strategy**:
   - `.cache()` stores data in memory (`MEMORY_AND_DISK_DESER`). Use only when a DataFrame is referenced multiple times in expensive downstream actions. Avoid unnecessary caching as it consumes executor memory needed for shuffle operations.

---

## 2. Hadoop Ecosystem: HDFS, MapReduce, and YARN

### HDFS (Hadoop Distributed File System)
- **Architecture**: Master/Worker pattern with **NameNode** (metadata manager, directory namespace) and **DataNodes** (block storage workers).
- **Storage Characteristics**: Default block size of 128MB (or 256MB) to optimize sequential disk reads; replication factor (typically 3) for hardware fault tolerance.
- **Write-Once-Read-Many**: Ideal for cold archive and historical batch ingestion.

### MapReduce
- **Execution**: Rigid two-phase paradigm (Map -> Shuffle & Sort -> Reduce).
- **Bottleneck**: Every MapReduce iteration must write intermediate states to physical disk and replicate across nodes. Iterative machine learning or multi-stage pipelines suffer immense disk and serialization penalties.

### YARN (Yet Another Resource Negotiator)
- **Decoupling**: Decoupled cluster resource management from compute frameworks.
- **Components**: **ResourceManager** (allocates global CPU/memory leases) and **NodeManager** (monitors per-node containers). Hosts MapReduce, Spark, Flink, and Presto on a shared physical cluster.

---

## 3. Apache Flink: Modern Stateful Stream Processing

### Architecture & Paradigm
- **True Event-Driven Streaming**: Flink processes records continuously on arrival (record-by-record) rather than in micro-batches.
- **Event Time vs. Processing Time**:
  - *Event Time*: The timestamp when an event occurred on the source device.
  - *Processing Time*: The local machine clock of the Flink node executing the transformation.
  - *Watermarks*: Monotonically increasing time markers embedded in the stream to measure event-time progress and handle out-of-order/late data.

### Stateful Processing & Exactly-Once Semantics
- **State Backends**: Maintains state locally in JVM heap or RocksDB.
- **Chandy-Lamport Checkpointing**: Asynchronous barrier snapshots injected into the stream that snapshot state to persistent storage without stopping pipeline execution. Guarantees true end-to-end **Exactly-Once** state recovery in the event of worker failure.

---

## 4. Architectural Comparison Matrix

| Feature / Dimension | Apache Hadoop (MapReduce) | Apache Spark | Apache Flink |
| :--- | :--- | :--- | :--- |
| **Primary Processing Model** | Batch (Disk-bound) | Unified Batch & Micro-batch | Native Streaming & Batch |
| **Latency** | Minutes to Hours | Sub-second to Seconds (Micro-batch) | Milliseconds (Sub-10ms True Stream) |
| **Data Abstractions** | InputFormat / RecordReader | RDD, DataFrame, Dataset | DataStream, Table & SQL |
| **Intermediate State** | Spilled to HDFS / Local Disk | Memory / RAM with spill fallback | Managed State Backend (RocksDB) |
| **Fault Tolerance** | Replay failed tasks from disk | RDD Lineage Graph recalculation | Lightweight Distributed Checkpointing |
| **Join Capabilities** | Map-side / Reduce-side joins | Broadcast, Sort-Merge, Shuffle Hash | Temporal, Window, and Interval Joins |
| **Best-fit Use Cases** | Massive historical archive, batch | Large-scale ETL, SQL marts, ML pipelines | Fraud detection, live telemetry, alerting |

---

## 5. What RetailPulse Demonstrates vs. Cluster Extensions

### Concepts Demonstrated Directly in RetailPulse Code:
1. **Explicit PySpark Schema Enforcement**: Prevents runtime schema inference penalties and eliminates driver crashes from unexpected column variations.
2. **Deterministic Window Partitioning**: Uses `Window.partitionBy("order_id").orderBy(...)` to eliminate data anomalies.
3. **Column Projection & Predicate Pushdown**: Uses targeted `.select()` and early `.filter()` passes.
4. **Columnar Parquet Storage**: Adopts Snappy-compressed Parquet files for Silver and Gold layers.
5. **Driver Local IP & Shuffle Configuration**: Explicitly binds `127.0.0.1` and tunes `spark.sql.shuffle.partitions=4` to optimize local single-node development.

### Conceptual Extensions (Production Cluster Reality):
- Multi-node worker provisioning across YARN or Kubernetes.
- Dynamic executor allocation (`spark.dynamicAllocation.enabled`).
- S3 / ADLS distributed object store eventual consistency handling.
- Real-time event consumption via Apache Kafka or AWS Kinesis.
