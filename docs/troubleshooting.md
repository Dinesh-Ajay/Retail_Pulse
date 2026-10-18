# RetailPulse — Troubleshooting & Operational Engineering Guide

This guide documents the root causes, diagnostic steps, and resolutions for real-world issues encountered when developing, running, and debugging distributed data pipelines with PySpark, MySQL, and Windows.

---

## 1. PySpark & Java Version Incompatibilities

### Issue: `UnsupportedOperationException: getSubject is not supported`
- **Symptom**: During SparkSession initialization or task creation, the driver fails with:
  ```
  py4j.protocol.Py4JJavaError: An error occurred while calling None.org.apache.spark.api.java.JavaSparkContext.
  : java.lang.UnsupportedOperationException: getSubject is not supported
      at java.base/javax.security.auth.Subject.getSubject(Subject.java:277)
      at org.apache.hadoop.security.UserGroupInformation.getCurrentUser(UserGroupInformation.java:577)
  ```
- **Root Cause**: Starting in Java 23 and Java 24 (via JEP 411 and Security Manager deprecation/removal), `Subject.getSubject(AccessControlContext)` is no longer supported and throws an unconditional runtime exception. Apache Spark 3.5.x and Hadoop 3.3.x depend on this method in `UserGroupInformation`.
- **Resolution**: PySpark 3.5.x requires **Java 8, 11, or 17 LTS**. In `config/settings.py`, RetailPulse automatically detects and configures OpenJDK 17 located at `~/.jdk17`:
  ```python
  os.environ["JAVA_HOME"] = str(Path.home() / ".jdk17" / "jdk-17.0.20.1+1")
  ```

---

## 2. Windows Hadoop Native Binaries (`winutils.exe`)

### Issue: `Shell: Did not find winutils.exe` or `UnsatisfiedLinkError`
- **Symptom**:
  ```
  WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
  ```
- **Root Cause**: Apache Hadoop relies on POSIX-compliant file permissions. On Windows, Hadoop requires `winutils.exe` and `hadoop.dll` to emulate POSIX filesystem operations.
- **Resolution**:
  1. Download compatible Windows Hadoop binaries (Hadoop 3.3.5) into `~/.hadoop/bin`.
  2. Set `HADOOP_HOME` and prepend `%HADOOP_HOME%\bin` to `PATH`.
  3. RetailPulse handles this dynamically at runtime in `config/settings.py`.

---

## 3. PySpark Python Worker Execution & Discovery

### Issue: `Python worker failed to connect back` or `Python was not found`
- **Symptom**:
  ```
  Python was not found; run without arguments to install from the Microsoft Store...
  org.apache.spark.SparkException: Python worker failed to connect back.
  ```
- **Root Cause**: By default, PySpark spawns worker processes using the bare `python` command. On Windows, the Microsoft Store app execution alias intercepts `python` if not configured, or worker processes resolve to a different interpreter version than the driver.
- **Resolution**: Explicitly bind worker and driver executables in Python to the active virtual environment:
  ```python
  import sys, os
  os.environ["PYSPARK_PYTHON"] = sys.executable
  os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
  ```

---

## 4. Local Driver Network Binding & Py4J Gateway Timeouts

### Issue: Driver hangs or throws `SocketTimeoutException: Accept timed out`
- **Symptom**: SparkSession initialization hangs for minutes when attempting to bind to network interfaces.
- **Root Cause**: On multi-NIC systems, VPNs, or Windows machines, Spark may attempt to bind its Py4J gateway to an external IP or unresolvable hostname.
- **Resolution**: Force local loopback binding:
  ```python
  os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
  builder.config("spark.driver.host", "127.0.0.1")
  builder.config("spark.driver.bindAddress", "127.0.0.1")
  ```

---

## 5. Shuffle Partition Overhead on Local Workloads

### Issue: Simple DataFrame transformations take 20–30 seconds for small files
- **Symptom**: Even for a 1,000-row file, grouping or joining takes excessive time.
- **Root Cause**: Spark's default `spark.sql.shuffle.partitions` is set to **200**, designed for multi-node clusters. On a local single-node computer, creating 200 physical task threads causes massive thread scheduling and context-switching overhead.
- **Resolution**: RetailPulse tunes shuffle partitions for local development:
  ```python
  builder.config("spark.sql.shuffle.partitions", "4")
  builder.config("spark.default.parallelism", "4")
  ```

---

## 6. MySQL Transaction Failures & Isolation

### Issue: Partial data corruption after an ETL failure
- **Symptom**: If the warehouse write fails halfway, some tables have new rows while others are missing, breaking referential integrity.
- **Resolution**:
  1. RetailPulse wraps the dimension and fact updates inside an explicit transaction context (`autocommit=False`).
  2. If any batch fails, `conn.rollback()` is invoked immediately in the `except` block.
  3. If MySQL is unreachable, the pipeline logs a warning and marks the run as `SKIPPED_UNAVAILABLE` rather than failing the Parquet generation.
