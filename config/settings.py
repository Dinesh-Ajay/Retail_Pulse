"""
Configuration and runtime environment setup for RetailPulse pipeline.
Handles environment variables, local path resolution, JVM/Hadoop compatibility,
and SparkSession provisioning.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Define base project directories
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load .env if present
env_path = PROJECT_ROOT / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

# Path definitions
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
REJECTED_DIR = DATA_DIR / "rejected"
LOGS_DIR = PROJECT_ROOT / "logs"
REPORTS_DIR = PROJECT_ROOT / "reports"
SQL_DIR = PROJECT_ROOT / "sql"

# Ensure runtime directories exist
for directory in [DATA_DIR, RAW_DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, REJECTED_DIR, LOGS_DIR, REPORTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------
# Windows / PySpark / JVM Environment Configuration
# Ensures seamless local execution across Windows development setups
# ---------------------------------------------------------
def configure_spark_environment():
    """Configure system environment variables required for PySpark on Windows."""
    # Configure Python executable for PySpark driver & workers
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    # Search and bind compatible JDK (prioritizing local OpenJDK 17)
    user_home = Path.home()
    jdk17_candidate = user_home / ".jdk17"
    if jdk17_candidate.exists():
        subdirs = [d for d in jdk17_candidate.iterdir() if d.is_dir() and (d / "bin" / "java.exe").exists()]
        target_jdk = str(subdirs[0]) if subdirs else str(jdk17_candidate)
        os.environ["JAVA_HOME"] = target_jdk
    elif not os.environ.get("JAVA_HOME") or not Path(os.environ.get("JAVA_HOME", "")).exists():
        # Fallback check
        program_files_jdk = Path("C:/Program Files/Java")
        if program_files_jdk.exists():
            jdks = list(program_files_jdk.glob("jdk*"))
            if jdks:
                os.environ["JAVA_HOME"] = str(jdks[0])

    # Search and bind Windows Hadoop binaries (winutils.exe & hadoop.dll)
    hadoop_candidate = user_home / ".hadoop"
    if hadoop_candidate.exists() and (hadoop_candidate / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(hadoop_candidate)
        hadoop_bin = str(hadoop_candidate / "bin")
        current_path = os.environ.get("PATH", "")
        if hadoop_bin not in current_path:
            os.environ["PATH"] = f"{hadoop_bin};{current_path}"

    # Also prepend JAVA_HOME/bin to PATH if set
    java_home = os.environ.get("JAVA_HOME")
    if java_home and Path(java_home).exists():
        java_bin = str(Path(java_home) / "bin")
        current_path = os.environ.get("PATH", "")
        if java_bin not in current_path:
            os.environ["PATH"] = f"{java_bin};{current_path}"

# Run environment configuration on module import
configure_spark_environment()

# ---------------------------------------------------------
# Database Configuration (MySQL)
# ---------------------------------------------------------
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "retailpulse_dw")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")

# Pipeline Configuration
APP_ENV = os.getenv("APP_ENV", "development")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "RetailPulseETL")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

def get_spark_session(app_name: str = SPARK_APP_NAME, master: str = SPARK_MASTER):
    """
    Build and return an optimized SparkSession for local batch execution.
    Configured with appropriate shuffle partitions and driver binding.
    """
    configure_spark_environment()
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(PROJECT_ROOT / ".spark_warehouse"))
    )

    return builder.getOrCreate()
