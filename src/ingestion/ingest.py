"""
Data ingestion and Bronze layer implementation for RetailPulse.
Reads raw CSV files, enforces schema completeness, stamps audit metadata,
and preserves an immutable Bronze copy.
"""

from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
from config.settings import BRONZE_DIR, RAW_DATA_DIR
from src.quality.validation import assert_schema_validity
from src.utils.logging_config import setup_logger

logger = setup_logger("RetailPulse.Ingestion")

def ingest_raw_sales_csv(
    spark: SparkSession,
    input_path: Optional[Path] = None,
    save_bronze: bool = True
) -> DataFrame:
    """
    Ingests raw sales CSV into a PySpark DataFrame, performs initial schema
    integrity validation, adds ingestion audit metadata, and persists to Bronze layer.

    Architectural Note on Lineage vs Bronze Storage:
    Storing data in the Bronze layer provides raw data retention and an audit trail of incoming
    payloads at rest. However, this is distinct from full enterprise data lineage (which tracks
    fine-grained field transformations, upstream system provenance, and downstream catalog
    dependencies across DAG orchestrations like OpenLineage or Apache Atlas).
    """
    if input_path is None:
        input_path = RAW_DATA_DIR / "retail_sales.csv"

    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found at: {input_path}")

    logger.info("Reading raw CSV file from %s", input_path)

    # Read CSV treating all columns as strings initially to prevent silent parsing drops
    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(str(input_path))
    )

    # Validate required columns exist before proceeding
    assert_schema_validity(raw_df)

    # Attach operational audit metadata
    bronze_df = (
        raw_df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", lit(input_path.name))
    )

    if save_bronze:
        bronze_output_path = BRONZE_DIR / "raw_transactions"
        logger.info("Persisting Bronze raw dataset to %s", bronze_output_path)
        bronze_df.write.mode("overwrite").parquet(str(bronze_output_path))

    return bronze_df
