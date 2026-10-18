"""
Parquet storage manager for RetailPulse.
Handles columnar storage persistence with Snappy compression for Silver,
Gold, and Rejected datasets.
"""

from pathlib import Path
from typing import Dict
from pyspark.sql import DataFrame
from config.settings import SILVER_DIR, GOLD_DIR, REJECTED_DIR
from src.utils.logging_config import setup_logger

logger = setup_logger("RetailPulse.Storage.Parquet")

def save_silver_dataset(silver_df: DataFrame) -> str:
    """Persists cleaned Silver records to Parquet."""
    output_path = SILVER_DIR / "cleaned_sales"
    logger.info("Writing Silver Parquet dataset to %s", output_path)
    (
        silver_df
        .repartition(2)
        .write
        .mode("overwrite")
        .parquet(str(output_path))
    )
    return str(output_path)

def save_rejected_dataset(rejected_df: DataFrame) -> Dict[str, str]:
    """Persists quarantined rejected records to Parquet and CSV for auditing."""
    parquet_path = REJECTED_DIR / "quarantined_sales"
    csv_path = REJECTED_DIR / "quarantined_sales_csv"

    logger.info("Writing Rejected dataset to %s", parquet_path)
    (
        rejected_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(parquet_path))
    )

    # Also save as CSV for easy human debugging & auditor verification
    (
        rejected_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(csv_path))
    )

    return {
        "parquet": str(parquet_path),
        "csv": str(csv_path)
    }

def save_gold_datasets(gold_dfs: Dict[str, DataFrame]) -> Dict[str, str]:
    """Persists all Gold analytical DataFrames to individual Parquet directories."""
    saved_paths = {}
    for name, df in gold_dfs.items():
        out_path = GOLD_DIR / name
        logger.info("Writing Gold %s Parquet dataset to %s", name, out_path)
        (
            df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(str(out_path))
        )
        saved_paths[name] = str(out_path)

    return saved_paths
