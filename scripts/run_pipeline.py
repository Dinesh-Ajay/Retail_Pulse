"""
Main execution pipeline entry point for RetailPulse.
Orchestrates:
  Raw Ingestion -> Bronze Layer -> Silver Cleansing & Validation ->
  Gold Business Aggregations -> Parquet Persistence -> MySQL Star Schema Loading ->
  Observability & Reporting.
"""

import argparse
import sys
import time
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    RAW_DATA_DIR, BRONZE_DIR, SILVER_DIR,
    GOLD_DIR, REJECTED_DIR, get_spark_session
)
from src.utils.logging_config import setup_logger
from src.utils.reporting import PipelineReporter
from scripts.generate_dataset import generate_retail_dataset
from src.ingestion.ingest import ingest_raw_sales_csv
from src.transformations.clean_data import clean_and_transform_sales
from src.transformations.aggregate_data import generate_gold_aggregations
from src.storage.parquet_storage import (
    save_silver_dataset, save_rejected_dataset, save_gold_datasets
)
from src.database.connection import (
    test_mysql_connection, initialize_database_schema
)
from src.storage.mysql_loader import (
    load_silver_to_mysql, record_pipeline_run_metadata
)

logger = setup_logger("RetailPulse.Pipeline")

def run_pipeline(
    input_path: Path = None,
    generate_sample: bool = False,
    sample_count: int = 150,
    skip_mysql: bool = False
) -> int:
    """
    Executes the end-to-end RetailPulse Big Data pipeline.
    Returns 0 on success, 1 on fatal failure.
    """
    reporter = PipelineReporter()
    start_wall_time = time.time()
    spark = None

    logger.info("=" * 70)
    logger.info("Starting RetailPulse ETL Pipeline [Run ID: %s]", reporter.run_id)
    logger.info("=" * 70)

    try:
        # Step 0: Input file determination & optional sample dataset generation
        if input_path is None:
            input_path = RAW_DATA_DIR / "retail_sales.csv"

        input_path = Path(input_path)

        if generate_sample or not input_path.exists():
            logger.info("Generating fresh synthetic dataset (%d rows) at %s", sample_count, input_path)
            generate_retail_dataset(count=sample_count, output_path=input_path)
            reporter.record_stage("dataset_generation", "SUCCESS")
        else:
            logger.info("Using existing source dataset at %s", input_path)

        # Step 1: Initialize PySpark Session
        logger.info("Initializing Apache Spark session...")
        spark = get_spark_session()
        logger.info("Spark session created successfully (Version: %s)", spark.version)
        reporter.record_stage("spark_init", "SUCCESS")

        # Step 2: Ingestion & Bronze Layer
        logger.info("Phase 1: Ingesting raw dataset into Bronze layer...")
        bronze_df = ingest_raw_sales_csv(spark, input_path=input_path, save_bronze=True)
        input_count = bronze_df.count()
        logger.info("Bronze layer completed. Total raw records ingested: %d", input_count)
        reporter.record_metric("input_records", input_count)
        reporter.record_metric("bronze_records", input_count)
        reporter.set_output_location("bronze", str(BRONZE_DIR / "raw_transactions"))
        reporter.record_stage("bronze_layer", "SUCCESS")

        # Step 3: Transformation, Cleaning & Silver Layer
        logger.info("Phase 2: Cleaning, validating, and transforming data for Silver layer...")
        silver_df, rejected_df, rejection_metrics = clean_and_transform_sales(bronze_df)
        silver_count = silver_df.count()
        rejected_count = rejected_df.count()
        duplicate_count = rejection_metrics.get("DUPLICATE_ORDER_ID", 0)

        logger.info("Silver valid records: %d | Quarantined rejected records: %d (Duplicates: %d)",
                    silver_count, rejected_count, duplicate_count)

        silver_path = save_silver_dataset(silver_df)
        rejected_paths = save_rejected_dataset(rejected_df)

        reporter.record_metric("silver_records", silver_count)
        reporter.record_metric("rejected_records", rejected_count)
        reporter.record_metric("duplicate_records", duplicate_count)
        reporter.set_rejection_breakdown(rejection_metrics)
        reporter.set_output_location("silver", silver_path)
        reporter.set_output_location("rejected_parquet", rejected_paths["parquet"])
        reporter.set_output_location("rejected_csv", rejected_paths["csv"])
        reporter.record_stage("silver_layer", "SUCCESS")

        # Step 4: Gold Layer Business Aggregations
        logger.info("Phase 3: Computing Gold layer business analytics and KPIs...")
        gold_dfs = generate_gold_aggregations(silver_df)
        gold_paths = save_gold_datasets(gold_dfs)

        reporter.record_metric("gold_daily_records", gold_dfs["daily_sales"].count())
        reporter.record_metric("gold_product_records", gold_dfs["product_performance"].count())
        reporter.record_metric("gold_regional_records", gold_dfs["regional_sales"].count())
        reporter.record_metric("gold_category_records", gold_dfs["category_performance"].count())
        reporter.record_metric("gold_payment_records", gold_dfs["payment_methods"].count())
        for k, v in gold_paths.items():
            reporter.set_output_location(f"gold_{k}", v)
        reporter.record_stage("gold_layer", "SUCCESS")

        # Step 5: MySQL Data Warehouse Loading
        if skip_mysql:
            logger.info("MySQL DW loading skipped as requested (--skip-mysql).")
            reporter.record_stage("mysql_warehouse", "SKIPPED_USER_FLAG")
        else:
            logger.info("Phase 4: Checking MySQL Data Warehouse availability...")
            if test_mysql_connection():
                logger.info("MySQL connection established. Initializing schema if needed...")
                initialize_database_schema()
                facts_loaded = load_silver_to_mysql(silver_df, reporter.run_id, reporter.metrics)
                reporter.record_metric("mysql_facts_loaded", facts_loaded)
                reporter.record_stage("mysql_warehouse", "SUCCESS")
                logger.info("MySQL Data Warehouse successfully updated (%d fact rows).", facts_loaded)
            else:
                logger.warning("MySQL instance unreachable or credentials invalid. Skipping DW load.")
                reporter.record_stage("mysql_warehouse", "SKIPPED_UNAVAILABLE")

        # Step 6: Finalize Reporting & Observability
        pipeline_status = "SUCCESS" if (skip_mysql or reporter.stages.get("mysql_warehouse") in ["SUCCESS", "SKIPPED_USER_FLAG"]) else "PARTIAL_SUCCESS"
        final_report = reporter.finalize(status=pipeline_status)

        # Record operational lineage into MySQL pipeline_runs if DB is active
        if reporter.stages.get("mysql_warehouse") == "SUCCESS":
            record_pipeline_run_metadata(final_report)

        duration = time.time() - start_wall_time
        logger.info("=" * 70)
        logger.info("RetailPulse Pipeline execution finished: %s in %.2f seconds", pipeline_status, duration)
        logger.info("Execution report persisted at: reports/latest_run.json")
        logger.info("=" * 70)
        return 0

    except Exception as exc:
        logger.exception("FATAL: Pipeline execution halted with unhandled error: %s", exc)
        reporter.finalize(status="FAILED", error_message=str(exc))
        return 1

    finally:
        if spark is not None:
            logger.info("Stopping Spark session and liberating JVM resources...")
            spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the RetailPulse End-to-End Data Pipeline.")
    parser.add_argument("--input", type=str, default=None, help="Path to input sales CSV")
    parser.add_argument("--generate-sample", action="store_true", help="Generate fresh synthetic sample dataset")
    parser.add_argument("--sample-count", type=int, default=150, help="Number of records to generate if sample enabled")
    parser.add_argument("--skip-mysql", action="store_true", help="Skip loading into MySQL Data Warehouse")

    args = parser.parse_args()

    exit_code = run_pipeline(
        input_path=Path(args.input) if args.input else None,
        generate_sample=args.generate_sample,
        sample_count=args.sample_count,
        skip_mysql=args.skip_mysql
    )
    sys.exit(exit_code)
