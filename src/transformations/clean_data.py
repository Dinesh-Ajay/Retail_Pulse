"""
Silver layer cleaning, validation, and rejection routing engine for RetailPulse.
Transforms raw Bronze records into verified Silver records while quarantining
defective or duplicate rows with precise rejection reasons.
"""

from typing import Tuple, Dict
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, trim, to_date, when, current_timestamp,
    round as spark_round, row_number, count, lit
)
from pyspark.sql.types import (
    IntegerType, DecimalType, StringType, DateType
)
from src.utils.logging_config import setup_logger

logger = setup_logger("RetailPulse.Transformations.Clean")

def clean_and_transform_sales(bronze_df: DataFrame) -> Tuple[DataFrame, DataFrame, Dict[str, int]]:
    """
    Cleans raw transaction DataFrame, enforces data quality rules, isolates duplicates,
    calculates transaction financial totals, and partitions into Silver and Rejected datasets.

    Returns:
        (silver_df, rejected_df, rejection_counts)
    """
    logger.info("Initiating Silver layer data transformation and cleaning")

    # 1. Normalize column names (strip whitespace and lower-case)
    normalized_df = bronze_df
    for c in bronze_df.columns:
        normalized_df = normalized_df.withColumnRenamed(c, c.strip().lower())

    # 2. Trim string columns and attempt tolerant type casting
    trimmed_df = (
        normalized_df
        .withColumn("order_id_clean", trim(col("order_id")))
        .withColumn("customer_id_clean", trim(col("customer_id")))
        .withColumn("product_id_clean", trim(col("product_id")))
        .withColumn("product_name_clean", trim(col("product_name")))
        .withColumn("category_clean", trim(col("category")))
        .withColumn("region_clean", trim(col("region")))
        .withColumn("payment_method_clean", trim(col("payment_method")))
        .withColumn("parsed_date", to_date(trim(col("order_date")), "yyyy-MM-dd"))
        .withColumn("parsed_qty", trim(col("quantity")).cast(IntegerType()))
        .withColumn("parsed_price", trim(col("unit_price")).cast(DecimalType(10, 2)))
    )

    # 3. Evaluate atomic validation conditions
    # Rule hierarchy for single-record validation
    val_condition = (
        when(col("order_id_clean").isNull() | (col("order_id_clean") == ""), "MISSING_ORDER_ID")
        .when(col("customer_id_clean").isNull() | (col("customer_id_clean") == ""), "MISSING_CUSTOMER_ID")
        .when(col("product_id_clean").isNull() | (col("product_id_clean") == ""), "MISSING_PRODUCT_ID")
        .when(col("product_name_clean").isNull() | (col("product_name_clean") == ""), "MISSING_PRODUCT_NAME")
        .when(col("category_clean").isNull() | (col("category_clean") == ""), "MISSING_CATEGORY")
        .when(col("parsed_date").isNull(), "INVALID_ORDER_DATE")
        .when(col("parsed_qty").isNull() | (col("parsed_qty") <= 0), "INVALID_QUANTITY")
        .when(col("parsed_price").isNull() | (col("parsed_price") < 0), "INVALID_UNIT_PRICE")
        .when(col("region_clean").isNull() | (col("region_clean") == ""), "MISSING_REGION")
        .when(col("payment_method_clean").isNull() | (col("payment_method_clean") == ""), "MISSING_PAYMENT_METHOD")
        .otherwise(lit(None))
    )

    with_reasons_df = trimmed_df.withColumn("rule_defect", val_condition)

    # 4. Deterministic Duplicate Resolution using Window function
    # Treat order_id as unique business key. Retain the first occurrence of an otherwise valid order.
    # Note on Business Assumption: In real-world multi-item systems, an order can have multiple line items
    # (identified by composite key: order_id + order_item_id). In RetailPulse transaction grain,
    # each record represents an atomic transaction where order_id is assumed globally unique per run.
    order_window = Window.partitionBy("order_id_clean").orderBy(
        col("_ingestion_timestamp").asc(),
        col("product_id_clean").asc()
    )

    with_dup_df = with_reasons_df.withColumn("row_num", row_number().over(order_window))

    # Determine final rejection reason
    final_rejection_col = (
        when(col("rule_defect").isNotNull(), col("rule_defect"))
        .when(col("row_num") > 1, "DUPLICATE_ORDER_ID")
        .otherwise(lit(None))
    )

    classified_df = with_dup_df.withColumn("rejection_reason", final_rejection_col)

    # 5. Split into Silver (Valid) and Rejected datasets
    # Silver DataFrame: Valid records only
    silver_raw = classified_df.filter(col("rejection_reason").isNull())

    silver_df = (
        silver_raw
        .select(
            col("order_id_clean").alias("order_id"),
            col("customer_id_clean").alias("customer_id"),
            col("product_id_clean").alias("product_id"),
            col("product_name_clean").alias("product_name"),
            col("category_clean").alias("category"),
            col("parsed_qty").alias("quantity"),
            col("parsed_price").alias("unit_price"),
            # Calculated column: total_amount = quantity * unit_price
            spark_round(col("parsed_qty") * col("parsed_price"), 2).cast(DecimalType(12, 2)).alias("total_amount"),
            col("parsed_date").alias("order_date"),
            col("region_clean").alias("region"),
            col("payment_method_clean").alias("payment_method"),
            col("_ingestion_timestamp"),
            col("_source_file")
        )
    )

    # Rejected DataFrame: All quarantined records with diagnosis metadata
    rejected_raw = classified_df.filter(col("rejection_reason").isNotNull())

    rejected_df = (
        rejected_raw
        .select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("product_name"),
            col("category"),
            col("quantity"),
            col("unit_price"),
            col("order_date"),
            col("region"),
            col("payment_method"),
            col("rejection_reason"),
            current_timestamp().alias("rejection_timestamp"),
            col("_source_file")
        )
    )

    # Compute rejection counts for observability
    rejection_summary = rejected_df.groupBy("rejection_reason").count().collect()
    rejection_metrics = {row["rejection_reason"]: row["count"] for row in rejection_summary}

    logger.info("Silver transformations completed. Rejection breakdown: %s", rejection_metrics)
    return silver_df, rejected_df, rejection_metrics
