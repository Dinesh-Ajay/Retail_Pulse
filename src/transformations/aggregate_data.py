"""
Gold layer business aggregation engine for RetailPulse.
Generates analytical datasets from the cleaned Silver layer using PySpark DataFrame
aggregations and Spark SQL functions.
"""

from typing import Dict, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct,
    round as spark_round, avg
)
from pyspark.sql.types import DecimalType
from src.utils.logging_config import setup_logger

logger = setup_logger("RetailPulse.Transformations.Aggregate")

def generate_gold_aggregations(silver_df: DataFrame) -> Dict[str, DataFrame]:
    """
    Computes all business aggregation datasets for the Gold layer.
    
    Returns a dictionary of named DataFrames:
        - daily_sales
        - product_performance
        - regional_sales
        - category_performance
        - payment_methods
    """
    logger.info("Computing Gold layer business analytics and aggregations")

    # 1. Daily Sales Summary
    daily_sales_df = (
        silver_df
        .groupBy("order_date")
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("quantity").alias("total_quantity"),
            spark_round(spark_sum("total_amount"), 2).cast(DecimalType(14, 2)).alias("total_revenue")
        )
        .orderBy("order_date")
    )

    # 2. Product Performance Summary
    product_performance_df = (
        silver_df
        .groupBy("product_id", "product_name", "category")
        .agg(
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_round(spark_sum("total_amount"), 2).cast(DecimalType(14, 2)).alias("total_revenue"),
            spark_round(spark_sum("total_amount") / spark_sum("quantity"), 2).cast(DecimalType(10, 2)).alias("average_selling_price")
        )
        .orderBy(col("total_revenue").desc())
    )

    # 3. Regional Sales Summary
    regional_sales_df = (
        silver_df
        .groupBy("region")
        .agg(
            count("order_id").alias("total_orders"),
            spark_round(spark_sum("total_amount"), 2).cast(DecimalType(14, 2)).alias("total_revenue")
        )
        .orderBy(col("total_revenue").desc())
    )

    # 4. Category Performance Summary
    category_performance_df = (
        silver_df
        .groupBy("category")
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_round(spark_sum("total_amount"), 2).cast(DecimalType(14, 2)).alias("total_revenue")
        )
        .orderBy(col("total_revenue").desc())
    )

    # 5. Payment Method Summary
    payment_methods_df = (
        silver_df
        .groupBy("payment_method")
        .agg(
            count("order_id").alias("transaction_count"),
            spark_round(spark_sum("total_amount"), 2).cast(DecimalType(14, 2)).alias("total_revenue")
        )
        .orderBy(col("total_revenue").desc())
    )

    logger.info("Gold aggregations computed successfully")
    return {
        "daily_sales": daily_sales_df,
        "product_performance": product_performance_df,
        "regional_sales": regional_sales_df,
        "category_performance": category_performance_df,
        "payment_methods": payment_methods_df
    }
