"""
Unit tests for Silver layer transformations, data cleansing, and quarantine logic.
"""

import pytest
from pyspark.sql.functions import current_timestamp, lit
from src.transformations.clean_data import clean_and_transform_sales

COLUMNS = [
    "order_id", "customer_id", "product_id", "product_name",
    "category", "quantity", "unit_price", "order_date",
    "region", "payment_method"
]

@pytest.mark.unit
def test_valid_transaction_cleaning(spark_session):
    raw_data = [
        (" 1001 ", " C101 ", "P101", " UltraBook Pro Laptop ", " Electronics ", "2", "65000.00", "2026-09-01", "South", "UPI")
    ]
    raw_df = spark_session.createDataFrame(raw_data, COLUMNS)
    bronze_df = raw_df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn("_source_file", lit("test.csv"))

    silver_df, rejected_df, rejection_metrics = clean_and_transform_sales(bronze_df)

    assert silver_df.count() == 1
    assert rejected_df.count() == 0

    row = silver_df.first()
    assert row["order_id"] == "1001"
    assert row["customer_id"] == "C101"
    assert row["product_name"] == "UltraBook Pro Laptop"
    assert row["quantity"] == 2
    assert float(row["unit_price"]) == 65000.00
    assert float(row["total_amount"]) == 130000.00

@pytest.mark.unit
def test_duplicate_order_id_handling(spark_session):
    raw_data = [
        ("1001", "C101", "P101", "Laptop", "Electronics", "1", "50000.00", "2026-09-01", "South", "UPI"),
        ("1001", "C101", "P101", "Laptop", "Electronics", "1", "50000.00", "2026-09-01", "South", "UPI"),
    ]
    raw_df = spark_session.createDataFrame(raw_data, COLUMNS)
    bronze_df = raw_df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn("_source_file", lit("test.csv"))

    silver_df, rejected_df, rejection_metrics = clean_and_transform_sales(bronze_df)

    assert silver_df.count() == 1
    assert rejected_df.count() == 1
    assert rejection_metrics.get("DUPLICATE_ORDER_ID") == 1

@pytest.mark.unit
def test_missing_customer_id_rejection(spark_session):
    raw_data = [
        ("1002", "", "P102", "Mouse", "Accessories", "1", "1200.00", "2026-09-01", "South", "Card"),
        ("1003", None, "P102", "Mouse", "Accessories", "1", "1200.00", "2026-09-01", "South", "Card")
    ]
    raw_df = spark_session.createDataFrame(raw_data, COLUMNS)
    bronze_df = raw_df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn("_source_file", lit("test.csv"))

    silver_df, rejected_df, rejection_metrics = clean_and_transform_sales(bronze_df)

    assert silver_df.count() == 0
    assert rejected_df.count() == 2
    assert rejection_metrics.get("MISSING_CUSTOMER_ID") == 2

@pytest.mark.unit
def test_invalid_order_date_rejection(spark_session):
    raw_data = [
        ("1004", "C104", "P102", "Mouse", "Accessories", "1", "1200.00", "2026-15-99", "South", "Card")
    ]
    raw_df = spark_session.createDataFrame(raw_data, COLUMNS)
    bronze_df = raw_df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn("_source_file", lit("test.csv"))

    silver_df, rejected_df, rejection_metrics = clean_and_transform_sales(bronze_df)

    assert silver_df.count() == 0
    assert rejected_df.count() == 1
    assert rejection_metrics.get("INVALID_ORDER_DATE") == 1

@pytest.mark.unit
def test_negative_quantity_and_zero_quantity_rejection(spark_session):
    raw_data = [
        ("1005", "C105", "P102", "Mouse", "Accessories", "-2", "1200.00", "2026-09-01", "South", "Card"),
        ("1006", "C106", "P102", "Mouse", "Accessories", "0", "1200.00", "2026-09-01", "South", "Card")
    ]
    raw_df = spark_session.createDataFrame(raw_data, COLUMNS)
    bronze_df = raw_df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn("_source_file", lit("test.csv"))

    silver_df, rejected_df, rejection_metrics = clean_and_transform_sales(bronze_df)

    assert silver_df.count() == 0
    assert rejected_df.count() == 2
    assert rejection_metrics.get("INVALID_QUANTITY") == 2

@pytest.mark.unit
def test_negative_unit_price_rejection(spark_session):
    raw_data = [
        ("1007", "C107", "P102", "Mouse", "Accessories", "1", "-50.00", "2026-09-01", "South", "Card")
    ]
    raw_df = spark_session.createDataFrame(raw_data, COLUMNS)
    bronze_df = raw_df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn("_source_file", lit("test.csv"))

    silver_df, rejected_df, rejection_metrics = clean_and_transform_sales(bronze_df)

    assert silver_df.count() == 0
    assert rejected_df.count() == 1
    assert rejection_metrics.get("INVALID_UNIT_PRICE") == 1

@pytest.mark.unit
def test_missing_product_name_rejection(spark_session):
    raw_data = [
        ("1008", "C108", "P102", "", "Accessories", "1", "1200.00", "2026-09-01", "South", "Card")
    ]
    raw_df = spark_session.createDataFrame(raw_data, COLUMNS)
    bronze_df = raw_df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn("_source_file", lit("test.csv"))

    silver_df, rejected_df, rejection_metrics = clean_and_transform_sales(bronze_df)

    assert silver_df.count() == 0
    assert rejected_df.count() == 1
    assert rejection_metrics.get("MISSING_PRODUCT_NAME") == 1
