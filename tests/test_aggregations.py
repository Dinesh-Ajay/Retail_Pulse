"""
Unit tests for Gold layer business aggregations and KPI calculations.
"""

import pytest
from decimal import Decimal
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
)
from datetime import date, datetime
from src.transformations.aggregate_data import generate_gold_aggregations

@pytest.fixture
def clean_silver_test_df(spark_session):
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("total_amount", DecimalType(12, 2), False),
        StructField("order_date", DateType(), False),
        StructField("region", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("_ingestion_timestamp", TimestampType(), True),
        StructField("_source_file", StringType(), True),
    ])

    ts = datetime(2026, 9, 1, 10, 0, 0)
    data = [
        ("1001", "C101", "P101", "Laptop", "Electronics", 1, Decimal("50000.00"), Decimal("50000.00"), date(2026, 9, 1), "South", "UPI", ts, "test.csv"),
        ("1002", "C102", "P102", "Mouse", "Accessories", 2, Decimal("1000.00"), Decimal("2000.00"), date(2026, 9, 1), "South", "Card", ts, "test.csv"),
        ("1003", "C103", "P101", "Laptop", "Electronics", 2, Decimal("50000.00"), Decimal("100000.00"), date(2026, 9, 2), "North", "UPI", ts, "test.csv"),
    ]
    return spark_session.createDataFrame(data, schema)

@pytest.mark.unit
def test_daily_sales_aggregation(clean_silver_test_df):
    gold = generate_gold_aggregations(clean_silver_test_df)
    daily_df = gold["daily_sales"].collect()

    assert len(daily_df) == 2

    # 2026-09-01: 2 orders, 3 units, 52000.00 revenue
    day1 = [r for r in daily_df if str(r["order_date"]) == "2026-09-01"][0]
    assert day1["total_orders"] == 2
    assert day1["total_quantity"] == 3
    assert float(day1["total_revenue"]) == 52000.00

    # 2026-09-02: 1 order, 2 units, 100000.00 revenue
    day2 = [r for r in daily_df if str(r["order_date"]) == "2026-09-02"][0]
    assert day2["total_orders"] == 1
    assert day2["total_quantity"] == 2
    assert float(day2["total_revenue"]) == 100000.00

@pytest.mark.unit
def test_product_performance_aggregation(clean_silver_test_df):
    gold = generate_gold_aggregations(clean_silver_test_df)
    prod_df = gold["product_performance"].collect()

    assert len(prod_df) == 2

    # Laptop (P101): 3 units sold, 150000.00 total revenue, avg price 50000.00
    laptop = [r for r in prod_df if r["product_id"] == "P101"][0]
    assert laptop["total_quantity_sold"] == 3
    assert float(laptop["total_revenue"]) == 150000.00
    assert float(laptop["average_selling_price"]) == 50000.00

@pytest.mark.unit
def test_financial_reconciliation_across_gold_views(clean_silver_test_df):
    gold = generate_gold_aggregations(clean_silver_test_df)

    daily_rev = sum([float(r["total_revenue"]) for r in gold["daily_sales"].collect()])
    prod_rev = sum([float(r["total_revenue"]) for r in gold["product_performance"].collect()])
    reg_rev = sum([float(r["total_revenue"]) for r in gold["regional_sales"].collect()])
    cat_rev = sum([float(r["total_revenue"]) for r in gold["category_performance"].collect()])

    # All dimension views must reconcile to the exact same total revenue
    expected_total = 152000.00
    assert daily_rev == expected_total
    assert prod_rev == expected_total
    assert reg_rev == expected_total
    assert cat_rev == expected_total
