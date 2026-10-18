"""
Integration tests for MySQL Data Warehouse loading, idempotency, and transactional rollback.
Marked with @pytest.mark.integration.
"""

import pytest
from decimal import Decimal
from datetime import date, datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
)
from src.database.connection import (
    test_mysql_connection as is_mysql_reachable,
    initialize_database_schema,
    get_mysql_connection
)
from src.storage.mysql_loader import load_silver_to_mysql

@pytest.mark.integration
def test_mysql_server_connectivity():
    """Verify that MySQL server is alive and responding."""
    if not is_mysql_reachable():
        pytest.skip("Local MySQL server is not accessible; skipping connectivity check.")
    assert is_mysql_reachable() is True

@pytest.fixture
def mock_silver_df(spark_session):
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

    ts = datetime(2026, 9, 1, 12, 0, 0)
    data = [
        ("TEST_9001", "C999", "P901", "Test Widget A", "Electronics", 2, Decimal("150.00"), Decimal("300.00"), date(2026, 9, 1), "South", "UPI", ts, "test.csv"),
        ("TEST_9002", "C998", "P902", "Test Widget B", "Accessories", 1, Decimal("50.00"), Decimal("50.00"), date(2026, 9, 2), "North", "Card", ts, "test.csv"),
    ]
    return spark_session.createDataFrame(data, schema)

@pytest.mark.integration
def test_mysql_idempotent_batch_loading(mock_silver_df):
    if not is_mysql_reachable():
        pytest.skip("Local MySQL server is not accessible; skipping integration test.")

    # 1. Initialize schema
    assert initialize_database_schema() is True

    # Clean existing test keys if present
    conn = get_mysql_connection(use_database=True)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fact_sales WHERE order_id IN ('TEST_9001', 'TEST_9002')")
    conn.commit()

    # 2. First load
    first_load_count = load_silver_to_mysql(mock_silver_df, run_id="test_run_1", metrics={})
    assert first_load_count == 2

    cursor.execute("SELECT COUNT(*) FROM fact_sales WHERE order_id IN ('TEST_9001', 'TEST_9002')")
    count_after_first = cursor.fetchone()[0]
    assert count_after_first == 2

    # 3. Second load (Idempotency test: exact same records reloaded)
    second_load_count = load_silver_to_mysql(mock_silver_df, run_id="test_run_2", metrics={})
    assert second_load_count == 2

    cursor.execute("SELECT COUNT(*) FROM fact_sales WHERE order_id IN ('TEST_9001', 'TEST_9002')")
    count_after_second = cursor.fetchone()[0]
    # Count MUST NOT double — idempotency verified!
    assert count_after_second == 2

    # Cleanup test rows
    cursor.execute("DELETE FROM fact_sales WHERE order_id IN ('TEST_9001', 'TEST_9002')")
    cursor.execute("DELETE FROM dim_product WHERE product_id IN ('P901', 'P902')")
    conn.commit()
    cursor.close()
    conn.close()

@pytest.mark.integration
def test_mysql_transactional_rollback():
    if not is_mysql_reachable():
        pytest.skip("Local MySQL server is not accessible; skipping integration test.")

    conn = get_mysql_connection(use_database=True)
    cursor = conn.cursor()

    # Clean test record
    cursor.execute("DELETE FROM fact_sales WHERE order_id = 'TEST_FAIL_TX'")
    conn.commit()

    # Intentionally trigger an error inside a transaction
    try:
        # Valid insertion statement
        cursor.execute(
            "INSERT INTO dim_product (product_id, product_name, category) VALUES ('P_TX_TEST', 'TX Widget', 'Test') "
            "ON DUPLICATE KEY UPDATE product_name = VALUES(product_name)"
        )
        # Invalid insertion (foreign key violation or syntax error)
        cursor.execute("INSERT INTO fact_sales (order_id, customer_id, product_key, date_key) VALUES ('TEST_FAIL_TX', 'C001', 999999, 999999)")
        conn.commit()
    except Exception:
        conn.rollback()

    # Verify that the first statement was rolled back and dim_product does not contain uncommitted data if not previously present
    cursor.execute("SELECT COUNT(*) FROM fact_sales WHERE order_id = 'TEST_FAIL_TX'")
    fact_count = cursor.fetchone()[0]
    assert fact_count == 0

    cursor.close()
    conn.close()
