"""
Pytest configuration and shared test fixtures for RetailPulse.
Initializes isolated local SparkSession and provides sample data fixtures.
"""

import os
import sys
from pathlib import Path
import pytest

# Ensure project root is available to tests
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import configure_spark_environment

@pytest.fixture(scope="session")
def spark_session():
    """Provides a thread-safe, local SparkSession fixture for all unit tests."""
    configure_spark_environment()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("RetailPulseUnitTest")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()

@pytest.fixture
def sample_raw_records():
    """Provides representative mock records covering valid and invalid conditions."""
    return [
        # Valid transactions
        ("1001", "C101", "P101", "UltraBook Pro Laptop", "Electronics", "1", "65000.00", "2026-09-01", "South", "UPI"),
        ("1002", "C102", "P102", "Ergonomic Wireless Mouse", "Accessories", "2", "1200.00", "2026-09-01", "South", "Card"),
        ("1003", "C103", "P103", "Mechanical RGB Keyboard", "Accessories", "3", "3500.00", "2026-09-02", "North", "Cash"),
        # Duplicate transaction of 1001
        ("1001", "C101", "P101", "UltraBook Pro Laptop", "Electronics", "1", "65000.00", "2026-09-01", "South", "UPI"),
        # Missing customer
        ("1004", "", "P101", "UltraBook Pro Laptop", "Electronics", "1", "65000.00", "2026-09-02", "West", "UPI"),
        # Invalid date
        ("1005", "C105", "P102", "Ergonomic Wireless Mouse", "Accessories", "1", "1200.00", "2026-99-99", "West", "Card"),
        # Negative quantity
        ("1006", "C106", "P103", "Mechanical RGB Keyboard", "Accessories", "-2", "3500.00", "2026-09-03", "East", "Cash"),
        # Negative price
        ("1007", "C107", "P101", "UltraBook Pro Laptop", "Electronics", "1", "-500.00", "2026-09-03", "Central", "UPI"),
        # Missing product name
        ("1008", "C108", "P102", "", "Accessories", "1", "1200.00", "2026-09-04", "North", "Card"),
    ]
