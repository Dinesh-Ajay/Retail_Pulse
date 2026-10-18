"""
Unit tests for schema integrity, empty datasets, and data quality validation.
"""

import pytest
from src.quality.validation import validate_input_schema, assert_schema_validity, check_dataset_not_empty

@pytest.mark.unit
def test_schema_validity_on_complete_columns(spark_session):
    cols = ["order_id", "customer_id", "product_id", "product_name",
            "category", "quantity", "unit_price", "order_date",
            "region", "payment_method"]
    df = spark_session.createDataFrame([], f"{', '.join([c + ' STRING' for c in cols])}")
    is_valid, missing = validate_input_schema(df)
    assert is_valid is True
    assert len(missing) == 0

@pytest.mark.unit
def test_schema_validity_missing_required_column_raises(spark_session):
    # Omit payment_method and unit_price
    df = spark_session.createDataFrame([], "order_id STRING, customer_id STRING, product_id STRING")
    is_valid, missing = validate_input_schema(df)
    assert is_valid is False
    assert "unit_price" in missing
    assert "payment_method" in missing

    with pytest.raises(ValueError) as excinfo:
        assert_schema_validity(df)
    assert "Schema validation failed" in str(excinfo.value)

@pytest.mark.unit
def test_check_dataset_not_empty(spark_session):
    df_empty = spark_session.createDataFrame([], "id INT")
    assert check_dataset_not_empty(df_empty) is False

    df_not_empty = spark_session.createDataFrame([(1,)], ["id"])
    assert check_dataset_not_empty(df_not_empty) is True
