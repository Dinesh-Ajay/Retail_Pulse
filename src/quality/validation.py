"""
Data quality and schema validation rules for RetailPulse.
Validates structural integrity, required schema definitions, and non-emptiness.
"""

from typing import List, Tuple
from pyspark.sql import DataFrame

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "order_date",
    "region",
    "payment_method"
]

def validate_input_schema(df: DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate that all required columns exist in the DataFrame.
    Returns (is_valid, missing_columns).
    """
    existing_columns = set(df.columns)
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in existing_columns]

    if missing_columns:
        return False, missing_columns
    return True, []

def assert_schema_validity(df: DataFrame):
    """
    Raises a ValueError if required columns are missing from the ingested DataFrame.
    """
    is_valid, missing = validate_input_schema(df)
    if not is_valid:
        raise ValueError(
            f"Schema validation failed! Missing required columns: {', '.join(missing)}. "
            f"Expected columns: {', '.join(REQUIRED_COLUMNS)}"
        )

def check_dataset_not_empty(df: DataFrame) -> bool:
    """Returns True if the DataFrame contains at least one record, else False."""
    return df.take(1) != []
