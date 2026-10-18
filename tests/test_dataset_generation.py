"""
Unit tests for synthetic dataset generation logic.
"""

import csv
import pytest
from pathlib import Path
from scripts.generate_dataset import generate_retail_dataset

@pytest.mark.unit
def test_dataset_generation_row_count_and_columns(tmp_path):
    output_csv = tmp_path / "test_sales.csv"
    generated_path = generate_retail_dataset(count=120, output_path=output_csv, seed=123, invalid_ratio=0.10)

    assert generated_path.exists()

    with open(generated_path, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 120

        expected_columns = {
            "order_id", "customer_id", "product_id", "product_name",
            "category", "quantity", "unit_price", "order_date",
            "region", "payment_method"
        }
        assert set(reader[0].keys()) == expected_columns

@pytest.mark.unit
def test_dataset_generation_reproducibility(tmp_path):
    file1 = tmp_path / "run1.csv"
    file2 = tmp_path / "run2.csv"

    generate_retail_dataset(count=50, output_path=file1, seed=42)
    generate_retail_dataset(count=50, output_path=file2, seed=42)

    with open(file1, "r", encoding="utf-8") as f1, open(file2, "r", encoding="utf-8") as f2:
        assert f1.read() == f2.read()
