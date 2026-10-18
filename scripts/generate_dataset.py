"""
Synthetic retail sales dataset generator for RetailPulse.
Generates reproducible, deterministic CSV files with configurable sizes
and controlled data quality defects (duplicates, nulls, invalid dates, negative amounts).
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import RAW_DATA_DIR

PRODUCT_CATALOG = [
    ("P101", "UltraBook Pro Laptop", "Electronics", 65000.0),
    ("P102", "Ergonomic Wireless Mouse", "Accessories", 1200.0),
    ("P103", "Mechanical RGB Keyboard", "Accessories", 3500.0),
    ("P104", "Noise-Cancelling Headphones", "Electronics", 15000.0),
    ("P105", "4K Ultra-HD Monitor", "Electronics", 28000.0),
    ("P106", "Cotton Graphic T-Shirt", "Apparel", 800.0),
    ("P107", "Denim Slim Jeans", "Apparel", 2200.0),
    ("P108", "Automatic Espresso Maker", "Home & Kitchen", 18500.0),
    ("P109", "Non-Stick Cookware Set", "Home & Kitchen", 4500.0),
    ("P110", "Designing Data-Intensive Apps", "Books", 1800.0),
    ("P111", "Learning Spark 2nd Ed", "Books", 1400.0),
    ("P112", "USB-C Multiport Hub", "Accessories", 2500.0),
]

REGIONS = ["North", "South", "East", "West", "Central"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash"]

def generate_retail_dataset(
    count: int = 150,
    output_path: Path = None,
    seed: int = 42,
    invalid_ratio: float = 0.15
) -> Path:
    """
    Generate a deterministic synthetic CSV dataset containing both valid
    and deliberately defective records for data quality testing.
    """
    random.seed(seed)
    if output_path is None:
        output_path = RAW_DATA_DIR / "retail_sales.csv"

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    start_date = datetime(2026, 9, 1)
    records = []

    # Calculate defect quotas
    num_invalid = int(count * invalid_ratio)
    num_valid = count - num_invalid

    base_order_id = 1000

    # 1. Generate clean valid records
    for i in range(num_valid):
        order_id = str(base_order_id + i)
        cust_id = f"C{random.randint(101, 140):03d}"
        prod_id, prod_name, category, base_price = random.choice(PRODUCT_CATALOG)
        qty = random.randint(1, 5)
        # Add slight realistic price fluctuation (+- 5%)
        price = round(base_price * random.uniform(0.95, 1.05), 2)
        order_date = (start_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
        region = random.choice(REGIONS)
        payment = random.choice(PAYMENT_METHODS)

        records.append({
            "order_id": order_id,
            "customer_id": cust_id,
            "product_id": prod_id,
            "product_name": prod_name,
            "category": category,
            "quantity": str(qty),
            "unit_price": f"{price:.2f}",
            "order_date": order_date,
            "region": region,
            "payment_method": payment
        })

    # 2. Generate controlled invalid records
    defect_types = [
        "DUPLICATE_ORDER_ID",
        "MISSING_CUSTOMER_ID",
        "INVALID_ORDER_DATE",
        "NEGATIVE_QUANTITY",
        "ZERO_QUANTITY",
        "NEGATIVE_PRICE",
        "MISSING_PRODUCT_NAME",
        "MISSING_CATEGORY",
        "MISSING_REGION",
        "MISSING_PAYMENT_METHOD"
    ]

    for j in range(num_invalid):
        defect = defect_types[j % len(defect_types)]
        prod_id, prod_name, category, base_price = random.choice(PRODUCT_CATALOG)
        order_date = (start_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")

        rec = {
            "order_id": str(base_order_id + num_valid + j),
            "customer_id": f"C{random.randint(101, 140):03d}",
            "product_id": prod_id,
            "product_name": prod_name,
            "category": category,
            "quantity": str(random.randint(1, 4)),
            "unit_price": f"{base_price:.2f}",
            "order_date": order_date,
            "region": random.choice(REGIONS),
            "payment_method": random.choice(PAYMENT_METHODS)
        }

        if defect == "DUPLICATE_ORDER_ID":
            # Re-use an existing valid order_id to simulate an intentional duplicate
            if records:
                target_valid = records[random.randint(0, min(10, len(records) - 1))]
                rec["order_id"] = target_valid["order_id"]
        elif defect == "MISSING_CUSTOMER_ID":
            rec["customer_id"] = ""
        elif defect == "INVALID_ORDER_DATE":
            rec["order_date"] = "2026-14-99"  # Non-existent month & day
        elif defect == "NEGATIVE_QUANTITY":
            rec["quantity"] = "-3"
        elif defect == "ZERO_QUANTITY":
            rec["quantity"] = "0"
        elif defect == "NEGATIVE_PRICE":
            rec["unit_price"] = "-250.00"
        elif defect == "MISSING_PRODUCT_NAME":
            rec["product_name"] = ""
        elif defect == "MISSING_CATEGORY":
            rec["category"] = ""
        elif defect == "MISSING_REGION":
            rec["region"] = ""
        elif defect == "MISSING_PAYMENT_METHOD":
            rec["payment_method"] = ""

        records.append(rec)

    # Deterministic shuffle so defects are dispersed naturally
    random.shuffle(records)

    fieldnames = [
        "order_id", "customer_id", "product_id", "product_name",
        "category", "quantity", "unit_price", "order_date",
        "region", "payment_method"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    return output_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic retail sales dataset.")
    parser.add_argument("--count", type=int, default=150, help="Number of rows to generate (default: 150)")
    parser.add_argument("--output", type=str, default=str(RAW_DATA_DIR / "retail_sales.csv"), help="Output CSV path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--invalid-ratio", type=float, default=0.15, help="Proportion of defective records (0.0 to 1.0)")
    args = parser.parse_args()

    out_file = generate_retail_dataset(
        count=args.count,
        output_path=Path(args.output),
        seed=args.seed,
        invalid_ratio=args.invalid_ratio
    )
    print(f"Generated {args.count} retail sales records at: {out_file}")
