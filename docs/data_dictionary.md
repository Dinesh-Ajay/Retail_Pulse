# RetailPulse — Data Dictionary & Schema Specification

## 1. Source CSV Dataset & Bronze Layer Schema

The incoming data represents raw point-of-sale and online e-commerce transactions.

| Column Name | Source Type | Bronze Type | Nullable | Description | Example |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `order_id` | String | String | No | Transaction identifier | `"1001"` |
| `customer_id` | String | String | No | Customer account code | `"C105"` |
| `product_id` | String | String | No | Product catalog SKU | `"P101"` |
| `product_name` | String | String | No | Product description | `"UltraBook Pro Laptop"` |
| `category` | String | String | No | High-level product department | `"Electronics"` |
| `quantity` | String | String | No | Units ordered | `"2"` |
| `unit_price` | String | String | No | Unit selling price | `"65000.00"` |
| `order_date` | String | String | No | Transaction date (`YYYY-MM-DD`) | `"2026-09-01"` |
| `region` | String | String | No | Delivery sales territory | `"South"` |
| `payment_method` | String | String | No | Tender method | `"UPI"` |
| `_ingestion_timestamp` | System | Timestamp | No | System UTC timestamp when recorded | `2026-10-10 00:14:56` |
| `_source_file` | System | String | No | Input source filename | `"retail_sales.csv"` |

---

## 2. Silver Cleansed Dataset Schema (`data/silver/cleaned_sales`)

| Column Name | Spark Data Type | Nullable | Constraints & Transformations | Description |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | `StringType` | No | Trimmed, unique per run | Cleaned transaction identifier |
| `customer_id` | `StringType` | No | Trimmed, non-empty | Verified customer identifier |
| `product_id` | `StringType` | No | Trimmed, non-empty | Verified product code |
| `product_name` | `StringType` | No | Trimmed, non-empty | Clean product title |
| `category` | `StringType` | No | Trimmed, non-empty | Standard category |
| `quantity` | `IntegerType` | No | `quantity > 0` | Verified positive item count |
| `unit_price` | `DecimalType(10, 2)` | No | `unit_price >= 0.00` | Unit sales price |
| `total_amount` | `DecimalType(12, 2)` | No | `round(qty * price, 2)` | Line item financial total |
| `order_date` | `DateType` | No | Valid calendar date | Date of transaction |
| `region` | `StringType` | No | Non-empty | Sales geographic territory |
| `payment_method` | `StringType` | No | Non-empty | Mode of payment |
| `_ingestion_timestamp`| `TimestampType`| No | Preserved from Bronze | Audit timestamp |
| `_source_file` | `StringType` | No | Preserved from Bronze | Source file origin |

---

## 3. Quarantined Rejected Schema (`data/rejected/`)

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `order_id` | `StringType` | Raw order ID |
| `customer_id` | `StringType` | Raw customer ID |
| `product_id` | `StringType` | Raw product ID |
| `product_name` | `StringType` | Raw product name |
| `category` | `StringType` | Raw category |
| `quantity` | `StringType` | Raw quantity |
| `unit_price` | `StringType` | Raw unit price |
| `order_date` | `StringType` | Raw order date |
| `region` | `StringType` | Raw region |
| `payment_method` | `StringType` | Raw payment method |
| `rejection_reason` | `StringType` | Rejection code (`MISSING_CUSTOMER_ID`, `INVALID_ORDER_DATE`, `DUPLICATE_ORDER_ID`, `INVALID_QUANTITY`, `INVALID_UNIT_PRICE`, `MISSING_PRODUCT_NAME`) |
| `rejection_timestamp` | `TimestampType` | UTC timestamp when quarantined |
| `_source_file` | `StringType` | Source file origin |

---

## 4. MySQL Data Warehouse Dimensional Schema (`retailpulse_dw`)

### `dim_product`
- `product_key` (INT, AUTO_INCREMENT, PK): Synthetic surrogate key.
- `product_id` (VARCHAR(50), UNIQUE, NOT NULL): Natural SKU code.
- `product_name` (VARCHAR(255), NOT NULL): Product display name.
- `category` (VARCHAR(100), NOT NULL, INDEXED): Merchandise hierarchy.
- `created_at` (TIMESTAMP): Dimension insertion timestamp.

### `dim_date`
- `date_key` (INT, PK): Date key in `YYYYMMDD` format (e.g. `20260901`).
- `full_date` (DATE, UNIQUE, NOT NULL): Standard date value.
- `day` (INT, NOT NULL): Day of month (1–31).
- `month` (INT, NOT NULL): Month number (1–12).
- `month_name` (VARCHAR(20), NOT NULL): e.g., "September".
- `quarter` (INT, NOT NULL): Quarter (1–4).
- `year` (INT, NOT NULL): Calendar year (e.g. 2026).
- `weekday_name` (VARCHAR(20), NOT NULL): e.g., "Tuesday".

### `fact_sales`
- `order_id` (VARCHAR(50), PK): Unique transaction identifier.
- `customer_id` (VARCHAR(50), NOT NULL, INDEXED): Customer key.
- `product_key` (INT, NOT NULL, FK -> `dim_product`): Product surrogate key.
- `date_key` (INT, NOT NULL, FK -> `dim_date`): Date surrogate key.
- `quantity` (INT, NOT NULL): Quantity sold.
- `unit_price` (DECIMAL(10, 2), NOT NULL): Realized unit price.
- `total_amount` (DECIMAL(12, 2), NOT NULL): Total transaction value.
- `region` (VARCHAR(50), NOT NULL, INDEXED): Sales territory.
- `payment_method` (VARCHAR(50), NOT NULL, INDEXED): Payment method.
- `loaded_at` (TIMESTAMP): Warehouse load audit timestamp.

### `pipeline_runs`
- `run_id` (VARCHAR(100), PK): Execution identifier.
- `start_time` (TIMESTAMP): Pipeline start timestamp.
- `end_time` (TIMESTAMP): Pipeline completion timestamp.
- `status` (VARCHAR(50)): `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`.
- `input_count` (INT): Total rows ingested.
- `valid_count` (INT): Rows loaded into Silver / Warehouse.
- `rejected_count` (INT): Rows quarantined.
- `duplicate_count` (INT): Duplicate transactions intercepted.
- `duration_seconds` (DECIMAL(10, 3)): Wall-clock runtime.
- `error_message` (TEXT, NULL): Diagnostic error message if failed.
