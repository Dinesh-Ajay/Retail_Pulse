-- ========================================================
-- RetailPulse Data Warehouse: DDL Schema Definition
-- Star-Schema Analytical Model + Operational Monitoring
-- ========================================================

USE retailpulse_dw;

-- --------------------------------------------------------
-- 1. Product Dimension (dim_product)
-- Natural key: product_id
-- Surrogate key: product_key (Auto-increment integer)
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_product_category (category)
) ENGINE=InnoDB;

-- --------------------------------------------------------
-- 2. Date Dimension (dim_date)
-- Surrogate key: date_key (Format: YYYYMMDD integer)
-- Allows high-performance partition/index pruning in analytical queries
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    weekday_name VARCHAR(20) NOT NULL,
    INDEX idx_date_year_month (year, month)
) ENGINE=InnoDB;

-- --------------------------------------------------------
-- 3. Sales Fact Table (fact_sales)
-- Grain: One record per atomic valid customer transaction
-- Natural unique key: order_id
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_key INT NOT NULL,
    date_key INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    region VARCHAR(50) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    CONSTRAINT fk_fact_sales_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    INDEX idx_fact_customer (customer_id),
    INDEX idx_fact_product (product_key),
    INDEX idx_fact_date (date_key),
    INDEX idx_fact_region (region),
    INDEX idx_fact_payment (payment_method)
) ENGINE=InnoDB;

-- --------------------------------------------------------
-- 4. Pipeline Operational Metadata (pipeline_runs)
-- Tracks run lineage, execution time, and reconciliation statistics
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(100) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    input_count INT NOT NULL,
    valid_count INT NOT NULL,
    rejected_count INT NOT NULL,
    duplicate_count INT NOT NULL,
    duration_seconds DECIMAL(10, 3) NOT NULL,
    error_message TEXT NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;
