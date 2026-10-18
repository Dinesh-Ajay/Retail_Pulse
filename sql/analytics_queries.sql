-- ========================================================
-- RetailPulse Data Warehouse: SQL Analytics Queries
-- Designed for Star-Schema Fact & Dimension Analysis
-- ========================================================

USE retailpulse_dw;

-- --------------------------------------------------------
-- Query 1: Total Revenue Across All Transactions
-- --------------------------------------------------------
SELECT 
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM fact_sales;

-- --------------------------------------------------------
-- Query 2: Total Number of Transactions (Orders)
-- --------------------------------------------------------
SELECT 
    COUNT(order_id) AS total_transactions
FROM fact_sales;

-- --------------------------------------------------------
-- Query 3: Total Units Sold
-- --------------------------------------------------------
SELECT 
    SUM(quantity) AS total_units_sold
FROM fact_sales;

-- --------------------------------------------------------
-- Query 4: Average Transaction Value (ATV / AOV)
-- --------------------------------------------------------
SELECT 
    ROUND(AVG(total_amount), 2) AS average_transaction_value
FROM fact_sales;

-- --------------------------------------------------------
-- Query 5: Top 5 Products by Revenue
-- Exploit join with dim_product on indexed product_key
-- --------------------------------------------------------
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_key, p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 5;

-- --------------------------------------------------------
-- Query 6: Top 5 Products by Quantity Sold
-- --------------------------------------------------------
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.quantity) AS total_quantity_sold,
    ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_key, p.product_id, p.product_name, p.category
ORDER BY total_quantity_sold DESC
LIMIT 5;

-- --------------------------------------------------------
-- Query 7: Daily Revenue Breakdown
-- Joins fact_sales with dim_date on date_key
-- --------------------------------------------------------
SELECT 
    d.full_date,
    COUNT(f.order_id) AS total_orders,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.total_amount), 2) AS daily_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.date_key, d.full_date
ORDER BY d.full_date ASC;

-- --------------------------------------------------------
-- Query 8: Monthly Revenue Breakdown
-- --------------------------------------------------------
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(f.order_id) AS total_orders,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.total_amount), 2) AS monthly_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year ASC, d.month ASC;

-- --------------------------------------------------------
-- Query 9: Revenue by Geographic Region
-- --------------------------------------------------------
SELECT 
    region,
    COUNT(order_id) AS total_orders,
    SUM(quantity) AS units_sold,
    ROUND(SUM(total_amount), 2) AS regional_revenue
FROM fact_sales
GROUP BY region
ORDER BY regional_revenue DESC;

-- --------------------------------------------------------
-- Query 10: Revenue by Product Category
-- --------------------------------------------------------
SELECT 
    p.category,
    COUNT(f.order_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.total_amount), 2) AS category_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY category_revenue DESC;

-- --------------------------------------------------------
-- Query 11: Revenue by Payment Method
-- --------------------------------------------------------
SELECT 
    payment_method,
    COUNT(order_id) AS transaction_count,
    ROUND(SUM(total_amount), 2) AS payment_revenue,
    ROUND(AVG(total_amount), 2) AS avg_ticket_size
FROM fact_sales
GROUP BY payment_method
ORDER BY payment_revenue DESC;

-- --------------------------------------------------------
-- Query 12: Products with Zero Sales (Orphaned / Low-turnover catalog items)
-- Identifies products in catalog that have not recorded any fact sales
-- --------------------------------------------------------
SELECT 
    p.product_id,
    p.product_name,
    p.category
FROM dim_product p
LEFT JOIN fact_sales f ON p.product_key = f.product_key
WHERE f.order_id IS NULL;

-- --------------------------------------------------------
-- Query 13: Fact Record Uniqueness & Duplicate Audit Check
-- Reconciles that no order_id has multiple occurrences in fact_sales
-- --------------------------------------------------------
SELECT 
    order_id,
    COUNT(*) AS occurrence_count
FROM fact_sales
GROUP BY order_id
HAVING COUNT(*) > 1;

-- --------------------------------------------------------
-- Query 14: Data-Quality Foreign Key Reconciliation
-- Checks for referential integrity violations (should return 0 rows)
-- --------------------------------------------------------
SELECT 
    COUNT(*) AS orphaned_fact_rows
FROM fact_sales f
LEFT JOIN dim_product p ON f.product_key = p.product_key
LEFT JOIN dim_date d ON f.date_key = d.date_key
WHERE p.product_key IS NULL OR d.date_key IS NULL;

-- --------------------------------------------------------
-- Query 15: Regional Revenue Comparison with Window Functions
-- Calculates revenue, regional rank, and percentage contribution to overall business
-- --------------------------------------------------------
SELECT 
    region,
    ROUND(SUM(total_amount), 2) AS regional_revenue,
    RANK() OVER (ORDER BY SUM(total_amount) DESC) AS revenue_rank,
    ROUND(
        100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (),
        2
    ) AS revenue_percentage_share
FROM fact_sales
GROUP BY region
ORDER BY revenue_rank ASC;
