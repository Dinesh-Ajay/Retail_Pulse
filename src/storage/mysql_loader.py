"""
Idempotent MySQL Data Warehouse loader for RetailPulse.
Extracts normalized dimensional attributes and facts from the Silver DataFrame,
populates dim_product, dim_date, and fact_sales using transactional batch operations,
and records operational run metrics into pipeline_runs.
"""

from typing import Dict, Any, List, Tuple
from datetime import datetime
from pyspark.sql import DataFrame
from mysql.connector import Error as MySQLError
from src.database.connection import get_mysql_connection
from src.utils.logging_config import setup_logger

logger = setup_logger("RetailPulse.Storage.MySQL")

def load_silver_to_mysql(
    silver_df: DataFrame,
    run_id: str,
    metrics: Dict[str, Any]
) -> int:
    """
    Idempotently loads cleaned Silver sales records into the MySQL Star Schema Data Warehouse.
    Dimensions are populated and reconciled first, followed by transactional batch loading
    of fact_sales and recording run lineage in pipeline_runs.
    """
    conn = get_mysql_connection(use_database=True)
    cursor = conn.cursor()

    try:
        logger.info("Commencing transactional Star-Schema load for run_id=%s", run_id)

        # ----------------------------------------------------
        # 1. Populate dim_product
        # ----------------------------------------------------
        distinct_products = (
            silver_df
            .select("product_id", "product_name", "category")
            .distinct()
            .collect()
        )

        product_records = [
            (row["product_id"], row["product_name"], row["category"])
            for row in distinct_products
        ]

        product_upsert_sql = """
            INSERT INTO dim_product (product_id, product_name, category)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                category = VALUES(category)
        """
        cursor.executemany(product_upsert_sql, product_records)

        # Build in-memory lookup cache: product_id -> product_key
        cursor.execute("SELECT product_id, product_key FROM dim_product")
        product_key_map = {row[0]: row[1] for row in cursor.fetchall()}

        # ----------------------------------------------------
        # 2. Populate dim_date
        # ----------------------------------------------------
        distinct_dates = (
            silver_df
            .select("order_date")
            .distinct()
            .collect()
        )

        date_records = []
        for row in distinct_dates:
            dt = row["order_date"]
            if isinstance(dt, str):
                dt = datetime.strptime(dt, "%Y-%m-%d").date()
            date_key = int(dt.strftime("%Y%m%d"))
            date_records.append((
                date_key,
                dt,
                dt.day,
                dt.month,
                dt.strftime("%B"),
                (dt.month - 1) // 3 + 1,
                dt.year,
                dt.strftime("%A")
            ))

        date_upsert_sql = """
            INSERT INTO dim_date (date_key, full_date, day, month, month_name, quarter, year, weekday_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                day = VALUES(day)
        """
        cursor.executemany(date_upsert_sql, date_records)

        # ----------------------------------------------------
        # 3. Populate fact_sales (Batch Transactional Upsert)
        # ----------------------------------------------------
        silver_rows = (
            silver_df
            .select(
                "order_id", "customer_id", "product_id", "order_date",
                "quantity", "unit_price", "total_amount", "region", "payment_method"
            )
            .collect()
        )

        fact_batch = []
        for r in silver_rows:
            p_key = product_key_map.get(r["product_id"])
            dt = r["order_date"]
            if isinstance(dt, str):
                dt = datetime.strptime(dt, "%Y-%m-%d").date()
            d_key = int(dt.strftime("%Y%m%d"))

            fact_batch.append((
                r["order_id"],
                r["customer_id"],
                p_key,
                d_key,
                r["quantity"],
                float(r["unit_price"]),
                float(r["total_amount"]),
                r["region"],
                r["payment_method"]
            ))

        fact_upsert_sql = """
            INSERT INTO fact_sales (
                order_id, customer_id, product_key, date_key,
                quantity, unit_price, total_amount, region, payment_method
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                customer_id = VALUES(customer_id),
                product_key = VALUES(product_key),
                date_key = VALUES(date_key),
                quantity = VALUES(quantity),
                unit_price = VALUES(unit_price),
                total_amount = VALUES(total_amount),
                region = VALUES(region),
                payment_method = VALUES(payment_method)
        """
        cursor.executemany(fact_upsert_sql, fact_batch)

        # Commit all dimensional and fact operations atomically
        conn.commit()
        loaded_count = len(fact_batch)
        logger.info("Successfully committed %d fact records to fact_sales", loaded_count)
        return loaded_count

    except Exception as e:
        conn.rollback()
        logger.error("Transactional DW load failed, rolled back changes! Error: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()

def record_pipeline_run_metadata(report_data: Dict[str, Any]):
    """Records operational run status in the pipeline_runs table."""
    try:
        conn = get_mysql_connection(use_database=True)
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO pipeline_runs (
                run_id, start_time, end_time, status,
                input_count, valid_count, rejected_count, duplicate_count,
                duration_seconds, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                end_time = VALUES(end_time),
                status = VALUES(status),
                duration_seconds = VALUES(duration_seconds),
                error_message = VALUES(error_message)
        """

        # Convert ISO strings to MySQL-compatible datetime strings
        start_dt = datetime.fromisoformat(report_data["start_time"])
        end_dt = datetime.fromisoformat(report_data["end_time"])

        metrics = report_data.get("metrics", {})
        rejection_breakdown = report_data.get("rejection_breakdown", {})
        dup_count = rejection_breakdown.get("DUPLICATE_ORDER_ID", 0)

        cursor.execute(insert_sql, (
            report_data["run_id"],
            start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            report_data["status"],
            metrics.get("input_records", 0),
            metrics.get("silver_records", 0),
            metrics.get("rejected_records", 0),
            dup_count,
            report_data.get("duration_seconds", 0.0),
            report_data.get("error_message")
        ))

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Run operational metadata saved to pipeline_runs table")
    except Exception as e:
        logger.warning("Could not record run metadata to MySQL (non-fatal): %s", e)
