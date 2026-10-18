"""
MySQL connection manager and database schema initialiser for RetailPulse.
Manages connection lifecycle, transaction contexts, and graceful offline detection.
"""

from pathlib import Path
from typing import Optional
import mysql.connector
from mysql.connector import Error as MySQLError
from config.settings import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE,
    MYSQL_USER, MYSQL_PASSWORD, SQL_DIR
)
from src.utils.logging_config import setup_logger

logger = setup_logger("RetailPulse.Database.Connection")

def get_mysql_connection(use_database: bool = True):
    """
    Returns an active MySQL connection instance.
    If use_database is False, connects to the server without selecting the target database.
    """
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE if use_database else None,
            charset="utf8mb4",
            autocommit=False,
            connect_timeout=3
        )
        return conn
    except MySQLError as err:
        logger.warning(
            "MySQL connection failed to %s:%s (Database: %s). Error: %s",
            MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE if use_database else "[None]", err.msg
        )
        raise

def test_mysql_connection() -> bool:
    """Tests whether MySQL server is reachable with configured credentials."""
    try:
        conn = get_mysql_connection(use_database=False)
        if conn.is_connected():
            conn.close()
            return True
        return False
    except Exception:
        return False

def initialize_database_schema(sql_dir: Optional[Path] = None) -> bool:
    """
    Executes sql/create_database.sql and sql/create_tables.sql
    to guarantee database and star-schema tables exist.
    """
    if sql_dir is None:
        sql_dir = SQL_DIR

    logger.info("Verifying and initializing MySQL Data Warehouse schema")
    try:
        # Step 1: Create Database
        conn_server = get_mysql_connection(use_database=False)
        cursor = conn_server.cursor()
        create_db_file = sql_dir / "create_database.sql"
        if create_db_file.exists():
            with open(create_db_file, "r", encoding="utf-8") as f:
                statements = [stmt.strip() for stmt in f.read().split(";") if stmt.strip()]
                for stmt in statements:
                    cursor.execute(stmt)
            conn_server.commit()
        cursor.close()
        conn_server.close()

        # Step 2: Create Tables in target DB
        conn_db = get_mysql_connection(use_database=True)
        cursor = conn_db.cursor()
        create_tables_file = sql_dir / "create_tables.sql"
        if create_tables_file.exists():
            with open(create_tables_file, "r", encoding="utf-8") as f:
                statements = [stmt.strip() for stmt in f.read().split(";") if stmt.strip()]
                for stmt in statements:
                    cursor.execute(stmt)
            conn_db.commit()
        cursor.close()
        conn_db.close()
        logger.info("MySQL DW schema successfully initialized")
        return True
    except MySQLError as e:
        logger.error("Failed to initialize database schema: %s", e)
        return False
