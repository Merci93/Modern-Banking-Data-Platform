"""Module to check if certain tables already contain data to avoid unnecessary seeding."""
from pathlib import Path

from psycopg2 import sql

from utils.db_connection import db_connect
from utils.helpers import read_reference_data
from utils.log_handler import get_logger


logger = get_logger(__name__)

VALID_TABLES = ["dim_currency", "dim_transaction_categories", "dim_merchants"]
BASE_DIR = Path(__file__).resolve().parent.parent


def get_raw_data_count(table_name: str) -> int:
    """
    Read reference data

    :param: Table for which the reference data is to be read.
    """
    data_mapping = {
        "dim_merchants": (BASE_DIR / "data" / "merchants.json", "merchants"),
        "dim_currency": (BASE_DIR / "data" / "currency.json", "currencies"),
        "dim_transaction_categories": (BASE_DIR / "data" / "transaction_categories.json", "categories"),
    }

    file_path, key = data_mapping[table_name]

    reference_data = read_reference_data(file_path=file_path, key=key)

    return len(reference_data)


def check_seeded_table(table_name: str) -> bool:
    """Check if the specified table already contains data."""

    if table_name not in VALID_TABLES:
        raise ValueError(f"Invalid table name {table_name}.")

    data_count = get_raw_data_count(table_name=table_name)

    conn, cur = db_connect()

    try:
        query = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))

        cur.execute(query)
        txn_count = cur.fetchone()[0]

        is_seeded = txn_count >= data_count

        if is_seeded:
            logger.info("Table '%s' already contains %s records.", table_name, txn_count)
        else:
            logger.info("Table '%s' contains %s of %s expected records.", table_name, txn_count, data_count)

        return is_seeded

    except Exception as e:
        logger.error(f"Error checking table '{table_name}': {e}")
        raise

    finally:
        cur.close()
        conn.close()
        logger.info("Connection closed.")
