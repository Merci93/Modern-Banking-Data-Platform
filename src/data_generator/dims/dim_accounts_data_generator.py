"""Module to generate fake account data for the database."""
import random
from pathlib import Path
from typing import Any, List, Tuple

from faker import Faker

from utils.send_data_to_database_table import add_data_to_database_table
from utils import (
    db_connection,
    helpers,
    log_handler,
)


logger = log_handler.get_logger(__name__)

FAKER = Faker()

TABLE_NAME = "dim_accounts"
COLUMNS = ("customer_id", "account_type", "account_number", "currency_code", "status",)
ACCOUNTS_PER_CUSTOMER = 3
ACCOUNT_TYPES = ("savings", "current")
ACCOUNT_STATUS = "active"

BASE_DIR = Path(__file__).resolve().parent.parent
CURRENCY_CODE_FILE_PATH = BASE_DIR / "data" / "currency.json"


def fetch_lookup_id():
    """Fetch lookup id from the database."""
    conn = None
    cur = None

    try:
        logger.info("Connecting to database to get customer ids ...")
        conn, cur = db_connection.db_connect()

        cur.execute("SELECT id FROM dim_customers")
        customer_ids = [row[0] for row in cur.fetchall()]
        logger.info("Customer IDs retrieval completed.")
    except Exception as e:
        logger.error("Failed to fetch customer ids: %s", str(e))
        raise
    finally:
        if cur:
            cur.close()

        if conn:
            conn.close()

        logger.info("Connection closed.")

    return customer_ids


def generate_accounts_data() -> List[Tuple[Any]]:
    """Function to generate accounts data."""

    currency_codes = [c[0] for c in helpers.read_reference_data(CURRENCY_CODE_FILE_PATH, "currencies")]
    customer_ids = fetch_lookup_id()

    logger.info("Generating accounts data ...")

    accounts_data = [
        (
            customer_id,
            random.choice(ACCOUNT_TYPES),
            FAKER.bban(),
            random.choice(currency_codes),
            ACCOUNT_STATUS,
        )
        for customer_id in customer_ids
        for _ in range(ACCOUNTS_PER_CUSTOMER)
    ]

    logger.info("Generating accounts data completed.")
    return accounts_data


def add_account_data() -> None:
    """Add generated accounts data into the database table."""
    add_data_to_database_table(
        data=generate_accounts_data(),
        table_name=TABLE_NAME,
        columns=COLUMNS,
    )
