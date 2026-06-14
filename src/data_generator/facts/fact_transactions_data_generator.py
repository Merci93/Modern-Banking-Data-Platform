"""Module to generate fake transaction data for the database."""
import random
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, List, Tuple

from faker import Faker

from utils.send_data_to_database_table import add_data_to_database_table
from utils.db_connection import db_connect
from utils.helpers import read_reference_data
from utils.log_handler import get_logger


logger = get_logger(__name__)

FAKER = Faker()

TABLE_NAME = "fact_transactions"
COLUMNS = (
    "account_id", "customer_id", "merchant_id", "transaction_type", "amount",
    "currency_code", "category_id", "transaction_timestamp", "channel",
    "status", "reference_id",
)
NUM_OF_TRANSACTIONS = 15
MIN_TXN_AMOUNT = Decimal("45.00")
MAX_TXN_AMOUNT = Decimal("50000.00")
TXN_TYPE = ("debit", "credit")
TXN_STATUS = ("completed", "pending", "failed")
CHANNELS = ("POS", "ATM", "ONLINE", "BRANCH", "MOBILE")

BASE_DIR = Path(__file__).resolve().parent.parent
CURRENCY_CODE_FILE_PATH = BASE_DIR / "data" / "currency.json"


def random_money():
    """Helper function to generate random money amounts with 2 decimal places for transactions."""
    cents = random.randint(4500, 5000000)
    return Decimal(cents) / Decimal("100")


def fetch_lookup_ids():
    """Fetch lookup ID from the database."""

    conn = None
    cur = None

    try:
        logger.info("Fetching lookup IDs for generating transactions data ...")

        conn, cur = db_connect()

        cur.execute("SELECT id FROM dim_transaction_categories")
        category_ids = [row[0] for row in cur.fetchall()]

        if not category_ids:
            raise ValueError("Category table is empty.")

        cur.execute("SELECT id FROM dim_merchants")
        merchant_ids = [row[0] for row in cur.fetchall()]

        if not merchant_ids:
            raise ValueError("Merchant table is empty.")

        cur.execute("SELECT id, customer_id FROM dim_accounts")
        accounts = cur.fetchall()

        if not accounts:
            raise ValueError("Accounts table is empty.")

        logger.info("Lookup ID fetch completed.")

    except Exception as e:
        logger.error("Error fetching lookup ids: %s", str(e))
        raise

    finally:
        if cur:
            cur.close()

        if conn:
            conn.close()

        logger.info("Connection closed.")

    return category_ids, merchant_ids, accounts


def generate_transaction_data() -> List[Tuple[Any]]:
    """Function to generate fake transactions data for the database."""

    currency_codes = [c[0] for c in read_reference_data(CURRENCY_CODE_FILE_PATH, "currencies")]
    category_ids, merchant_ids, accounts = fetch_lookup_ids()

    logger.info("Generating transactions data ...")

    transactions_data = []
    now = datetime.now()

    transactions_data = [
        (
            account_id,
            customer_id,
            random.choice(merchant_ids),
            random.choice(TXN_TYPE),
            random_money(),
            random.choice(currency_codes),
            random.choice(category_ids),
            now - timedelta(days=random.randint(0, 30)),
            random.choice(CHANNELS),
            random.choice(TXN_STATUS),
            FAKER.uuid4(),
        )
        for account_id, customer_id in (
            random.choice(accounts)
            for _ in range(NUM_OF_TRANSACTIONS)
        )
    ]

    logger.info("Completed generating transactions data.")
    return transactions_data


def add_transactions_data():
    """Insert transactions data into the fact_transactions table."""
    add_data_to_database_table(
        data=generate_transaction_data(),
        table_name=TABLE_NAME,
        columns=COLUMNS
    )
