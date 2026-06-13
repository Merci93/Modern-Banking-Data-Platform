"""Module to seed merchant reference data into the dim_merchants table."""
import random
from pathlib import Path
from typing import List, Tuple

from data_generator.send_data_to_database_table import add_data_to_database_table
from utils.helpers import read_reference_data
from utils.log_handler import get_logger


logger = get_logger(__name__)

TABLE_NAME = "dim_merchants"
COLUMNS = ("merchant_name", "merchant_category")

BASE_DIR = Path(__file__).resolve().parent.parent
MERCHANT_FILE_PATH = BASE_DIR / "data" / "merchants.json"
CATEGORY_FILE_PATH = BASE_DIR / "data" / "transaction_categories.json"


def fetch_merchant_data() -> List[Tuple[str, str]]:
    """Fetch reference data to seed database."""
    categories = read_reference_data(CATEGORY_FILE_PATH, "categories")
    merchants = read_reference_data(MERCHANT_FILE_PATH, "merchants")
    return [(merchant, random.choice(categories)) for merchant in merchants]


def seed_merchant_table(is_seeded: bool = False) -> None:
    """
    Helper function to seed merchant reference data into the dim_merchants table.
    Uses ON CONFLICT DO NOTHING to avoid duplicates.

    :param is_seeded: Parameter that shows if the table should be seeded or not.
    """
    if is_seeded:
        logger.info("Skipping seeding for %s", TABLE_NAME)
        return

    merchant_data = fetch_merchant_data()

    add_data_to_database_table(
        data=merchant_data,
        table_name=TABLE_NAME,
        columns=COLUMNS,
        on_conflict=COLUMNS[0],
    )
