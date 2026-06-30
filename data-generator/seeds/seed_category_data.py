"""Module to seed category reference data into the dim_transaction_categories table."""
from pathlib import Path

from utils.helpers import read_reference_data
from utils.log_handler import get_logger
from utils.send_data_to_database_table import add_data_to_database_table


logger = get_logger(__name__)

TABLE_NAME = "dim_transaction_categories"
COLUMNS = ("category_name",)
BASE_DIR = Path(__file__).resolve().parent.parent
CATEGORY_FILE_PATH = BASE_DIR / "data" / "transaction_categories.json"


def fetch_category_data() -> None:
    """fetch catgeory data for seeding dim_category table."""
    categories = read_reference_data(CATEGORY_FILE_PATH, "categories")
    return [(cat,) for cat in categories]


def seed_categories_table(is_seeded: bool = False) -> None:
    """
    Function to seed category reference data into the dim_transaction_categories table.
    Uses ON CONFLICT DO NOTHING to avoid duplicates.

    :param is_seeded: Parameter that shows if the table should be seeded or not.
    """
    if is_seeded:
        logger.info("Skipping seeding for %s", TABLE_NAME)
        return

    category_data = fetch_category_data()

    add_data_to_database_table(
        data=category_data,
        table_name=TABLE_NAME,
        columns=COLUMNS,
        on_conflict=COLUMNS[0],
    )
