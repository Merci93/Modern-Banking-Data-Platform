"""Module to seed currency reference data into the dim_currency table."""

from pathlib import Path
from typing import List, Tuple

from utils.helpers import read_reference_data
from utils.log_handler import get_logger
from utils.send_data_to_database_table import add_data_to_database_table


logger = get_logger(__name__)


TABLE_NAME = "dim_currency"
COLUMNS = ("currency_code", "currency_name")
BASE_DIR = Path(__file__).resolve().parent.parent
CURRENCY_FILE_PATH = BASE_DIR / "data" / "currency.json"


def fetch_currency_data() -> List[Tuple[str, str]]:
    """Function to get currency data to be seeded into database table."""
    currencies = read_reference_data(CURRENCY_FILE_PATH, "currencies")
    return [(c[0], c[1]) for c in currencies]


def seed_currency_table(is_seeded: bool = False) -> None:
    """
    Function to seed reference data into dim_currency table.
    Uses ON CONFLICT DO NOTHING to avoid duplicates.

    :param is_seeded: Parameter that shows if the table should be seeded or not.
    """
    if is_seeded:
        logger.info("Skipping seeding for %s", TABLE_NAME)
        return

    currency_data = fetch_currency_data()

    add_data_to_database_table(
        data=currency_data,
        table_name=TABLE_NAME,
        columns=COLUMNS,
        on_conflict=COLUMNS[0],
    )
