"""Module to generate customer data for the dim_customers table in the database."""
from typing import Any, List, Tuple

from faker import Faker

from utils.send_data_to_database_table import add_data_to_database_table
from utils.log_handler import get_logger


logger = get_logger(__name__)

FAKER = Faker()

TABLE_NAME = "dim_customers"
COLUMNS = (
    "first_name", "middle_name", "last_name",
    "phone", "email", "country", "address",
)
NUM_OF_CUSTOMERS = 10


def generate_customers_data() -> List[Tuple[Any, ...]]:
    """Generate customers data to be added into the dim_customers table."""
    logger.info("Generating customers data ...")

    customers_data = [
        (
            FAKER.first_name(),
            FAKER.first_name(),  # This will server as middle name for simplicity
            FAKER.last_name(),
            FAKER.unique.numerify("080########"),
            FAKER.unique.email(),
            FAKER.country(),
            FAKER.address(),
        )
        for _ in range(NUM_OF_CUSTOMERS)
    ]

    logger.info("Customer's data completed.")
    return customers_data


def add_customers_data() -> None:
    """Insert generated customers data into the dim_customers database."""
    add_data_to_database_table(
        data=generate_customers_data(),
        table_name=TABLE_NAME,
        columns=COLUMNS,
    )
