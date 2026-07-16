"""Module to handle connection between detezium, kafka and database."""
import json
import logging
import os

import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

DEBEZIUM_URL = os.getenv("DEBEZIUM_URL")

CONNECTOR_CONFIG = {
    "name": "banking-db-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "topic.prefix": "banking_server",
        "table.include.list":
            "public.dim_customers,"
            "public.dim_accounts,"
            "public.fact_transactions,"
            "public.dim_currency,"
            "public.dim_merchants,"
            "public.dim_transaction_categories",
        "slot.name": "banking_slot",
        "snapshot.mode": "initial",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double"  # This keeps all decimal format as double
    }
}


def wait_for_debezium() -> None:
    """
    Wait for Debezium Connect REST API to become available.
    """

    logger.info("Waiting for Debezium to become available...")

    while True:
        try:
            response = requests.get(f"{DEBEZIUM_URL}/connectors", timeout=10)

            logger.info("Debezium response status: %s", response.status_code)

            if response.status_code == 200:
                logger.info("Debezium is ready.")
                return

        except requests.RequestException as e:
            logger.warning("Unable to reach Debezium. Msg: %s", str(e))
            pass


def connector_exists() -> bool:
    """
    Check whether connector already exists.
    """
    response = requests.get(
        f"{DEBEZIUM_URL}/connectors/{CONNECTOR_CONFIG['name']}",
        timeout=10
    )

    return response.status_code == 200


def create_connector() -> None:
    """
    Create Debezium connector.
    """
    payload = {
        "name": CONNECTOR_CONFIG["name"],
        "config": CONNECTOR_CONFIG["config"]
    }

    response = requests.post(
        f"{DEBEZIUM_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=30
    )

    response.raise_for_status()

    logger.info("Debezium connector created successfully.")


def update_connector() -> None:
    """
    Update existing connector configuration.
    """
    response = requests.put(
        f"{DEBEZIUM_URL}/connectors/{CONNECTOR_CONFIG["name"]}/config",
        headers={"Content-Type": "application/json"},
        data=json.dumps(CONNECTOR_CONFIG),
        timeout=30
    )

    response.raise_for_status()

    logger.info("Debezium connector updated successfully.")


def establish_connection() -> None:
    """
    Create or update Debezium connector.
    """
    try:
        wait_for_debezium()

        if connector_exists():
            logger.info("Connector already exists. Updating configuration...")
            update_connector()
        else:
            logger.info("Connector not found. Creating connector...")
            create_connector()

    except Exception as exc:
        logger.exception("Failed to configure Debezium connector: %s", exc)
        raise


if __name__ == "__main__":
    establish_connection()
