"""A module to add generated data into the database table."""
from typing import Any, List, Tuple

from psycopg2 import sql
from psycopg2.extras import execute_values

from utils import (
    db_connection,
    log_handler,
)


logger = log_handler.get_logger(__name__)


def add_data_to_database_table(
    data: List[Tuple[Any]],
    table_name: str,
    columns: Tuple[str],
    on_conflict: str = "",
) -> None:
    """
    Fuction to handle data insertion into the database table.

    :param data: Data to be inserted into the database table.
    :param table_name: Database table name where the data will be inserted.
    :param columns: Table column names.
    """
    conn = None
    cur = None

    try:
        placeholders = sql.SQL(", ").join(sql.SQL("%s") for _ in columns)

        query = sql.SQL(
            """
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT {conflict} DO NOTHING
            """
        ).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            placeholders=placeholders,
            conflict=sql.Identifier(on_conflict)
        )

        conn, cur = db_connection.db_connect()

        execute_values(cur, query, data)

        conn.commit()

        logger.info("Data insert into %s completed.", table_name)

    except Exception as e:
        logger.error("Failed to insert data into %s. Error Message: %s", table_name, str(e))
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()
        logger.info("Connection closed.")
