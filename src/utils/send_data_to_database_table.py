"""A module to add generated data into the database table."""
from typing import Any, List, Tuple

from psycopg2 import sql
from psycopg2.extras import execute_values

from utils.db_connection import db_connect
from utils.log_handler import get_logger


logger = get_logger(__name__)


def add_data_to_database_table(
    data: List[Tuple[Any, ...]],
    table_name: str,
    columns: Tuple[str, ...],
    on_conflict: str | Tuple[str, ...] | None = None,
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
        print(data)
        placeholder = sql.SQL("%s")
        column_values = sql.SQL(", ").join(sql.Identifier(col) for col in columns)

        query = sql.SQL(
            """
            INSERT INTO {table} ({columns})
            VALUES {placeholder}
            """
        ).format(
            table=sql.Identifier(table_name),
            columns=column_values,
            placeholder=placeholder,
        )

        if on_conflict:
            if isinstance(on_conflict, str):
                on_conflict = (on_conflict,)

            conflict_cols = sql.SQL(", ").join(
                sql.Identifier(col)
                for col in on_conflict
            )

            query += sql.SQL(
                " ON CONFLICT ({}) DO NOTHING"
            ).format(conflict_cols)

        conn, cur = db_connect()

        execute_values(cur, query, data)

        conn.commit()

        logger.info("Data insert into %s completed.", table_name)

    except Exception:
        logger.error("Failed to insert data into %s", table_name)
        if conn:
            conn.rollback()
        raise

    finally:
        if cur:
            cur.close()

        if conn:
            conn.close()

        logger.info("Connection closed.")
