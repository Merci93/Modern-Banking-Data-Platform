"""Database connection module."""
import os

import psycopg2
# from dotenv import load_dotenv


# load_dotenv()


def db_connect():
    """Postgres Database Connection."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    return conn, conn.cursor()


if __name__ == "__main__":
    # Test the connection
    try:
        conn, cur = db_connect()
        print("Database connection successful!")
    except Exception as e:
        print(f"Database connection failed: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
