"""
Dag to copy generated files from MinIO to Databricks volume and
load parquet files into Databricks Bronze Delta tables.
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from databricks.sdk import WorkspaceClient

from utils.db_connection import db_connect


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
BASE_DIRECTORY = "/tmp/minio_downloads"

TABLES = [
    "dim_customers",
    "dim_accounts",
    "fact_transactions",
    "dim_merchants",
    "dim_currency",
    "dim_transaction_categories",
]

# Databricks Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_VOLUME_PATH = os.getenv("VOLUME_PATH")
DATABRICKS_JOB_ID = os.getenv("JOB_ID")

# DAG default arguments
DEFAULT_ARGS = {
    "owner": "MinIO Databricks",
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
}


# MinIO client connection
def minio_client() -> boto3.client:
    """Create MinIO client connection."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


# Databricks client connection
def databricks_client() -> WorkspaceClient:
    """Create databricks client connection."""
    return WorkspaceClient(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN,
    )


# Get checkpointing data
def fetch_table_last_checkpoint(table: str) -> datetime | None:
    """Fetch the last checkpoint of the referenced table from the database."""

    conn, cur = db_connect()

    try:
        cur.execute(
            """
            SELECT object_key
            FROM ingestion_checkpoint
            WHERE table_name = %s
            """,
            (table,)
        )

        row = cur.fetchone()

        if row:
            return row[0]

        return None

    except Exception as e:
        logger.error("Failed fetching checkpoint for %s. Error: %s", table, str(e))
        raise

    finally:
        cur.close()
        conn.close()


# Update checkpointing data
def update_table_checkpoint(table: str, object_key: str) -> None:
    """Upsert the last checkpoint for referenced table."""

    conn, cur = db_connect()

    try:
        cur.execute(
            """
            INSERT INTO ingestion_checkpoint (
                table_name,
                object_key
            )
            VALUES (%s, %s)

            ON CONFLICT (table_name)
            DO UPDATE
            SET
                object_key = EXCLUDED.object_key,
                updated_at = NOW()
            """,
            (
                table,
                object_key,
            )
        )

        conn.commit()

    except Exception as e:
        logger.error("Checkpoint update failed for %s. Error: %s", table, str(e))
        raise

    finally:
        cur.close()
        conn.close()


# Extract
@task
def download_from_minio() -> dict[str, Any]:
    """Download files from MinIO."""

    logger.info("Downloading files from MinIO...")

    s3 = minio_client()

    downloaded_files: dict[str, list[str]] = {}
    checkpoints: dict[str, str] = {}

    for table in TABLES:

        downloaded_files[table] = []

        checkpoint = fetch_table_last_checkpoint(table)

        paginator = s3.get_paginator("list_objects_v2")

        pagination_args = {
            "Bucket": MINIO_BUCKET,
            "Prefix": f"raw/{table}/",
        }

        if checkpoint:
            pagination_args["StartAfter"] = checkpoint

        latest_key = None

        for page in paginator.paginate(**pagination_args):

            objects = page.get("Contents", [])

            for obj in objects:

                key = obj["Key"]

                local_directory = os.path.join(
                    BASE_DIRECTORY,
                    os.path.dirname(key)
                )

                os.makedirs(local_directory, exist_ok=True)

                local_path = os.path.join(local_directory, os.path.basename(key))

                s3.download_file(MINIO_BUCKET, key, local_path)

                downloaded_files[table].append(local_path)

                latest_key = key

        if latest_key:
            checkpoints[table] = latest_key

    total_files = sum(len(files) for files in downloaded_files.values())

    logger.info("%s files downloaded successfully.", total_files)

    return {
        "files": downloaded_files,
        "checkpoints": checkpoints,
    }


# Upload to Databricks Volume
@task
def upload_to_databricks(downloaded_files: dict[str, Any]) -> dict[str, list[str]]:
    """Upload downloaded files into Databricks."""

    files = downloaded_files["files"]

    if not files:
        logger.info("No new files to upload.")
        return {}

    db = databricks_client()

    uploaded_files: dict[str, list[str]] = {}

    counter = 0

    for table, table_files in files.items():

        uploaded_files[table] = []

        for file in table_files:

            relative_path = os.path.relpath(file, BASE_DIRECTORY)

            target = (
                f"{DATABRICKS_VOLUME_PATH}"
                f"{relative_path}"
            )

            with open(file, "rb") as f:
                db.files.upload(target, f, overwrite=True)

            counter += 1
            uploaded_files[table].append(target)

    logger.info("Uploaded %s files successfully.", counter)

    return uploaded_files


# Create ingestion manifest file
@task
def create_manifests(uploaded_files: dict[str, list[str]]) -> None:
    """Create ingestion manifest file."""

    time_stamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')

    manifest_path = (
        f"{DATABRICKS_VOLUME_PATH}raw/"
        f"manifests/history/"
        f"manifest_{time_stamp}.json"
    )

    manifest = {
        "run_id": time_stamp,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": "",
        "completed_at": "",
        "error": None,
        "status": "READY",
        "files": uploaded_files,
    }

    latest_path = (
        f"{DATABRICKS_VOLUME_PATH}raw/"
        f"manifests/latest.json"
    )

    db = databricks_client()

    # Write historical manifest
    db.files.upload(
        manifest_path,
        BytesIO(
            json.dumps(
                manifest,
                indent=2
            ).encode("utf-8")
        ),
        overwrite=True,
    )

    # Update latest pointer file
    pointer = {
        "manifest": manifest_path,
        "created_at": manifest["created_at"],
        "status": "READY",
    }

    db.files.upload(
        latest_path,
        BytesIO(
            json.dumps(
                pointer,
                indent=2
            ).encode("utf-8")
        ),
        overwrite=True,
    )

    logger.info("Manifest file created succesfully.")


# Update checkpoint
@task
def update_checkpoint(downloaded_files: dict) -> None:

    checkpoints = downloaded_files["checkpoints"]

    for table, key in checkpoints.items():

        update_table_checkpoint(
            table,
            key,
        )


with DAG(
    dag_id="minio_to_databricks_volume",
    description="Downloads files from MinIO and uploads into Databricks volume.",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["MinIO", "Databricks"],
) as dag:

    extract_files = download_from_minio()

    upload_files = upload_to_databricks(extract_files)

    manifest = create_manifests(upload_files)

    load_to_bronze = DatabricksRunNowOperator(
        task_id="load_to_bronze",
        job_id=DATABRICKS_JOB_ID,
    )

    checkpoint = update_checkpoint(extract_files)

    extract_files >> upload_files >> manifest >> load_to_bronze >> checkpoint
