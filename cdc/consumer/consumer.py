"""
Module to read/stream data from kafka into MinIo storage layer.
MinIo in this scenario serves as S3 bucket for object storage.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from kafka import KafkaConsumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", 60))
TOPICS = (
    "banking_server.public.dim_customers",
    "banking_server.public.dim_accounts",
    "banking_server.public.fact_transactions",
    "banking_server.public.dim_merchants",
    "banking_server.public.dim_currency",
    "banking_server.public.dim_transaction_categories",
)


def create_consumer() -> KafkaConsumer:
    """Create and configure Kafka consumer"""
    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVER"),
        group_id=os.getenv("KAFKA_GROUP"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        max_poll_records=500,
    )

    consumer.subscribe(TOPICS)

    return consumer


def create_minio_client() -> boto3.client:
    """Create Minio client"""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
    )


def validate_minio_connection(s3_client,) -> None:
    """Check MinIO availability."""
    try:
        s3_client.list_buckets()
        logger.info("MinIO connection established.")

    except Exception:
        logger.exception("Unable to connect to MinIO.")
        raise


def create_or_validate_bucket(s3_client: boto3.client, bucket_name: str) -> None:
    """Create bucke if it does not exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info("Bucket already exists.")

    except ClientError as error:
        error_code = error.response["Error"]["Code"]

        if error_code in ("404",  "NoSuchBucket",):
            logger.info("Creating bucket ...")

            s3_client.create_bucket(Bucket=bucket_name)

            logger.info("Bucket created.")

        else:
            raise


def wait_for_topics(consumer: KafkaConsumer) -> None:
    """Wait for Kafka topics to be created."""

    while True:

        available_topics = consumer.topics()

        logger.info("Available topics: %s", sorted(available_topics))

        missing_topics = [
            topic
            for topic in TOPICS
            if topic not in available_topics
        ]

        if not missing_topics:
            logger.info("All Kafka topics available.")
            return

        logger.info("Waiting for topics: %s", ", ".join(missing_topics))

        time.sleep(5)


def flush_buffers(
    buffers: dict,
    s3_client,
    bucket_name: str,
) -> None:
    """Flush remaining buffered records to MinIO."""

    for topic, records in buffers.items():

        if not records:
            continue

        write_to_minio(
            s3_client=s3_client,
            bucket_name=bucket_name,
            table_name=topic.split(".")[-1],
            records=records,
        )

        buffers[topic].clear()


def write_to_minio(
    s3_client,
    bucket_name: str,
    table_name: str,
    records: list[dict],
) -> None:
    """Write records to MinIO as Parquet."""

    if not records:
        return

    df = pd.DataFrame(records)

    now = datetime.now(timezone.utc)

    local_file = f"{table_name}_{now.strftime('%Y%m%d%H%M%S')}.parquet"

    df.to_parquet(local_file, engine="pyarrow", index=False,)

    s3_key = (
        f"raw/{table_name}/"
        f"ingestion_date={now.year}-{now.month:02d}-{now.day:02d}/"
        f"{local_file}"
    )

    s3_client.upload_file(
        local_file,
        bucket_name,
        s3_key,
    )

    os.remove(local_file)

    logger.info("Uploaded %s records to s3://%s/%s", len(records), bucket_name, s3_key)


def initialize_buffers() -> dict:
    """Create topic buffers."""

    return {topic: [] for topic in TOPICS}


def process_message(
    message,
    buffers: dict,
    s3_client,
    bucket_name: str,
) -> None:
    """Process a single Kafka message."""
    topic = message.topic

    if topic not in buffers:
        logger.warning("Ignoring unknow topic %s", topic)
        return

    event = message.value

    payload = event.get("payload")

    if not payload:
        return

    operation = payload.get("op")

    # Ignore delete operations
    if operation == "d":
        return

    record = payload.get("after")

    if not record:
        return

    buffers[topic].append(record)

    if len(buffers[topic]) >= BATCH_SIZE:
        write_to_minio(
            s3_client=s3_client,
            bucket_name=bucket_name,
            table_name=topic.split(".")[-1],
            records=buffers[topic],
        )

        buffers[topic].clear()


def consume_messages() -> None:
    """Consume Kafka messages and write batches to MinIO."""

    consumer = create_consumer()

    wait_for_topics(consumer)

    s3_client = create_minio_client()

    validate_minio_connection(s3_client)

    bucket_name = os.getenv("MINIO_BUCKET")

    create_or_validate_bucket(
        s3_client=s3_client,
        bucket_name=bucket_name,
    )

    buffers = initialize_buffers()

    last_flush_time = time.time()

    logger.info("Kafka connection established. Listening for messages ...")

    while True:

        messages = consumer.poll(
            timeout_ms=1000
        )

        if messages:

            for _, records in messages.items():

                for message in records:

                    process_message(
                        message=message,
                        buffers=buffers,
                        s3_client=s3_client,
                        bucket_name=bucket_name,
                    )

        # Time-based flush
        if time.time() - last_flush_time >= FLUSH_INTERVAL:

            logger.info("Flush interval reached. Uploading buffered records ...")

            flush_buffers(
                buffers=buffers,
                s3_client=s3_client,
                bucket_name=bucket_name,
            )

            last_flush_time = time.time()


if __name__ == "__main__":
    consume_messages()
