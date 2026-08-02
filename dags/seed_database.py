"""A dag to seed the database with fake data for testing and development."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor

from dims import dim_accounts_data_generator, dim_customer_data_generator
from facts import fact_transactions_data_generator
from seeds import (
    check_seeded_table,
    seed_category_data,
    seed_currency_data,
    seed_merchant_data
)


DEFAULT_ARGS = {
    "owner": "Database Seeder",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


@task
def check_tables(table_name: str) -> bool:
    """Task to check if the tables already have data to avoid unnecessary seeding."""
    return check_seeded_table.check_seeded_table(table_name)


@task
def currency(is_seeded: bool) -> None:
    """Task to seed the currency table with reference data."""
    seed_currency_data.seed_currency_table(is_seeded)


@task
def category(is_seeded: bool) -> None:
    """Task to seed category table with reference data."""
    seed_category_data.seed_categories_table(is_seeded)


@task
def merchant(is_seeded: bool) -> None:
    """Task to seed merchant table with reference data."""
    seed_merchant_data.seed_merchant_table(is_seeded)


@task
def generate_customer_data() -> None:
    """Task to generate fake customer data."""
    dim_customer_data_generator.add_customers_data()


@task
def generate_account_data() -> None:
    """Task to generate fake account data."""
    dim_accounts_data_generator.add_account_data()


@task
def generate_transaction_data() -> None:
    """Task to generate fake transaction data."""
    fact_transactions_data_generator.add_transactions_data()


with DAG(
    dag_id="Data_Generator",
    description="A dag to seed the tables with reference data and also generate fake customer and account data for testing and development.",
    schedule=None,  # Run once on demand
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["database", "seeding", "reference-data"],
    start_date=datetime(2026, 1, 1),
) as dag:

    check_currency_table = check_tables(table_name="dim_currency")
    check_transaction_categories_table = check_tables(table_name="dim_transaction_categories")
    check_merchants_table = check_tables(table_name="dim_merchants")

    delay_trigger = TimeDeltaSensor(
        task_id="delay_next_dag_trigger",
        delta=timedelta(seconds=90)
    )

    trigger_minio_to_databricks = TriggerDagRunOperator(
        task_id="trigger_minio_to_databricks",
        trigger_dag_id="minio_to_databricks_volume",
        wait_for_completion=True,
    )

    [
        currency(check_currency_table),
        category(check_transaction_categories_table),
        merchant(check_merchants_table),
    ] >> generate_customer_data() >> generate_account_data() >> generate_transaction_data() >> delay_trigger >> trigger_minio_to_databricks
