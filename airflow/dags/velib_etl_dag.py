"""
Velib ETL DAG - Extract from API, store in MongoDB, transform and load to PostgreSQL
Scheduled to run every 5 minutes
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task

# Declare datasets for data-aware scheduling
MONGODB_VELIB_RAW = Dataset("mongodb://velib_datalake/velib_raw")
POSTGRES_STATIONS = Dataset("postgresql://airflow/stations")
POSTGRES_AVAILABILITY = Dataset("postgresql://airflow/station_availability")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@task(outlets=[MONGODB_VELIB_RAW])
def extract_velib_data():
    """Extract data from Velib API and store in MongoDB."""
    from getApi import extract_velib_data as extract
    return extract()


@task(outlets=[POSTGRES_STATIONS, POSTGRES_AVAILABILITY])
def transform_and_load(extraction_result: dict):
    """Transform data from MongoDB and load to PostgreSQL."""
    from traitement import transform_and_load as transform_load
    return transform_load()


with DAG(
    dag_id="velib_etl",
    default_args=default_args,
    description="ETL pipeline for Velib bike sharing data",
    schedule="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["velib", "etl", "bigdata"],
) as dag:

    # Define task dependencies using TaskFlow API
    extraction_result = extract_velib_data()
    transform_and_load(extraction_result)
