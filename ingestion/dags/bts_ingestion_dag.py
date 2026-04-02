"""
bts_ingestion_dag.py - Monthly DAG for BTS airline on-time performance pipeline.
Orchestrates ETL (CSV → PostgreSQL) and ELT (CSV → GCS) branches in parallel.
"""

import sys
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make ingestion/ importable inside Airflow container
# ingestion/ is mounted at /opt/airflow/ingestion via docker-compose volume
INGESTION_DIR = Path("/opt/airflow/ingestion")
sys.path.insert(0, str(INGESTION_DIR))

from etl.extract import run as extract_run
from etl.load_raw import run as load_raw_run
from etl.transform_load_staging import run as transform_run
from elt.upload_to_gcs import run as gcs_run


def _get_year_month(context) -> tuple[int, int]:
    """Extract year and month from Airflow execution context."""
    dt = context["data_interval_start"]
    return dt.year, dt.month


# --- Task functions ---

def extract_task(**context):
    year, month = _get_year_month(context)
    extract_run(year, month)


def load_raw_task(**context):
    year, month = _get_year_month(context)
    load_raw_run(year, month)


def transform_load_staging_task(**context):
    year, month = _get_year_month(context)
    transform_run(year, month)


def upload_to_gcs_task(**context):
    year, month = _get_year_month(context)
    gcs_run(year, month)


# --- DAG definition ---

with DAG(
    dag_id="bts_ingestion_dag",
    start_date=datetime(2023, 1, 1),   # backfill starts from here
    schedule_interval="@monthly",
    catchup=True,                       # enables backfill for past months
    max_active_runs=1,                  # run one month at a time to avoid overload
    default_args={
        "retries": 1,                   # retry once on failure before marking failed
    },
) as dag:

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_task,
    )

    load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=load_raw_task,
    )

    transform_load_staging = PythonOperator(
        task_id="transform_load_staging",
        python_callable=transform_load_staging_task,
    )

    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs_task,
    )

    # ETL branch: extract → load_raw → transform_staging
    # ELT branch: extract → upload_to_gcs → upload_lookups
    extract_data >> [load_raw, upload_to_gcs]
    load_raw >> transform_load_staging
