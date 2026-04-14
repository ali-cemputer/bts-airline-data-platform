"""
 bts_processing_dag.py - Monthly DAG for BTS Spark transformation pipeline.
Reads bronze Parquet from GCS, writes silver Parquet, loads BigQuery.
"""

import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_spark_transform(**context):
    year = context["data_interval_start"].year
    month = context["data_interval_start"].month

    cmd = [
        "docker", "run", "--rm",
        "--network", "docker_pipeline-net",
        "-e", "GOOGLE_APPLICATION_CREDENTIALS=/opt/spark/keys/bd-project-cemputer-credentials.json",
        "docker-spark",
        "/opt/spark/bin/spark-submit",
        "--master", "local[*]",
        "/opt/spark/processing/spark_transform.py",
        "--year", str(year),
        "--month", str(month),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)

    if result.returncode != 0:
        raise Exception(f"Spark job failed with return code {result.returncode}")


with DAG(
    dag_id="bts_processing_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1},
) as dag:

    spark_transform = PythonOperator(
        task_id="spark_transform",
        python_callable=run_spark_transform,
    )