# Base image: Apache Airflow 2.11.2 — stable version
FROM apache/airflow:2.11.2

USER airflow

# installing  for airflowDAG
USER root
RUN apt-get update \
    && apt-get install -y ca-certificates curl \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# We embed the extra packages into the image:
# - apache-airflow-providers-google : Adds GCS/BigQuery operators to Airflow
# - google-cloud-storage            : Directly access to GCS with Python
# - google-cloud-bigquery           : Directly access to BigQuery with Python
# - pandas                          : CSV/DataFrame jobs (ETL branch)
# - pyarrow                         : Parquet read/write (ELT branch)
RUN pip install --no-cache-dir \
    apache-airflow-providers-google \
    google-cloud-storage \
    google-cloud-bigquery \
    pandas \
    pyarrow

# GCP JSON key cannot be copied here — it's a security risk.