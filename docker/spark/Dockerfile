# Official Apache Spark image — includes Java, Python, and spark-submit
# scala2.12 is required for BigQuery connector JAR compatibility
FROM spark:3.5.8-scala2.12-java11-python3-r-ubuntu

USER root

ENV GCS_JAR_VERSION=hadoop3-2.2.26
ENV BQ_JAR_VERSION=0.44.1
ENV SPARK_JARS_DIR=/opt/spark/jars

# Create target directories before any COPY or mount
# Prevents Docker from creating these as files instead of directories
RUN mkdir -p /opt/spark/keys \
             /opt/spark/processing \
             /opt/spark/data/lookups

# Download GCS connector — shaded jar includes all dependencies
# -fL: fail on error + follow redirects (Maven Central redirects to CDN)
RUN curl -fL \
    "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/${GCS_JAR_VERSION}/gcs-connector-${GCS_JAR_VERSION}-shaded.jar" \
    -o "${SPARK_JARS_DIR}/gcs-connector-${GCS_JAR_VERSION}-shaded.jar" \
    && ls -lh "${SPARK_JARS_DIR}/gcs-connector-${GCS_JAR_VERSION}-shaded.jar"

# Download BigQuery connector — with-dependencies bundles all required libs
# _2.12 suffix matches the Scala version in the base image
RUN curl -fL \
    "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/${BQ_JAR_VERSION}/spark-bigquery-with-dependencies_2.12-${BQ_JAR_VERSION}.jar" \
    -o "${SPARK_JARS_DIR}/spark-bigquery-with-dependencies_2.12-${BQ_JAR_VERSION}.jar" \
    && ls -lh "${SPARK_JARS_DIR}/spark-bigquery-with-dependencies_2.12-${BQ_JAR_VERSION}.jar"

# Install Python dependencies needed by spark_transform.py
RUN pip install --no-cache-dir click

# Copy processing scripts — baked into image at build time
# For live editing during development, override with a volume mount in compose
COPY processing/ /opt/spark/processing/
COPY infra/keys/ /opt/spark/keys/
COPY data/lookups/ /opt/spark/data/lookups/

# Official spark image default non-root user UID
USER 185