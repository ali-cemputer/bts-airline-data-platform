import click
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (
    GCP_PROJECT_ID,
    GCP_CREDENTIALS_PATH,
    GCS_BUCKET,
    BRONZE_PREFIX,
    SILVER_PREFIX,
    BQ_DATASET,
    BQ_TABLE,
    BQ_PARTITION_COLUMN,
    BQ_CLUSTER_COLUMNS,
    LOOKUP_CARRIERS,
    LOOKUP_AIRPORT,
    LOOKUP_CANCELLATION,
)
from utils import get_logger

logger = get_logger("spark_transform")


@click.command()
@click.option("--year",  required=True, type=int)
@click.option("--month", required=True, type=int)
def main(year, month):

    # 1) Read / write paths ────────────────────────────────────────────────────
    read_path   = f"gs://{GCS_BUCKET}/{BRONZE_PREFIX}/year={year}/month={month}/data.parquet"
    silver_path = f"gs://{GCS_BUCKET}/{SILVER_PREFIX}/year={year}/month={month}"
    bq_table    = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    logger.info("Starting transform | year=%s month=%s", year, month)

    # 2) SparkSession — GCS + BigQuery connector config ────────────────────────
    # GCS auth: service account JSON keyfile
    # google.cloud.auth.type=USER_CREDENTIALS tells the connector to use
    # the refresh_token flow instead of a service account JSON keyfile
    spark = (
        SparkSession.builder
        .appName("bts_bronze_to_silver")
        .master("local[*]")
        # GCS connector — filesystem implementation
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        
        # ADC auth type: use authorized_user (refresh token) credentials
        .config("spark.hadoop.google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                GCP_CREDENTIALS_PATH)
        
        # BigQuery connector — project and credentials
        .config("spark.datasource.bigquery.project", GCP_PROJECT_ID)
        .config("spark.datasource.bigquery.credentials.file", GCP_CREDENTIALS_PATH)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")  # suppress verbose Spark logs

    # 3) Read bronze layer ─────────────────────────────────────────────────────
    logger.info("Reading bronze: %s", read_path)
    df = spark.read.parquet(read_path)

    # 4) Type casting ──────────────────────────────────────────────────────────
    cast_map = {
        "Year":             "int",
        "Month":            "int",
        "DayOfWeek":        "int",
        "FlightDate":       "date",
        "Reporting_Airline":"string",
        "Origin":           "string",
        "OriginCityName":   "string",
        "Dest":             "string",
        "DestCityName":     "string",
        "CRSDepTime":       "int",      # scheduled departure time hhmm → int
        "DepTime":          "int",      # actual departure time hhmm → int
        "DepDelay":         "int",      # negative = early departure
        "TaxiOut":          "int",      # gate to wheels-off duration in minutes
        "TaxiIn":           "int",      # wheels-on to gate duration in minutes
        "CRSArrTime":       "int",      # scheduled arrival time hhmm → int
        "ArrTime":          "int",      # actual arrival time hhmm → int
        "ArrDelay":         "int",      # negative = early arrival
        "Cancelled":        "int",      # float → int, cast to bool after null fill
        "CancellationCode": "string",
        "Diverted":         "int",      # 1=Yes, 0=No, cast to bool after null fill
        "ActualElapsedTime":"int",      # gate-to-gate duration in minutes
        "AirTime":          "int",      # airborne duration in minutes
        "Distance":         "int",      # distance between airports in miles
        "CarrierDelay":     "int",      # NULL means no carrier delay
        "WeatherDelay":     "int",      # NULL means no weather delay
        "NASDelay":         "int",      # NULL means no NAS delay
        "SecurityDelay":    "int",      # NULL means no security delay
        "LateAircraftDelay":"int",      # NULL means no late aircraft delay
    }
    for col_name, dtype in cast_map.items():
        df = df.withColumn(col_name, F.col(col_name).cast(dtype))

    # Cast Cancelled and Diverted to boolean after int conversion
    df = df.withColumn("Cancelled", F.col("Cancelled").cast("boolean"))
    df = df.withColumn("Diverted",  F.col("Diverted").cast("boolean"))

    # 5) Fill nulls ────────────────────────────────────────────────────────────
    # Delay breakdown columns: NULL means no delay from that cause, fill with 0
    delay_cause_cols = [
        "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
    ]
    for col_name in delay_cause_cols:
        df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

    # CancellationCode: NULL means flight was not cancelled, fill with "N/A" for clean lookup join
    df = df.withColumn(
        "CancellationCode",
        F.coalesce(F.col("CancellationCode"), F.lit("N/A")),
    )

    # Remaining columns (ArrDelay, DepDelay, AirTime, etc.) are left as NULL —
    # they belong to cancelled/diverted flights and have no meaningful fill value

    # 6) Lookup joins ──────────────────────────────────────────────────────────
    # Carrier code → airline name
    carriers = (
        spark.read.option("header", True).csv(LOOKUP_CARRIERS)
        .withColumnRenamed("Code",        "carrier_code")
        .withColumnRenamed("Description", "airline_name")
    )
    df = df.join(carriers, df.Reporting_Airline == carriers.carrier_code, "left").drop("carrier_code")

    # Airport code → origin airport name
    airports = (
        spark.read.option("header", True).csv(LOOKUP_AIRPORT)
        .withColumnRenamed("Code",        "airport_code")
        .withColumnRenamed("Description", "origin_airport_name")
    )
    df = df.join(airports, df.Origin == airports.airport_code, "left").drop("airport_code")

    # Cancellation code → cancellation reason
    cancellation = (
        spark.read.option("header", True).csv(LOOKUP_CANCELLATION)
        .withColumnRenamed("Code",        "cancel_code")
        .withColumnRenamed("Description", "cancellation_reason")
    )
    df = df.join(cancellation, df.CancellationCode == cancellation.cancel_code, "left").drop("cancel_code")

    # 7) Derived columns ───────────────────────────────────────────────────────
    # is_delayed: true if arrival delay exceeds 15 minutes (FAA standard)
    df = df.withColumn(
        "is_delayed",
        F.when(F.col("ArrDelay") > 15, True).otherwise(False),
    )

    # delay_category: human-readable tier based on arrival delay in minutes
    # NULL ArrDelay (cancelled flights) will produce NULL category — intentional
    df = df.withColumn(
        "delay_category",
        F.when(F.col("ArrDelay") <= 0,  "No Delay")
         .when(F.col("ArrDelay") <= 15, "Minor")
         .when(F.col("ArrDelay") <= 60, "Major")
         .otherwise("Severe"),
    )

    # 8) Rename to snake_case for BigQuery compatibility ───────────────────────
    rename_map = {
        "Year":              "year",
        "Month":             "month",
        "DayOfWeek":         "day_of_week",
        "FlightDate":        "flight_date",
        "Reporting_Airline": "reporting_airline",
        "Origin":            "origin",
        "OriginCityName":    "origin_city_name",
        "Dest":              "dest",
        "DestCityName":      "dest_city_name",
        "CRSDepTime":        "crs_dep_time",
        "DepTime":           "dep_time",
        "DepDelay":          "dep_delay",
        "TaxiOut":           "taxi_out",
        "TaxiIn":            "taxi_in",
        "CRSArrTime":        "crs_arr_time",
        "ArrTime":           "arr_time",
        "ArrDelay":          "arr_delay",
        "Cancelled":         "cancelled",
        "CancellationCode":  "cancellation_code",
        "Diverted":          "diverted",
        "ActualElapsedTime": "actual_elapsed_time",
        "AirTime":           "air_time",
        "Distance":          "distance",
        "CarrierDelay":      "carrier_delay",
        "WeatherDelay":      "weather_delay",
        "NASDelay":          "nas_delay",
        "SecurityDelay":     "security_delay",
        "LateAircraftDelay": "late_aircraft_delay",
    }
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)

    # 9) Write silver layer ────────────────────────────────────────────────────
    logger.info("Writing silver: %s", silver_path)
    df.write.mode("overwrite").parquet(silver_path)

    # 10) Load BigQuery ────────────────────────────────────────────────────────
    # parentProject: explicitly set because authorized_user JSON has no project_id field
    # writeMethod=indirect: stages data in GCS first, then loads into BQ via load job
    # temporaryGcsBucket: required for indirect write — uses same bucket as bronze/silver
    logger.info("Loading BigQuery table: %s", bq_table)
    (
        df.write
        .format("bigquery")
        .option("table",              bq_table)
        .option("parentProject",      GCP_PROJECT_ID)
        .option("partitionField",     BQ_PARTITION_COLUMN)
        .option("clusteredFields",    ",".join(BQ_CLUSTER_COLUMNS))
        .option("writeMethod",        "indirect")
        .option("temporaryGcsBucket", GCS_BUCKET)
        .mode("append")
        .save()
    )

    logger.info("Transform complete | year=%s month=%s", year, month)
    spark.stop()


if __name__ == "__main__":
    main()