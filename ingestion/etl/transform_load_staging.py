"""
transform_load_staging.py - Read from raw.carrier_report, apply transformations, and load into staging.carrier_report.
Transformations: type casting, NULL filling, derived columns (is_delayed, delay_category).
Usage: uv run python transform_load_staging.py --year 2023 --month 1
"""

import sys
from pathlib import Path

import click
import pandas as pd
from psycopg2.extras import execute_batch

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config import RAW_SCHEMA, STAGING_SCHEMA, REPORT_TABLE
from utils import get_connection, get_logger

logger = get_logger("transform_load_staging")

# Columns to cast to numeric (NULLs preserved as None)
FLOAT_COLUMNS = [
    "DepDelay", "TaxiOut", "TaxiIn", "ArrDelay",
    "ActualElapsedTime", "AirTime", "Distance",
    "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay",
]

# Columns to cast to boolean (source: "1.00" / "0.00")
BOOL_COLUMNS = ["Cancelled", "Diverted"]

def delay_category(arr_delay) -> str | None:
    """Bucket ArrDelay into a human-readable category. NULL stays NULL."""
    if pd.isna(arr_delay):
        return None
    elif arr_delay <= 0:
        return "No Delay"
    elif arr_delay <= 15:
        return "Minor"
    elif arr_delay <= 60:
        return "Major"
    else:
        return "Severe"
    
def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Convert FlightDate to datetime (already YYYY-MM-DD in source)
    df["FlightDate"] = pd.to_datetime(df["FlightDate"], errors="coerce").dt.date

    # Numeric columns — NULLs stay as None (will be NULL in PostgreSQL)
    df[FLOAT_COLUMNS] = df[FLOAT_COLUMNS].apply(pd.to_numeric, errors="coerce")
 
    # Boolean columns — "1.00" → True, "0.00" → False
    for col in BOOL_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").eq(1)
 
    # Derived columns
    df["delay_category"] = df["ArrDelay"].apply(delay_category)
 
    return df
 
 
def build_col_defs(df: pd.DataFrame) -> str:
    defs = []
    for col in df.columns:
        if col == "FlightDate":
            defs.append(f'"{col}" DATE')
        elif col in BOOL_COLUMNS:
            defs.append(f'"{col}" BOOLEAN')
        elif col in FLOAT_COLUMNS:
            defs.append(f'"{col}" FLOAT')
        else:
            defs.append(f'"{col}" TEXT')
    return ", ".join(defs)


def run(year: int, month: int) -> None:
    """Core logic — callable from both CLI and Airflow."""
    table = f"{STAGING_SCHEMA}.{REPORT_TABLE}"
 
    with get_connection() as conn:
        logger.info(f"Reading {RAW_SCHEMA}.{REPORT_TABLE} for {year}-{month:02d}")
        df = pd.read_sql(
            f'SELECT * FROM {RAW_SCHEMA}.{REPORT_TABLE} WHERE "Year" = %s AND "Month" = %s',
            conn, params=(str(year), str(month))
        )
        logger.info(f"Read {len(df):,} rows from raw")
 
        df = transform(df)
 
        columns = ", ".join(f'"{col}"' for col in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
 
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({build_col_defs(df)});")
            cur.execute(f'DELETE FROM {table} WHERE "Year" = %s AND "Month" = %s;', (str(year), str(month)))
            logger.info(f"Deleted existing rows for {year}-{month:02d}")
 
            rows = [tuple(row) for row in df.itertuples(index=False)]
            execute_batch(cur, f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", rows)
 
        conn.commit()
 
    logger.info(f"Loaded {len(df):,} rows into {table}")
 
 
@click.command()
@click.option("--year", required=True, type=int)
@click.option("--month", required=True, type=int)
def transform_load_staging(year: int, month: int) -> None:
    """CLI entry point — delegates to run()."""
    run(year, month)
 
 
if __name__ == "__main__":
    transform_load_staging()