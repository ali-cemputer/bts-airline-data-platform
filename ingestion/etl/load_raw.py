"""
load_raw.py - Load a filtered CSV into PostgreSQL raw.carrier_report table.
All cols stored as TEXT. Idempotent: deletes existing rows for the same year/month before loading.
Usage: uv run python load_raw.py --year 2023 --month 1
"""
import click
import sys
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_batch

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config import RAW_DATA_DIR, RAW_SCHEMA, REPORT_TABLE
from utils import get_connection, get_logger

logger=get_logger("load_raw")# Initialize logger for tracking the data loading process


def run(year: int, month: int):
    """Core logic - callable from both CLI and Airflow"""
    csv_path = RAW_DATA_DIR / str(year) / f"{year}_{month}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}. Run extract.py first.")
    
    df = pd.read_csv(csv_path, dtype=str)
    logger.info(f"Read {len(df):,} rows from {csv_path}")

    table = f"{RAW_SCHEMA}.{REPORT_TABLE}"
    columns = ", ".join(f'"{col}"' for col in df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Create schema and table if they don't exist (all columns as TEXT)
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
            col_defs = ", ".join(f'"{col}" TEXT' for col in df.columns)
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_defs});")
 
            # Delete existing rows for this year/month (idempotency)
            cur.execute(f'DELETE FROM {table} WHERE "Year" = %s AND "Month" = %s;', (str(year), str(month)))
            logger.info(f"Deleted existing rows for {year}-{month:02d}")
 
            # Bulk insert: Multiple rows are packaged and sent. (execute_batch)
            rows = [tuple(row) for row in df.itertuples(index=False)]
            execute_batch(cur, f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", rows)
 
        conn.commit()
 
    logger.info(f"Loaded {len(df):,} rows into {table}")


@click.command()
@click.option("--year", required=True, type=int)
@click.option("--month", required=True, type=int)
def load_raw(year, month):
    """CLI entry point — delegates to run()."""
    run(year, month)
 
 
if __name__ == "__main__":
    load_raw()


