"""
upload_to_gcs.py - Upload local CSV data to GCS bronze layer as Parquet.
Also handles uploading lookup CSV files to GCS.
Usage:
    python upload_to_gcs.py --year 2023 --month 1
    python upload_to_gcs.py --upload-lookups
"""

import click
import tempfile 
import pandas as pd
from pathlib import Path

from google.cloud import storage

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    RAW_DATA_DIR,
    LOOKUP_DATA_DIR,
    LOOKUP_FILES,
    GCS_BUCKET_NAME,
    GCS_KEY_PATH,
    GCS_BRONZE_PREFIX,
    GCS_LOOKUPS_PREFIX,
)

from utils import get_logger
logger = get_logger('upload_to_gcs')

def get_gcs_client() -> storage.Client:
    """Authenticate with service account key and return GCS client."""
    return storage.Client.from_service_account_json(GCS_KEY_PATH)



def run(year: int, month: int) -> None:
    """Read local CSV, convert to Parquet, upload to GCS bronze, delete temp file."""
    csv_path = RAW_DATA_DIR/ str(year) / f"{year}_{month}.csv"
    if not csv_path.exists():
        logger.error(f"CSV not found: {csv_path}. Run extract.py first.")
        raise FileNotFoundError(csv_path)
    
    logger.info(f"Reading {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)

    # Write Parquet to a temp file, then upload — avoids leaving files on disk
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        df.to_parquet(tmp_path, engine="pyarrow", index=False)
        logger.info(f"Parquet written to temp: {tmp_path}")

        # GCS destination path with Hive partition format
        gcs_path = f"{GCS_BRONZE_PREFIX}/year={year}/month={month}/data.parquet"

        client = get_gcs_client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(str(tmp_path))

        logger.info(f"Uploaded → gs://{GCS_BUCKET_NAME}/{gcs_path}")

    finally:
        tmp_path.unlink(missing_ok=True)  # always clean up temp file

def run_lookups() -> None:
    """Upload all lookup CSV files from data/lookups/ to GCS bronze/lookups/."""

    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)

    for key, filename in LOOKUP_FILES.items():
        local_path = LOOKUP_DATA_DIR / filename

        if not local_path.exists():
            logger.warning(f"Lookup file not found, skipping: {local_path}")
            continue

        gcs_path = f"{GCS_LOOKUPS_PREFIX}/{filename}"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(str(local_path))

        logger.info(f"Uploaded lookup → gs://{GCS_BUCKET_NAME}/{gcs_path}")

@click.command()
@click.option("--year", required=True, type=int)
@click.option("--month", required=True, type=int)
def upload_parquet(year: int, month: int) -> None:
    """CLI entry point for parquet upload — delegates to run()."""
    run(year, month)
 
 
if __name__ == "__main__":
    if "--upload-lookups" in sys.argv:
        sys.argv.remove("--upload-lookups")
        run_lookups()
    else:
        upload_parquet()

