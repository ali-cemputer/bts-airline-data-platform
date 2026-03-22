"""
extract.py - Download monthly data, filter cols, save as csv.
Usage: python extract.py --year 2023 --month 1
"""
import click # for terminal dynamically parameters manage.
import sys
import zipfile
from pathlib import Path
import requests
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config import RAW_DATA_DIR, LOOKUP_DATA_DIR, PREZIP_BASE_URL,PREZIP_FILENAME_TEMPLATE, SELECTED_COLUMNS

@click.command()
@click.option('--year',required=True, type=int )
@click.option('--month',required=True,type=int)

def download(year,month):
    download_file_path = PREZIP_FILENAME_TEMPLATE.format(year=year,month=month)
    url=f"{PREZIP_BASE_URL}/{download_file_path}"
    zip_path = RAW_DATA_DIR/str(year)/f"{year}_{month}.zip"
    csv_path = RAW_DATA_DIR/str(year)/f"{year}_{month}.csv"

    zip_path.parent.mkdir(parents=True, exist_ok=True) #create zip path.

    #Download zip
    print(f"[INFO]: Downloading {url}")
    r = requests.get(url, stream=True, timeout=120) # Download chunk by chunk with stream=True
    if r.status_code == 404:
        print(f"[WARNING]: Not found — {url}")
        return
    r.raise_for_status()

    with open(zip_path, "wb") as f:# write to zip path
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    
    # Extract, filter, save
    with zipfile.ZipFile(zip_path) as zf:
        csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, dtype=str, usecols=SELECTED_COLUMNS, low_memory=False)
    
    df.to_csv(csv_path, index=False)
    print(f"[INFO]: Saved {csv_path} ({len(df):,} rows)")
 
    zip_path.unlink()  # delete ZIP to save disk space


if __name__ =="__main__":
    download()


