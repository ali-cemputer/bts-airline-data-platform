"""
config.py - Central configuration for the BTS ETL & ELT pipelines.
All constants (columns, URLs, paths, DB settings) are defined here.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv() # for read to .env file and saves local env variables

# --- Paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[1] #go to main project path: bd_project/ingestion/ -> bd_project/
RAW_DATA_DIR = PROJECT_ROOT/"data"/"raw"# csv files: bd_project/data/raw/YYYY/
LOOKUP_DATA_DIR = PROJECT_ROOT/"data"/"lookups"# lookup tables here.

# --- TranStats PREZIP source ---
PREZIP_BASE_URL = "https://transtats.bts.gov/PREZIP"
PREZIP_FILENAME_TEMPLATE = (
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)

INGEST_YEARS = [2023, 2024, 2025]

LOOKUP_FILES = {
    "months":        "L_MONTHS.csv",
    "weekdays":      "L_WEEKDAYS.csv",
    "carriers":      "L_UNIQUE_CARRIERS.csv",
    "airports":      "L_AIRPORT.csv",       # Origin and Dest common lookup table
    "yes_no":        "L_YESNO_RESP.csv",    # Cancelled and Diverted common lookup table
    "cancellation":  "L_CANCELLATION.csv",
}

SELECTED_COLUMNS = [
    "Year",
    "Month",
    "DayOfWeek",
    "FlightDate",
    "Reporting_Airline",
    "Origin",
    "OriginCityName",
    "Dest",
    "DestCityName",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "TaxiOut",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "ActualElapsedTime",
    "AirTime",
    "Distance",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
]

# --- PostgreSQL connection (values read from .env)

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "bts"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),# its empty for security
}
 
# Schema and table names
RAW_SCHEMA = "raw"
STAGING_SCHEMA = "staging"
FLIGHTS_TABLE = "flights"