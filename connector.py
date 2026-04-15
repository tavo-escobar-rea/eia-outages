import os
import time
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import date, timedelta

# Load environment variable from .env file
load_dotenv()

# Logger setup for 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# We store the API KEY
API_KEY = os.getenv("EIA_API_KEY")

# --- Constants ---
EIA_URL = "https://api.eia.gov/v2"
PAGE_SIZE = 5000  # Max rows per request allowed by EIA API
MAX_RECORDS = 500  # Stop after 500 records per endpoint
MAX_RETRIES = 3
RETRY_DELAY = 5  # Seconds to wait between retries

ENDPOINTS = {
    "facility": "nuclear-outages/facility-nuclear-outages/data/",
    "generator": "nuclear-outages/generator-nuclear-outages/data/",
    "us": "nuclear-outages/us-nuclear-outages/data/",
}

# Columns to request explicitly per endpoint
DATA_COLUMNS = {
    "facility": ["capacity", "outage", "percentOutage"],
    "generator": ["capacity", "outage", "percentOutage"],
    "us": ["capacity", "outage", "percentOutage"],
}

# validate_api_key()
# Parameters: none — uses API_KEY loaded from .env
# Returns: True if the key is accepted, False if rejected or on error
def validate_api_key():
    try:
        response = requests.get(f"{EIA_URL}/nuclear-outages/facility-nuclear-outages/data/", params={"api_key": API_KEY, "length": 1}, timeout=15)
        if response.status_code == 200:
            logger.info("API key is valid.")
            return True
        elif response.status_code in (400, 403):
            logger.error(f"Invalid API key (HTTP {response.status_code}). Check your EIA_API_KEY.")
            return False
        elif response.status_code == 404:
            logger.error("API endpoint not found (HTTP 404). The URL may have changed.")
            return False
        elif response.status_code == 429:
            logger.error("Rate limit exceeded (HTTP 429). Too many requests.")
            return False
        elif response.status_code >= 500:
            logger.warning(f"EIA server error (HTTP {response.status_code}). Cannot verify key right now, proceeding anyway.")
            return True
        else:
            logger.warning(f"Unexpected response (HTTP {response.status_code}) during key validation.")
            return False
    except requests.exceptions.ConnectionError:
        logger.warning("Could not reach EIA API to validate key. Proceeding anyway.")
        return True
    except requests.exceptions.Timeout:
        logger.error("EIA server is down or unreachable. Cannot validate key.")
        return False


REQUIRED_FIELDS = {
    "facility": ["period", "facility", "facilityName", "capacity", "outage", "percentOutage"],
    "generator": ["period", "facility", "facilityName", "generator", "capacity", "outage", "percentOutage"],
    "us": ["period", "capacity", "outage", "percentOutage"],
}


# validate_records(records, endpoint_key)
# Parameters: raw record list from the API, endpoint name ("facility", "generator", "us")
# Returns: filtered list keeping only records that have all required fields
def validate_records(records, endpoint_key):
    required = REQUIRED_FIELDS.get(endpoint_key, [])
    valid = []

    for i, record in enumerate(records):
        missing = [f for f in required if f not in record or record[f] is None]
        if missing:
            logger.warning(f"Record {i} missing fields {missing} — skipping: {record}")
        else:
            valid.append(record)

    dropped = len(records) - len(valid)
    if dropped:
        logger.warning(f"Dropped {dropped} invalid records out of {len(records)} for {endpoint_key}.")
    else:
        logger.info(f"All {len(valid)} records passed validation for {endpoint_key}.")

    return valid


# get_last_date(filename)
# Parameters: parquet filename without path or extension
# Returns: most recent date in the "period" column, or None if the file does not exist yet
def get_last_date(filename):
    path = f"data/{filename}.parquet"
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path, columns=["period"])
        last = pd.to_datetime(df["period"]).max()
        logger.info(f"Existing data found in {filename}. Last period: {last.date()}")
        return last.date()
    except Exception as e:
        logger.warning(f"Could not read last date from {filename}: {e}. Starting full load.")
        return None


# save_to_parquet(records, filename)
# Parameters: list of records to save, parquet filename without path or extension
# Returns: nothing — writes or appends to data/<filename>.parquet and deduplicates rows
def save_to_parquet(records, filename):
    if not records:
        logger.warning(f"No new data to save for {filename}. Skipping.")
        return

    os.makedirs("data", exist_ok=True)
    path = f"data/{filename}.parquet"
    new_df = pd.DataFrame(records)

    try:
        # If a parquet already exists, append new rows and deduplicate
        if os.path.exists(path):
            existing_df = pd.read_parquet(path)
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined.drop_duplicates(inplace=True)
            combined.to_parquet(path, index=False)
            logger.info(f"Appended {len(new_df)} new records to {path} (total: {len(combined)})")
        else:
            new_df.to_parquet(path, index=False)
            logger.info(f"Saved {len(new_df)} records to {path}")
    except Exception as e:
        logger.error(f"Failed to save data to {path}: {e}")


# fetch_page(url, params)
# Parameters: endpoint URL, query parameters dict
# Returns: parsed JSON response dict, or empty dict if all retries fail
def fetch_page(url, params):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=120)

            # 401 means bad API key — no point retrying
            if response.status_code == 401:
                logger.error("Invalid API key. Check your EIA_API_KEY.")
                raise ValueError("Invalid API credentials (401).")

            response.raise_for_status()
            return response.json()

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            logger.warning(f"Network error on attempt {attempt}/{MAX_RETRIES}: {e}")
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error on attempt {attempt}/{MAX_RETRIES}: {e}")

        if attempt < MAX_RETRIES:
            logger.info(f"Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    logger.error(f"Failed to fetch page after {MAX_RETRIES} attempts. Skipping.")
    return {}


# fetch_all_pages(endpoint_key, start_date, end_date)
# Parameters: endpoint name ("facility", "generator", "us"), optional start and end dates
# Returns: flat list of all records fetched across all pages
def fetch_all_pages(endpoint_key, start_date=None, end_date=None):
    url = f"{EIA_URL}/{ENDPOINTS[endpoint_key]}"
    all_records = []
    offset = 0
    if start_date:
        logger.info(f"Starting incremental extraction for: {endpoint_key} (from {start_date})")
    else:
        logger.info(f"Starting full extraction for: {endpoint_key}")
    while True:
        params = {
            "api_key": API_KEY,
            "offset": offset,
            "length": PAGE_SIZE,
            "out": "json",
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        }
        if start_date:
            params["start"] = str(start_date)
        if end_date:
            params["end"] = str(end_date)
        for col in DATA_COLUMNS.get(endpoint_key, []):
            params[f"data[{col}]"] = col
        if MAX_RECORDS is not None:
            params["length"] = min(PAGE_SIZE, MAX_RECORDS - len(all_records))
        logger.info(f"Fetching {endpoint_key} | offset={offset}")
        data = fetch_page(url, params)
        records = data.get("response", {}).get("data", [])
        total = int(data.get("response", {}).get("total", 0))
        if not records:
            logger.info(f"No more records for {endpoint_key}. Total fetched: {len(all_records)}")
            break
        all_records.extend(records)
        logger.info(f"Fetched {len(records)} records (running total: {len(all_records)} / {total})")
        if MAX_RECORDS is not None and len(all_records) >= MAX_RECORDS:
            break
        if len(all_records) >= total:
            break
        offset += PAGE_SIZE

    return all_records


if __name__ == "__main__":
    # If API_KEY is None or empty, stop before making any requests
    if not API_KEY:
        logger.error("EIA_API_KEY not found. Set it in your .env file or environment.")
        raise SystemExit("Missing EIA_API_KEY environment variable.")
    logger.info("API key loaded successfully.")
    # Test the key against the API, if it gets rejected, stop before extracting any data
    if not validate_api_key():
        raise SystemExit("Aborting: API key validation failed.")

    # START DATE (first run only, when no data is saved yet)
    # THIS SETTING WILL PROVE THE REFRESH BUTTON WORKS IN THE FRONT END
    START_DATE = "2026-03-15"                              # DEMO: starts from March 15
    # START_DATE = str(date.today() - timedelta(days=7))  # PRODUCTION: starts from 7 days ago

    # END DATE
    END_DATE = "2026-03-20"   # DEMO: stops at March 20 so refresh can bring March 21-23
    # END_DATE = None         # PRODUCTION: fetches up to today

    # Incremental load: check what's already saved, only fetch what's new
    facility_last = get_last_date("facility_nuclear_outages") or START_DATE
    facility_data = fetch_all_pages("facility", start_date=facility_last, end_date=END_DATE)
    facility_data = validate_records(facility_data, "facility")
    save_to_parquet(facility_data, "facility_nuclear_outages")

    generator_last = get_last_date("generator_nuclear_outages") or START_DATE
    generator_data = fetch_all_pages("generator", start_date=generator_last, end_date=END_DATE)
    generator_data = validate_records(generator_data, "generator")
    save_to_parquet(generator_data, "generator_nuclear_outages")

    us_last = get_last_date("us_nuclear_outages") or START_DATE
    us_data = fetch_all_pages("us", start_date=us_last, end_date=END_DATE)
    us_data = validate_records(us_data, "us")
    save_to_parquet(us_data, "us_nuclear_outages")
