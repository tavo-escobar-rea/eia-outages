import os
import pandas as pd

# ============================================================
# PART 2: DATA MODEL
# Reads raw parquet files from the connector and saves each
# table as a separate file with proper types and clean schema.
#
# Output: data/facility.parquet
#         data/generator.parquet
#         data/us_nuclear.parquet
# ============================================================


# load_raw(filename)
# Parameters: parquet filename without path or extension
# Returns: raw DataFrame from data/<filename>.parquet, raises if the file is missing
def load_raw(filename):
    path = f"data/{filename}.parquet"
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing: {path}. Run connector.py first.")
    try:
        return pd.read_parquet(path)
    except Exception as e:
        raise RuntimeError(f"Failed to read {path}: {e}")


# build_facility(df)
# Parameters: raw facility DataFrame from the connector
# Returns: cleaned DataFrame with proper types, sorted by (facility, period)
def build_facility(df):
    # Select only the columns we need for this table
    table = df[["facility", "facilityName", "period", "capacity", "outage", "percentOutage"]].copy()
    # API returns all values as strings — convert to proper types
    table["period"]        = pd.to_datetime(table["period"])
    table["capacity"]      = pd.to_numeric(table["capacity"], errors="coerce")
    table["outage"]        = pd.to_numeric(table["outage"], errors="coerce")
    table["percentOutage"] = pd.to_numeric(table["percentOutage"], errors="coerce")
    # Sort by primary key (facility, period)
    return table.sort_values(["facility", "period"]).reset_index(drop=True)


# build_generator(df)
# Parameters: raw generator DataFrame from the connector
# Returns: cleaned DataFrame with proper types, sorted by (facility, generator, period)
def build_generator(df):
    # Select only the columns we need for this table
    # facilityName is intentionally excluded — it lives in facility and is fetched via JOIN
    table = df[["facility", "generator", "period", "capacity", "outage", "percentOutage"]].copy()
    # API returns all values as strings — convert to proper types
    table["generator"]     = pd.to_numeric(table["generator"], errors="coerce").astype("Int64")
    table["period"]        = pd.to_datetime(table["period"])
    table["capacity"]      = pd.to_numeric(table["capacity"], errors="coerce")
    table["outage"]        = pd.to_numeric(table["outage"], errors="coerce")
    table["percentOutage"] = pd.to_numeric(table["percentOutage"], errors="coerce")
    # Sort by primary key (facility, generator, period)
    return table.sort_values(["facility", "generator", "period"]).reset_index(drop=True)


# build_us_nuclear(df)
# Parameters: raw us_nuclear DataFrame from the connector
# Returns: cleaned DataFrame with proper types, sorted by period
def build_us_nuclear(df):
    # Select only the columns we need for this table
    table = df[["period", "capacity", "outage", "percentOutage"]].copy()
    # API returns all values as strings — convert to proper types
    table["period"]        = pd.to_datetime(table["period"])
    table["capacity"]      = pd.to_numeric(table["capacity"], errors="coerce")
    table["outage"]        = pd.to_numeric(table["outage"], errors="coerce")
    table["percentOutage"] = pd.to_numeric(table["percentOutage"], errors="coerce")
    # Sort by primary key (period)
    return table.sort_values("period").reset_index(drop=True)


# save(df, name)
# Parameters: cleaned DataFrame, output filename without path or extension
# Returns: nothing — saves to data/<name>.parquet
def save(df, name):
    os.makedirs("data", exist_ok=True)
    path = f"data/{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"  Saved {name}: {len(df)} rows -> {path}")


if __name__ == "__main__":
    print("Loading raw data...")
    facility_raw  = load_raw("facility_nuclear_outages")
    generator_raw = load_raw("generator_nuclear_outages")
    us_raw        = load_raw("us_nuclear_outages")

    print("\nBuilding model tables...")
    facility   = build_facility(facility_raw)
    generator  = build_generator(generator_raw)
    us_nuclear = build_us_nuclear(us_raw)

    print("\nSaving to data/...")
    save(facility,   "facility")
    save(generator,  "generator")
    save(us_nuclear, "us_nuclear")

    print("\nCleaning up raw files...")
    for f in ["facility_nuclear_outages", "generator_nuclear_outages", "us_nuclear_outages"]:
        path = f"data/{f}.parquet"
        if os.path.exists(path):
            os.remove(path)
            print(f"  Deleted {path}")

    print("\nDone.")
