import os
import threading
import pandas as pd
from fastapi import FastAPI, Query, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

import connector

load_dotenv()

app = FastAPI(title="Nuclear Outages API", redoc_url=None)

APP_API_KEY = os.getenv("APP_API_KEY")
api_key_header = APIKeyHeader(name="X-API-Key")

TABLES = {
    "facility":  "data/facility.parquet",
    "generator": "data/generator.parquet",
    "us":        "data/us_nuclear.parquet",
}

refresh_lock = threading.Lock()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# require_api_key(key)
# Parameters: API key string from the X-API-Key request header
# Returns: nothing — raises HTTP 401 if the key does not match APP_API_KEY
def require_api_key(key: str = Security(api_key_header)):
    if key != APP_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")


# parse_date(value, param_name)
# Parameters: date string to validate, parameter name used in the error message
# Returns: the original value unchanged, or raises HTTP 400 if the format is invalid
def parse_date(value, param_name):
    if value is None:
        return None
    try:
        pd.to_datetime(value)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format for '{param_name}': '{value}'. Use YYYY-MM-DD."
        )
    return value


# load_table(table_name, start, end)
# Parameters: table key ("facility", "generator", "us"), optional start and end date strings
# Returns: filtered DataFrame, or raises HTTP 404/500 if the file is missing or unreadable
def load_table(table_name, start=None, end=None):
    path = TABLES[table_name]
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"No data for '{table_name}'. Run /refresh first.")
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read '{table_name}': {e}")
    df["period"] = pd.to_datetime(df["period"], errors="coerce")
    if start:
        df = df[df["period"] >= pd.to_datetime(start)]
    if end:
        df = df[df["period"] <= pd.to_datetime(end)]
    return df



# GET /data
# Parameters: table, optional start/end dates, optional facility filter, page, page_size
# Returns: paginated rows from the requested table with total record and page counts
@app.get("/data", dependencies=[Depends(require_api_key)])
def get_data(
    table: str = Query(..., description="Table to query: facility, generator, or us"),
    start: str = Query(None, description="Filter from date (YYYY-MM-DD)"),
    end: str = Query(None, description="Filter to date (YYYY-MM-DD)"),
    facility: str = Query(None, description="Filter by plant ID (facility and generator only)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Records per page (max 1000)"),
):
    if table not in TABLES:
        raise HTTPException(status_code=400, detail=f"Invalid table '{table}'. Choose from: {list(TABLES.keys())}")

    parse_date(start, "start")
    parse_date(end, "end")

    df = load_table(table, start=start, end=end)

    if facility:
        if "facility" not in df.columns:
            raise HTTPException(status_code=400, detail="The 'facility' filter is not available for the 'us' table.")
        df = df[df["facility"].astype(str) == str(facility)]

    total_records = len(df)
    total_pages   = max(1, (total_records + page_size - 1) // page_size)
    page_df       = df.iloc[(page - 1) * page_size : page * page_size].copy()
    page_df["period"] = page_df["period"].dt.strftime("%Y-%m-%d")

    return {
        "table": table,
        "total_records": total_records,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "data": page_df.to_dict(orient="records"),
    }



# POST /refresh
# Parameters: none
# Returns: status "started" or "already_running" — refresh runs in a background thread
@app.post("/refresh", dependencies=[Depends(require_api_key)])
def refresh():
    if not connector.API_KEY:
        raise HTTPException(status_code=500, detail="EIA_API_KEY is not set. Check your .env file.")

    if not refresh_lock.acquire(blocking=False):
        return {"status": "already_running", "message": "A refresh is already in progress."}

    def run_refresh():
        try:
            import model as m
            import logging
            from datetime import timedelta
            logger = logging.getLogger("refresh")
            for endpoint_key, filename, build_fn in [
                ("facility",  "facility",   m.build_facility),
                ("generator", "generator",  m.build_generator),
                ("us",        "us_nuclear", m.build_us_nuclear),
            ]:
                last_date = connector.get_last_date(filename)
                if last_date:
                    last_date = last_date + timedelta(days=1)
                records = connector.fetch_all_pages(endpoint_key, start_date=last_date)
                records = connector.validate_records(records, endpoint_key)
                if not records:
                    continue
                new_df = build_fn(pd.DataFrame(records))
                path   = f"data/{filename}.parquet"
                if os.path.exists(path):
                    combined = pd.concat([pd.read_parquet(path), new_df], ignore_index=True).drop_duplicates()
                    combined.to_parquet(path, index=False)
                else:
                    new_df.to_parquet(path, index=False)
        except Exception as e:
            logging.getLogger("refresh").error(f"Refresh failed: {e}", exc_info=True)
        finally:
            refresh_lock.release()

    threading.Thread(target=run_refresh, daemon=True).start()
    return {"status": "started", "message": "Incremental refresh started in the background."}



# GET /status
# Parameters: none
# Returns: record count and date range for each of the three tables
@app.get("/status", dependencies=[Depends(require_api_key)])
def status():
    result = {}
    for name, path in TABLES.items():
        if not os.path.exists(path):
            result[name] = {"exists": False}
            continue
        try:
            df = pd.read_parquet(path, columns=["period"])
            df["period"] = pd.to_datetime(df["period"], errors="coerce")
            result[name] = {
                "exists": True,
                "records": len(df),
                "from": df["period"].min().strftime("%Y-%m-%d"),
                "to":   df["period"].max().strftime("%Y-%m-%d"),
            }
        except Exception as e:
            result[name] = {"exists": True, "error": str(e)}
    return result



# GET /analytics
# Parameters: query name, optional limit, date, threshold, start, and end
# Returns: aggregated result set for the requested query name
@app.get("/analytics", dependencies=[Depends(require_api_key)])
def analytics(
    query: str  = Query(..., description="Query name"),
    limit: int  = Query(10, ge=1, le=100, description="Top N results (default 10)"),
    date: str   = Query(None, description="Specific date YYYY-MM-DD (required for contribution_by_date)"),
    threshold: float = Query(10.0, ge=0, le=100, description="% threshold for spike_contributors"),
    start: str  = Query(None, description="Restrict data from this date (YYYY-MM-DD)"),
    end: str    = Query(None, description="Restrict data until this date (YYYY-MM-DD)"),
):
    parse_date(date, "date")
    parse_date(start, "start")
    parse_date(end, "end")
    try:
        return _run_query(query, limit=limit, date=date, threshold=threshold, start=start, end=end)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


# _run_query(query, limit, date, threshold, start, end)
# Parameters: query name and optional filter values passed from the analytics endpoint
# Returns: dict with query name and data list, or raises HTTP 400/404/500 on failure
def _run_query(query, limit=10, date=None, threshold=10.0, start=None, end=None):

    # load(table_key) — reads a parquet table, casts types, and applies date filters
    def load(table_key):
        path = TABLES[table_key]
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail=f"No data for '{table_key}'. Run /refresh first.")
        try:
            df = pd.read_parquet(path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read '{table_key}': {e}")
        df["period"] = pd.to_datetime(df["period"], errors="coerce")
        for col in ["capacity", "outage", "percentOutage"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if start:
            df = df[df["period"] >= pd.to_datetime(start)]
        if end:
            df = df[df["period"] <= pd.to_datetime(end)]
        return df

    # plant_names() — returns a deduplicated facility → facilityName lookup DataFrame
    def plant_names():
        path = TABLES["facility"]
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="No facility data. Run /refresh first.")
        return pd.read_parquet(path, columns=["facility", "facilityName"]).drop_duplicates("facility")

    # compute_streaks(df, group_cols) — returns the longest consecutive offline streak per group
    def compute_streaks(df, group_cols):
        df = df.sort_values(group_cols + ["period"]).copy()
        df["offline"]   = df["outage"] > 0
        grp_change      = (df[group_cols] != df[group_cols].shift()).any(axis=1)
        df["streak_id"] = (~df["offline"] | grp_change).cumsum()
        offline = df[df["offline"]]
        if offline.empty:
            return pd.DataFrame(columns=group_cols + ["streak_days"])
        lengths = offline.groupby(group_cols + ["streak_id"]).size().reset_index(name="streak_days")
        return lengths.groupby(group_cols)["streak_days"].max().reset_index()


    # All key metrics per plant: capacity, total outage, avg %, offline/online days, worst day
    if query == "plant_summary":
        df      = load("facility")
        total   = df.groupby(["facility", "facilityName"])["outage"].sum().reset_index()
        total.columns = ["facility", "facilityName", "total_outage_mw"]
        cap_max = df.groupby("facility")["capacity"].max().reset_index(name="max_capacity_mw")
        cap_min = df.groupby("facility")["capacity"].min().reset_index(name="min_capacity_mw")
        avg_pct = df.groupby("facility")["percentOutage"].mean().round(2).reset_index(name="avg_pct_outage")
        offline = df[df["outage"] > 0].groupby("facility").size().reset_index(name="offline_days")
        online  = df[df["outage"] == 0].groupby("facility").size().reset_index(name="online_days")
        idx     = df.groupby("facility")["outage"].idxmax()
        worst   = df.loc[idx][["facility", "period", "outage", "percentOutage"]].copy()
        worst   = worst.rename(columns={"period": "worst_day", "outage": "worst_day_mw", "percentOutage": "worst_day_pct"})
        worst["worst_day"]     = worst["worst_day"].dt.strftime("%Y-%m-%d")
        worst["worst_day_pct"] = worst["worst_day_pct"].round(1)
        r = (total
             .merge(cap_max, on="facility", how="left")
             .merge(cap_min, on="facility", how="left")
             .merge(avg_pct, on="facility", how="left")
             .merge(offline, on="facility", how="left")
             .merge(online,  on="facility", how="left")
             .merge(worst,   on="facility", how="left"))
        r["offline_days"] = r["offline_days"].fillna(0).astype(int)
        r["online_days"]  = r["online_days"].fillna(0).astype(int)
        r = r.sort_values("total_outage_mw", ascending=False).head(limit)
        cols = ["facility", "facilityName", "min_capacity_mw", "max_capacity_mw",
                "avg_pct_outage", "offline_days", "online_days",
                "worst_day", "worst_day_mw", "worst_day_pct"]
        return {"query": query, "data": r[cols].to_dict(orient="records")}

    # Average outage per facility grouped by calendar month
    elif query == "monthly_avg":
        df = load("facility")
        df["month"] = df["period"].dt.to_period("M").astype(str)
        r = df.groupby(["facilityName", "month"])["outage"].mean().reset_index()
        r.columns = ["facilityName", "month", "avg_outage_mw"]
        r["avg_outage_mw"] = r["avg_outage_mw"].round(2)
        return {"query": query, "data": r.sort_values(["facilityName", "month"]).to_dict(orient="records")}

    # Number of days each generator unit had outage > 0
    elif query == "generator_failures":
        df = load("generator")
        r  = df[df["outage"] > 0].groupby(["facility", "generator"]).size().reset_index(name="failure_days")
        r  = r.merge(plant_names(), on="facility", how="left")
        r  = r.sort_values("failure_days", ascending=False).head(limit)
        return {"query": query, "data": r[["facilityName", "generator", "failure_days"]].to_dict(orient="records")}

    # Longest consecutive days offline per generator unit
    elif query == "longest_streak":
        df      = load("generator")
        streaks = compute_streaks(df, ["facility", "generator"])
        if streaks.empty:
            return {"query": query, "data": []}
        r = streaks.merge(plant_names(), on="facility", how="left")
        r = r.sort_values("streak_days", ascending=False).head(limit)
        return {"query": query, "data": r[["facilityName", "generator", "streak_days"]].to_dict(orient="records")}

    # Average share (%) each generator contributes to its facility's total outage
    elif query == "generator_contribution":
        gen    = load("generator")
        fac    = load("facility")[["facility", "period", "outage"]].rename(columns={"outage": "facility_outage"})
        merged = gen.merge(fac, on=["facility", "period"], how="left")
        merged = merged.merge(plant_names(), on="facility", how="left")
        merged = merged[merged["facility_outage"] > 0]
        merged["contribution_pct"] = (merged["outage"] / merged["facility_outage"] * 100).round(2)
        r = merged.groupby(["facilityName", "generator"])["contribution_pct"].mean().reset_index()
        r = r.sort_values("contribution_pct", ascending=False).head(limit)
        return {"query": query, "data": r.to_dict(orient="records")}

    # Facilities where a single generator causes more than 90% of total outages
    elif query == "single_generator_outage":
        gen       = load("generator")
        fac_total = gen.groupby(["facility", "period"])["outage"].sum().reset_index(name="total")
        merged    = gen[["facility", "generator", "period", "outage"]].merge(fac_total, on=["facility", "period"])
        merged    = merged[merged["total"] > 0]
        merged["share"] = merged["outage"] / merged["total"]
        r = merged.groupby(["facility", "generator"])["share"].mean().reset_index()
        r = r[r["share"] > 0.9].sort_values("share", ascending=False)
        r = r.merge(plant_names(), on="facility", how="left")
        r["share_pct"] = (r["share"] * 100).round(2)
        return {"query": query, "data": r[["facilityName", "generator", "share_pct"]].head(limit).to_dict(orient="records")}

    # Max capacity per generator and how many days it was offline
    elif query == "generator_capacity":
        df   = load("generator").merge(plant_names(), on="facility", how="left")
        cap  = df.groupby(["facilityName", "generator"])["capacity"].max().reset_index(name="max_capacity_mw")
        down = df[df["outage"] > 0].groupby(["facilityName", "generator"]).size().reset_index(name="offline_days")
        r    = cap.merge(down, on=["facilityName", "generator"], how="left").fillna(0)
        r["offline_days"] = r["offline_days"].astype(int)
        r = r.sort_values("max_capacity_mw", ascending=False).head(limit)
        return {"query": query, "data": r.to_dict(orient="records")}

    # Average national % outage grouped by calendar month (Jan–Dec)
    elif query == "national_by_month":
        df = load("us")
        df["month"] = df["period"].dt.month
        r  = df.groupby("month")["percentOutage"].mean().reset_index()
        r.columns = ["month_num", "avg_pct_outage"]
        r["avg_pct_outage"] = r["avg_pct_outage"].round(2)
        names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                 7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
        r["month"] = r["month_num"].map(names)
        return {"query": query, "data": r[["month", "avg_pct_outage"]].to_dict(orient="records")}

    # Year-over-year average outage and % outage nationally
    elif query == "national_by_year":
        df = load("us")
        df["year"] = df["period"].dt.year
        r  = df.groupby("year")[["outage", "percentOutage"]].mean().round(2).reset_index()
        r.columns = ["year", "avg_outage_mw", "avg_pct_outage"]
        return {"query": query, "data": r.sort_values("year").to_dict(orient="records")}

    # Each facility's share of national outage on a specific date
    elif query == "contribution_by_date":
        if not date:
            raise HTTPException(status_code=400, detail="'date' parameter is required (YYYY-MM-DD).")
        fac     = load("facility")
        us      = load("us")
        target  = pd.to_datetime(date)
        fac_day = fac[fac["period"] == target][["facilityName", "outage"]].copy()
        us_day  = us[us["period"] == target]
        if fac_day.empty or us_day.empty:
            return {"query": query, "date": date, "data": []}
        nat_outage = float(us_day["outage"].iloc[0])
        fac_day["contribution_pct"] = (fac_day["outage"] / nat_outage * 100).round(2) if nat_outage > 0 else 0.0
        return {"query": query, "date": date, "national_outage_mw": nat_outage,
                "data": fac_day.sort_values("contribution_pct", ascending=False).to_dict(orient="records")}

    # Each facility's average historical share of national outage
    elif query == "avg_contribution":
        fac    = load("facility")
        us     = load("us")[["period", "outage"]].rename(columns={"outage": "national_outage"})
        merged = fac.merge(us, on="period", how="inner")
        merged = merged[merged["national_outage"] > 0]
        merged["contribution_pct"] = (merged["outage"] / merged["national_outage"] * 100).round(4)
        r = merged.groupby("facilityName")["contribution_pct"].mean().reset_index()
        r.columns = ["facilityName", "avg_contribution_pct"]
        r["avg_contribution_pct"] = r["avg_contribution_pct"].round(2)
        r = r.sort_values("avg_contribution_pct", ascending=False).head(limit)
        return {"query": query, "data": r.to_dict(orient="records")}

    # Facilities that were offline on the days with the highest national outage
    elif query == "peak_national_days":
        us   = load("us")
        fac  = load("facility")
        top  = us.nlargest(limit, "outage")[["period", "outage", "percentOutage"]]
        down = fac[fac["outage"] > 0][["period", "facilityName", "outage"]]
        merged = top.merge(down, on="period", how="left")
        merged = merged.rename(columns={"outage_x": "national_outage_mw", "outage_y": "facility_outage_mw"})
        merged["period"] = merged["period"].dt.strftime("%Y-%m-%d")
        return {"query": query, "data": merged.to_dict(orient="records")}

    # Top contributing facilities on days where national % outage exceeded the threshold
    elif query == "spike_contributors":
        fac    = load("facility")
        us     = load("us")[["period", "outage", "percentOutage"]].rename(
                     columns={"outage": "national_outage", "percentOutage": "national_pct"})
        spikes = us[us["national_pct"] > threshold]
        if spikes.empty:
            return {"query": query, "threshold": threshold, "data": []}
        merged = fac.merge(spikes[["period", "national_outage"]], on="period", how="inner")
        merged = merged[merged["national_outage"] > 0]
        merged["contribution_pct"] = (merged["outage"] / merged["national_outage"] * 100).round(2)
        r = merged.groupby("facilityName")["contribution_pct"].mean().reset_index()
        r = r.sort_values("contribution_pct", ascending=False).head(limit)
        return {"query": query, "threshold": threshold, "data": r.to_dict(orient="records")}

    else:
        raise HTTPException(status_code=400, detail=f"Unknown query: '{query}'.")
