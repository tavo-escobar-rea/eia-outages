# Essential tests for the Nuclear Outages pipeline.
# Covers connector (validate_records, get_last_date, save_to_parquet)
# and API (authentication, /data filters and pagination, /refresh).

import os
import pytest
import pandas as pd
from unittest.mock import patch
from fastapi.testclient import TestClient

import connector
import api

client = TestClient(api.app)
AUTH = {"X-API-Key": "eia-secret-2026"}


# CONNECTOR: validate_records
# Drops records missing a required field or containing null values before saving to parquet.

def test_validate_records_accepts_complete_record():
    # A fully populated facility record should pass through unchanged.
    records = [
        {"period": "2026-03-20", "facility": "46", "facilityName": "Browns Ferry",
         "capacity": 3755.8, "outage": 1259, "percentOutage": 33.52},
    ]
    assert len(connector.validate_records(records, "facility")) == 1


def test_validate_records_drops_missing_field():
    # A record missing "outage" must be dropped entirely.
    records = [
        {"period": "2026-03-20", "facility": "46", "facilityName": "Browns Ferry",
         "capacity": 3755.8, "percentOutage": 33.52},
    ]
    assert len(connector.validate_records(records, "facility")) == 0


def test_validate_records_drops_null_field():
    # A record with a None value on a required field must also be dropped.
    records = [
        {"period": "2026-03-20", "facility": "46", "facilityName": "Browns Ferry",
         "capacity": None, "outage": 1259, "percentOutage": 33.52},
    ]
    assert len(connector.validate_records(records, "facility")) == 0


def test_validate_records_keeps_only_valid_in_mixed_batch():
    # When mixing valid and invalid records, only the valid ones come back.
    records = [
        {"period": "2026-03-20", "facility": "46", "facilityName": "Browns Ferry",
         "capacity": 3755.8, "outage": 1259, "percentOutage": 33.52},
        {"period": "2026-03-20", "facility": "204", "facilityName": "Clinton",
         "capacity": None, "outage": 0, "percentOutage": 0},
    ]
    result = connector.validate_records(records, "facility")
    assert len(result) == 1
    assert result[0]["facility"] == "46"


# CONNECTOR: get_last_date
# Reads the max period from a saved parquet to enable incremental loads.

def test_get_last_date_returns_none_when_no_file(tmp_path, monkeypatch):
    # If no parquet exists yet, return None so the connector does a full load.
    monkeypatch.chdir(tmp_path)
    assert connector.get_last_date("facility") is None


def test_get_last_date_returns_most_recent_period(tmp_path, monkeypatch):
    # Should return the latest date in the file, not the first or a random one.
    monkeypatch.chdir(tmp_path)
    os.makedirs("data")
    df = pd.DataFrame({"period": ["2026-03-18", "2026-03-20", "2026-03-19"]})
    df.to_parquet("data/facility.parquet", index=False)
    assert str(connector.get_last_date("facility")) == "2026-03-20"


# CONNECTOR: save_to_parquet
# Creates the file on first run, appends on subsequent runs, and deduplicates rows.

def test_save_to_parquet_creates_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    records = [{"period": "2026-03-20", "facility": "46",
                "facilityName": "Browns Ferry", "capacity": 3755.8,
                "outage": 1259, "percentOutage": 33.52}]
    connector.save_to_parquet(records, "facility")
    assert os.path.exists("data/facility.parquet")
    assert len(pd.read_parquet("data/facility.parquet")) == 1


def test_save_to_parquet_appends_new_records(tmp_path, monkeypatch):
    # A second save with a different record should result in 2 rows total.
    monkeypatch.chdir(tmp_path)
    os.makedirs("data")
    existing = pd.DataFrame([{"period": "2026-03-19", "facility": "46",
                               "facilityName": "Browns Ferry", "capacity": 3755.8,
                               "outage": 1259, "percentOutage": 33.52}])
    existing.to_parquet("data/facility.parquet", index=False)
    connector.save_to_parquet(
        [{"period": "2026-03-20", "facility": "46", "facilityName": "Browns Ferry",
          "capacity": 3755.8, "outage": 1259, "percentOutage": 33.52}],
        "facility"
    )
    assert len(pd.read_parquet("data/facility.parquet")) == 2


def test_save_to_parquet_deduplicates(tmp_path, monkeypatch):
    # Saving the exact same record twice should not create a duplicate row.
    monkeypatch.chdir(tmp_path)
    os.makedirs("data")
    row = {"period": "2026-03-20", "facility": "46", "facilityName": "Browns Ferry",
           "capacity": 3755.8, "outage": 1259, "percentOutage": 33.52}
    pd.DataFrame([row]).to_parquet("data/facility.parquet", index=False)
    connector.save_to_parquet([row], "facility")
    assert len(pd.read_parquet("data/facility.parquet")) == 1


# API: authentication
# Every endpoint requires a valid X-API-Key header; wrong or missing keys return 401.

def test_missing_api_key_returns_401():
    assert client.get("/data?table=facility").status_code == 401


def test_wrong_api_key_returns_401():
    assert client.get("/data?table=facility",
                      headers={"X-API-Key": "wrong"}).status_code == 401


# API: /data
# Main query endpoint. Tests confirm structure, filters, pagination, and error handling.

def test_data_returns_facility_with_correct_structure():
    # Response must include table name, pagination metadata, and a data array.
    res = client.get("/data?table=facility&page_size=5", headers=AUTH)
    assert res.status_code == 200
    body = res.json()
    assert body["table"] == "facility"
    assert "total_records" in body and "total_pages" in body
    assert len(body["data"]) <= 5


def test_data_invalid_table_returns_400():
    assert client.get("/data?table=invalid", headers=AUTH).status_code == 400


def test_data_date_filter_returns_only_matching_rows():
    # Every row returned must fall within the requested date range.
    res = client.get("/data?table=facility&start=2026-03-20&end=2026-03-20",
                     headers=AUTH)
    assert res.status_code == 200
    for row in res.json()["data"]:
        assert row["period"] == "2026-03-20"


def test_data_facility_filter_returns_only_matching_plant():
    # Every row must belong to the requested facility ID.
    res = client.get("/data?table=facility&facility=46", headers=AUTH)
    assert res.status_code == 200
    for row in res.json()["data"]:
        assert row["facility"] == "46"


def test_data_facility_filter_not_available_on_us_table():
    # The us table has no facility column — filter must be rejected.
    assert client.get("/data?table=us&facility=46",
                      headers=AUTH).status_code == 400


def test_data_pagination_returns_different_pages():
    # Page 1 and page 2 must not contain the same records.
    p1 = client.get("/data?table=facility&page=1&page_size=10", headers=AUTH).json()
    p2 = client.get("/data?table=facility&page=2&page_size=10", headers=AUTH).json()
    ids_p1 = [r["period"] + str(r["facility"]) for r in p1["data"]]
    ids_p2 = [r["period"] + str(r["facility"]) for r in p2["data"]]
    assert ids_p1 != ids_p2


def test_data_page_size_above_limit_returns_422():
    # page_size is capped at 1000; anything above must be rejected by FastAPI.
    assert client.get("/data?table=facility&page_size=9999",
                      headers=AUTH).status_code == 422


# API: /refresh
# Triggers an incremental background load; always returns 200 even if already running.

def test_refresh_returns_200():
    res = client.post("/refresh", headers=AUTH)
    assert res.status_code == 200
    assert res.json()["status"] in ("started", "already_running")
