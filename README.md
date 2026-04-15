# Nuclear Outages — EIA Data Pipeline

A data pipeline that extracts U.S. nuclear power plant outage data from the [EIA Open Data API](https://www.eia.gov/opendata/), stores it locally as Parquet files, exposes it through a REST API, and displays it in a web dashboard.

---

## Quick Start

### 1. Clone and install

```bash
git clone <your-repo-url>
cd eia-outages
pip install -r requirements.txt
```

### 2. Set up API keys

```bash
python -c "open('.env', 'w', encoding='utf-8').write('EIA_API_KEY=XXXXXXXXXXX\nAPP_API_KEY=XXXXXXXXX\n')"
```

Then open `.env` and replace the `X` values with your actual keys:

- **EIA_API_KEY** — free key from https://www.eia.gov/opendata/register.php
- **APP_API_KEY** — any string you choose; used to protect the REST API

### 3. Extract data from the EIA API

```bash
python connector.py
```

Downloads nuclear outage data for all 3 endpoints and saves raw Parquet files to `data/`. On the first run it fetches everything available. On subsequent runs it only fetches records newer than what is already stored.

### 4. Build the data model

```bash
python model.py
```

Reads the raw Parquet files, converts all columns to proper types, applies the normalized schema, and saves the final tables back to `data/`. Raw files are deleted after a successful build.

### 5. Start the API

```bash
python -m uvicorn api:app --port 8000 --reload
```

API available at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

### 6. Open the dashboard

```bash
python -m http.server 3000
```

Then open **http://localhost:3000** in your browser.

> Do not open `index.html` by double-clicking it. Browsers block requests from `file://` URLs to `localhost` APIs due to CORS policy.

---

## Project Structure

```
eia-outages/
│
├── connector.py        # Part 1 — Pulls data from the EIA API (incremental)
├── model.py            # Part 2 — Cleans and normalizes raw data into final tables
├── api.py              # Part 3 — FastAPI REST service (data + analytics)
├── index.html          # Part 4 — Web dashboard
│
├── tests/
│   └── tests.py        # Unit + integration tests
│
├── data/               # Generated at runtime — not committed to git
│   ├── facility.parquet
│   ├── generator.parquet
│   └── us_nuclear.parquet
│
├── er_diagram.md       # ER diagram (Mermaid)
├── .env                # Your API keys — never committed
├── .gitignore
└── requirements.txt
```

---

## Data Model

See ER_DIAGRAM.pdf for the full ER diagram.

Data is stored in three tables, each at a different level of granularity:

- **`facility`** — one row per plant per day. Tracks total capacity and outage at the plant level. Each plant has a unique ID (`facility`) and a human-readable name (`facilityName`).
- **`generator`** — one row per reactor unit per day. Breaks down outage to the individual unit inside a plant. Links back to `facility` via the `facility` foreign key.
- **`us_nuclear`** — one row per day, national totals only. Aggregates capacity and outage across all U.S. nuclear plants for that date.

---

## API Reference

All endpoints are protected by an API key. To explore them interactively:

1. Start the API and open **http://localhost:8000/docs** in your browser
2. Click the **Authorize** button (top right, lock icon)
3. Paste your `APP_API_KEY` value in the **Value** field and click **Authorize** → **Close**
4. Pick any endpoint → **Try it out** → **Execute** to see the JSON response

---

## Tests

```bash
python -m pytest tests/ -v
```

`tests/tests.py` covers:

- **Connector** — `validate_records` (accepts valid, drops missing/null fields, handles mixed batches), `get_last_date` (returns `None` on first run, returns correct max date), `save_to_parquet` (creates file, appends new records, deduplicates)
- **API** — authentication (401 on missing or wrong key), `/data` (structure, date filter, facility filter, `us` table rejects facility filter, pagination, page size cap), `/refresh` (always returns 200)

---

## Assumptions

- **Incremental extraction:** The connector checks the most recent `period` already stored and only fetches records newer than that. The first run does a full load.
- **Explicit data columns:** The EIA API requires `capacity`, `outage`, and `percentOutage` to be explicitly requested via `data[field]=field` parameters — they are not returned by default.
- **Data cap:** `MAX_RECORDS = 500` per endpoint in `connector.py` is set for development. Increase it for a full historical load.
- **Local storage:** `data/` is excluded from version control. Anyone cloning must run `connector.py` then `model.py` before starting the API.
- **API authentication:** A static key passed in `X-API-Key`. Defined in `.env`, never hardcoded.
- **EIA key activation delay:** Newly registered EIA API keys can take a few minutes to become active. If `connector.py` returns a 403 or an authentication error right after registering, wait a few minutes and try again.
