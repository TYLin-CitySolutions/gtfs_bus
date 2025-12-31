# gtfs_bus

GTFS static ingestion + a Streamlit dashboard that counts scheduled bus trips near selected stops in a time window.

## What it does
- Ingests one or more GTFS static zip files into per-feed Parquet tables.
- Publishes combined Parquet tables to GitHub Pages via a workflow.
- Serves a Streamlit app that queries the Parquet tables with DuckDB and maps results.

## Data flow (high level)
1) Download GTFS static zip(s).
2) Update `ingest/feeds.yml` with feed `id` and source location.
3) Run `python ingest/ingest_gtfs.py` to create per-feed Parquet files.
4) Commit and push the `parquet/` output.
5) GitHub Actions combines per-feed files into single Parquet tables and publishes to GitHub Pages.
6) The Streamlit app reads the Parquet tables (local path or GitHub Pages URL) and runs queries.

## Repo layout
- `ingest/ingest_gtfs.py`: converts GTFS text files into Parquet tables.
- `ingest/feeds.yml`: feed list and download locations.
- `parquet/`: per-feed Parquet output (one file per feed per table).
- `.github/workflows/build_pages.yml`: combines and publishes Parquet to Pages.
- `buses_app.py`: Streamlit dashboard and query logic.

## Setup
- Clone repo
- Create and activate a venv
- Install deps

```powershell
python -m venv .venv
.\.venv\Scripts\Activate
pip install -r requirements.txt
```

## Ingest or update data
1) Download a GTFS static zip for each agency.
2) Edit `ingest/feeds.yml`:
   - `id`: unique feed identifier used in output filenames and queries
   - `source`: `file`, `url`/`sharelink`, or `graph`
   - `url` or `path`: where to read the GTFS zip
   - `graph` sources also require `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`, plus `site_id`, `drive_id`, `item_path`
3) Run the ingest script:

```powershell
python ingest\ingest_gtfs.py
```

This produces:
- `parquet/dim_stops/*.parquet`
- `parquet/dim_routes/*.parquet`
- `parquet/dim_trips/*.parquet`
- `parquet/calendar_base/*.parquet`
- `parquet/fact_stop_events/*.parquet`

4) Commit and push the `parquet/` folder so the workflow can combine and publish.

## Run the Streamlit app
The app reads from a base Parquet location. By default it points to the GitHub Pages site, but you can override it.

```powershell
$env:PARQ_BASE_URL = "C:\\path\\to\\repo\\parquet"
streamlit run buses_app.py
```

You can also point to the published Pages URL (already the default):

```powershell
$env:PARQ_BASE_URL = "https://tylin-citysolutions.github.io/gtfs_bus"
streamlit run buses_app.py
```

## Using the dashboard
- Choose day type, time window, radius, and feeds.
- Click the map to select intersections, add sites, then run the query.
- Results list scheduled buses by route/direction and show stops on a map.

## Notes
- The query uses `calendar.txt` service days (weekday/Sat/Sun). Exceptions in `calendar_dates.txt` are not applied.
- The GitHub workflow expects per-feed files under `parquet/<table>/*.parquet`.
