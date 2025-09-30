import io, zipfile, os, requests, pandas as pd
from pathlib import Path
import yaml, shutil
from pyproj import Transformer

OUT = Path("parquet"); OUT.mkdir(exist_ok=True, parents=True)

def to_sec(hms: str) -> int:
    h, m, s = (list(map(int, (hms+":00").split(":")[:3])))
    return h*3600 + m*60 + s

def load_zip_bytes(feed_cfg: dict) -> bytes:
    src = feed_cfg.get("source", "url").lower()
    if src == "file":
        p = feed_cfg.get("path") or feed_cfg.get("url")
        if not p:
            raise ValueError("source:file requires a 'path'.")
        return Path(p).read_bytes()
    elif src in ("url", "sharelink"):
        r = requests.get(feed_cfg["url"], timeout=90)
        r.raise_for_status()
        return r.content
    elif src == "graph":
        from sharepoint import sp_get_access_token, sp_download_file
        token = sp_get_access_token(os.environ["TENANT_ID"], os.environ["CLIENT_ID"], os.environ["CLIENT_SECRET"])
        return sp_download_file(feed_cfg["site_id"], feed_cfg["drive_id"], feed_cfg["item_path"], token)
    raise ValueError(f"Unknown source: {src}")

def load_gtfs_tables(zip_bytes: bytes) -> dict[str, pd.DataFrame]:
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    def read(name):
        with z.open(name) as f:
            return pd.read_csv(f)
    tables = {
        "routes": read("routes.txt"),
        "trips": read("trips.txt"),
        "stops": read("stops.txt"),
        "stop_times": read("stop_times.txt"),
        "calendar": read("calendar.txt"),
    }
    if "calendar_dates.txt" in z.namelist():
        tables["calendar_dates"] = read("calendar_dates.txt")
    return tables

def build_one(feed_id: str, t: dict[str, pd.DataFrame]):
    OUT.mkdir(parents=True, exist_ok=True)
    for sub in ["dim_stops","dim_trips","dim_routes","calendar_base","fact_stop_events"]:
        (OUT / sub).mkdir(parents=True, exist_ok=True)

    # --- stops (lat/lon + x2263/y2263 for 250-ft math) ---
    stops = (t["stops"]
             .rename(columns={"stop_lat":"lat","stop_lon":"lon"})
             .assign(feed_id=feed_id)
    )
        # ensure same datatype for each feed
    if "stop_desc" in stops.columns:
        stops["stop_desc"] = stops["stop_desc"].astype(str)
    STOP_COLS = ["stop_id","stop_name","stop_desc","lat","lon",'location_type',"parent_station","zone_id","feed_id"]
    for c in STOP_COLS:
        if c not in stops.columns:
            stops[c] = None
    stops = stops[STOP_COLS]

    tf = Transformer.from_crs("EPSG:4326", "EPSG:2263", always_xy=True)
    x, y = tf.transform(stops["lon"].to_numpy(), stops["lat"].to_numpy())
    stops["x2263"] = x
    stops["y2263"] = y

    # --- trips / routes / calendar ---
    trips = t["trips"][["trip_id","route_id","direction_id","service_id","trip_headsign"]].assign(feed_id=feed_id)
    routes = t["routes"].assign(feed_id=feed_id)
    ROUTES_COLS = ['route_id','agency_id','route_short_name','route_long_name','route_desc','route_type','route_color','route_text_color',"feed_id"]
    for c in ROUTES_COLS:
        if c not in routes.columns:
            routes[c] = None
    routes = routes[ROUTES_COLS]
    cal = t["calendar"].assign(feed_id=feed_id)

    # --- stop_times → fact_stop_events (arrival_sec) ---
    st = t["stop_times"][["trip_id","stop_id","stop_sequence","arrival_time"]].copy()
    st["arrival_sec"] = st["arrival_time"].map(to_sec)

    fact = (st.merge(trips, on="trip_id", how="inner")
              [["route_id","direction_id","service_id","stop_id","stop_sequence","arrival_sec","trip_id","feed_id"]]
              .assign(feed_id=feed_id))

    # --- write ONE parquet per feed per table ---
    stops.to_parquet(OUT / f"dim_stops/{feed_id}.parquet", engine="pyarrow", compression="zstd", index=False)
    trips.to_parquet(OUT / f"dim_trips/{feed_id}.parquet", engine="pyarrow", compression="zstd", index=False)
    routes.to_parquet(OUT / f"dim_routes/{feed_id}.parquet", engine="pyarrow", compression="zstd", index=False)
    cal.to_parquet(OUT / f"calendar_base/{feed_id}.parquet", engine="pyarrow", compression="zstd", index=False)
    fact.to_parquet(OUT / f"fact_stop_events/{feed_id}.parquet", engine="pyarrow", compression="zstd", index=False)
    
def main():
    cfg = yaml.safe_load(Path("ingest/feeds.yml").read_text())["feeds"]
    # fresh rebuild, clears subfolders for refresh
    if (OUT).exists():
        for sub in ["dim_stops","dim_trips","dim_routes","calendar_base","fact_stop_events"]:
            shutil.rmtree(OUT/sub, ignore_errors=True)

    for feed in cfg:
        zbytes = load_zip_bytes(feed)
        tables = load_gtfs_tables(zbytes)
        build_one(feed["id"], tables)
    print("✅ Wrote Parquet tables to", OUT.resolve())

if __name__ == "__main__":
    main()
