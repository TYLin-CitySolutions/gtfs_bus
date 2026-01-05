# app.py — Python-only dashboard + reusable function
import duckdb
import pandas as pd
import streamlit as st
from datetime import time
from pyproj import Transformer
import os
from pathlib import Path
from shapely.geometry import Point, LineString

import folium
from streamlit_folium import st_folium
from folium.plugins import Geocoder

# read from parquet
PARQ_BASE = (
    st.secrets.get("PARQ_BASE_URL")
    or os.getenv("PARQ_BASE_URL")
    or "https://tylin-citysolutions.github.io/gtfs_bus"
    or Path("parquet").resolve().as_posix()   # local dev default: ./parquet
    or r'parquet'
)


# Top-level helper for other module code (mirrors the in-function helper)
def parquet_path(table_name: str) -> str:
    base = PARQ_BASE.rstrip('/')
    if base.startswith("http"):
        return f"{base}/{table_name}.parquet"
    p = Path(base)
    single = p / f"{table_name}.parquet"
    folder = p / table_name
    if single.exists():
        return single.as_posix()
    if folder.exists() and any(folder.glob("*.parquet")):
        return (folder / "*.parquet").as_posix()
    return single.as_posix()

@st.cache_resource
def get_con():
    con = duckdb.connect()
    if PARQ_BASE.startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        # If you use S3/MinIO, set creds/region here from st.secrets/env
        # con.execute("SET s3_region='us-east-1'")
        # con.execute("SET s3_access_key_id=$1, s3_secret_access_key=$2", [AK, SK])
    return con

# ---------- helpers ----------
def to_sec(hms: str) -> int:
    hh, mm, *rest = hms.split(":")
    ss = int(rest[0]) if rest else 0
    return int(hh) * 3600 + int(mm) * 60 + ss

def _stop_side_by_segment_distance(
    stop_ctx: pd.DataFrame,
    intersection_lat: float,
    intersection_lon: float,
) -> pd.DataFrame:
    """
    Classify near/far side using distance from intersection to
    prev→current vs current→next segments.
    """
    keys = ["feed_id", "route_id", "direction_id", "service_id", "stop_id"]
    if stop_ctx.empty:
        return pd.DataFrame(columns=keys + ["stop_side"])

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:2263", always_xy=True)
    ix, iy = transformer.transform(intersection_lon, intersection_lat)
    intersection_point = Point(ix, iy)

    def _calc_side(row):
        dist_prev_current = None
        dist_current_next = None

        if pd.notna(row["prev_lat"]) and pd.notna(row["prev_lon"]):
            prev_x, prev_y = transformer.transform(row["prev_lon"], row["prev_lat"])
            cur_x, cur_y = transformer.transform(row["stop_lon"], row["stop_lat"])
            prev_current_line = LineString([(prev_x, prev_y), (cur_x, cur_y)])
            dist_prev_current = intersection_point.distance(prev_current_line)

        if pd.notna(row["next_lat"]) and pd.notna(row["next_lon"]):
            cur_x, cur_y = transformer.transform(row["stop_lon"], row["stop_lat"])
            next_x, next_y = transformer.transform(row["next_lon"], row["next_lat"])
            current_next_line = LineString([(cur_x, cur_y), (next_x, next_y)])
            dist_current_next = intersection_point.distance(current_next_line)

        if dist_prev_current is not None and dist_current_next is not None:
            return "far_side" if dist_prev_current < dist_current_next else "near_side"
        return "unknown"

    stop_ctx = stop_ctx.copy()
    # pick the most common (prev_stop_id, next_stop_id) pair per stop key
    pair_cols = ["prev_stop_id", "next_stop_id"]
    pair_counts = (
        stop_ctx.groupby(keys + pair_cols, dropna=False)
        .size()
        .reset_index(name="pair_count")
    )
    idx = pair_counts.groupby(keys)["pair_count"].idxmax()
    canonical_pairs = pair_counts.loc[idx, keys + pair_cols]

    canonical = stop_ctx.merge(canonical_pairs, on=keys + pair_cols, how="inner")
    canonical = canonical.drop_duplicates(subset=keys)
    canonical["stop_side"] = canonical.apply(_calc_side, axis=1)

    return canonical[keys + ["stop_side"]]

def buses_by_stop_route_dir_within_radius(
    lon: float,
    lat: float,
    start_time: str,       # "HH:MM" or "HH:MM:SS"
    end_time: str,         # "HH:MM" or "HH:MM:SS"
    day_type: str,         # "Weekday" | "Saturday" | "Sunday"
    radius_ft: int = 250,
    selected_feeds: list[str] | None = None, 
    con: duckdb.DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """
    Returns one row per (route_id, direction_id, stop_id) within radius,
    with stop name + lat/lon and count of buses in the inclusive time window.
    Handles midnight-spanning windows (e.g., 23:30–00:30).
    """

    # project the query point to EPSG:2263 (NY state plane feet)
    x0, y0 = Transformer.from_crs("EPSG:4326", "EPSG:2263", always_xy=True).transform(lon, lat)
    s, e = to_sec(start_time), to_sec(end_time)

    # define placeholders for feeds selection 
    sel = list(selected_feeds or [])
    if sel:
        values = ",".join(["(?)"] * len(sel))           # -> "(?),(?),(?)"
        chosen_cte = f"chosen_feeds(feed_id) AS (VALUES {values}),"
        feed_pred = "feed_id IN (SELECT feed_id FROM chosen_feeds)"
    else:
        chosen_cte = ""                                  # no CTE
        feed_pred = "TRUE"                               # no filter = all feeds

    # Helper to choose parquet path: prefer combined file when using http or when
    # a single-file exists; otherwise read all parquet files inside a folder.
    def _parquet_path(table_name: str) -> str:
        base = PARQ_BASE.rstrip('/')
        if base.startswith("http"):
            return f"{base}/{table_name}.parquet"
        p = Path(base)
        single = p / f"{table_name}.parquet"
        folder = p / table_name
        if single.exists():
            return single.as_posix()
        # if a folder with parquet files exists, use a wildcard to read all
        if folder.exists() and any(folder.glob("*.parquet")):
            return (folder / "*.parquet").as_posix()
        # fallback to single-file path (DuckDB will error if missing)
        return single.as_posix()

    # resolve parquet paths (single file or folder wildcard)
    p_dim_stops = _parquet_path('dim_stops')
    p_dim_trips = _parquet_path('dim_trips')
    p_dim_routes = _parquet_path('dim_routes')
    p_calendar_base = _parquet_path('calendar_base')
    p_fact_stop_events = _parquet_path('fact_stop_events')

    sql = f"""
    WITH
    {chosen_cte}
    dim_stops AS (SELECT * FROM read_parquet('{p_dim_stops}')),
    dim_trips  AS (SELECT * FROM read_parquet('{p_dim_trips}')),
    dim_routes AS (SELECT * FROM read_parquet('{p_dim_routes}')),
    calendar_base AS (SELECT * FROM read_parquet('{p_calendar_base}')),
    fact_stop_events AS (SELECT * FROM read_parquet('{p_fact_stop_events}')),
    svcs AS (
      SELECT DISTINCT feed_id, service_id
      FROM calendar_base
      WHERE {feed_pred}
      AND (
        (? = 'Weekday'  AND (monday=1 OR tuesday=1 OR wednesday=1 OR thursday=1 OR friday=1))
        OR (? = 'Saturday' AND saturday=1)
        OR (? = 'Sunday'   AND sunday=1)
        )
    ),
    win AS (SELECT ?::INTEGER AS s, ?::INTEGER AS e),
    near_stops AS (
      SELECT feed_id, stop_id, stop_name, lat, lon
      FROM dim_stops
      WHERE {feed_pred}
      AND ((x2263 - ?)*(x2263 - ?) + (y2263 - ?)*(y2263 - ?)) <= ?*?
    )
    SELECT
      r.feed_id,
      r.route_id,
      t.trip_headsign,
      t.direction_id,
      f.service_id,
      s.stop_id,
      s.stop_name,
      s.lat  AS stop_lat,
      s.lon  AS stop_lon,
      COUNT(*) AS buses_scheduled
    FROM fact_stop_events f
    JOIN dim_trips  t ON f.feed_id = t.feed_id AND f.trip_id = t.trip_id
    JOIN dim_routes r ON t.feed_id = r.feed_id AND t.route_id = r.route_id
    JOIN svcs       v ON f.feed_id = v.feed_id AND f.service_id = v.service_id
    JOIN near_stops s ON f.feed_id = s.feed_id AND f.stop_id   = s.stop_id
    CROSS JOIN win
    WHERE
      (
        (SELECT e FROM win) >= (SELECT s FROM win)
        AND f.arrival_sec BETWEEN (SELECT s FROM win) AND (SELECT e FROM win)
      )
      OR
      (
        (SELECT e FROM win) < (SELECT s FROM win)   -- midnight wrap
        AND (f.arrival_sec >= (SELECT s FROM win) OR f.arrival_sec <= (SELECT e FROM win))
      )
    GROUP BY r.feed_id, r.route_id, t.direction_id, t.trip_headsign, s.stop_id, s.stop_name, s.lat, s.lon, f.service_id
    ORDER BY s.stop_name, r.feed_id, r.route_id, t.direction_id, f.service_id;
    """

    params = []
    # 1) If feeds are selected, add one param per "(?)" in chosen_feeds CTE
    if sel:
        params += sel

    # 2) Always add the rest: day_type, time window, spatial params
    params += [day_type, day_type, day_type]                # 3 day-type placeholders
    params += [s, e]                                        # window
    params += [x0, x0, y0, y0, int(radius_ft), int(radius_ft)]  # spatial
    df = con.execute(sql, params).fetchdf()

    if df.empty:
        return df

    stop_ctx_sql = f"""
    WITH
    {chosen_cte}
    dim_stops AS (SELECT * FROM read_parquet('{p_dim_stops}')),
    calendar_base AS (SELECT * FROM read_parquet('{p_calendar_base}')),
    fact_stop_events AS (SELECT * FROM read_parquet('{p_fact_stop_events}')),
    svcs AS (
      SELECT DISTINCT feed_id, service_id
      FROM calendar_base
      WHERE {feed_pred}
      AND (
        (? = 'Weekday'  AND (monday=1 OR tuesday=1 OR wednesday=1 OR thursday=1 OR friday=1))
        OR (? = 'Saturday' AND saturday=1)
        OR (? = 'Sunday'   AND sunday=1)
        )
    ),
    win AS (SELECT ?::INTEGER AS s, ?::INTEGER AS e),
    near_stops AS (
      SELECT feed_id, stop_id
      FROM dim_stops
      WHERE {feed_pred}
      AND ((x2263 - ?)*(x2263 - ?) + (y2263 - ?)*(y2263 - ?)) <= ?*?
    ),
    trips_in_window AS (
      SELECT DISTINCT f.feed_id, f.trip_id
      FROM fact_stop_events f
      JOIN svcs v ON f.feed_id = v.feed_id AND f.service_id = v.service_id
      CROSS JOIN win
      WHERE {feed_pred}
      AND (
        (SELECT e FROM win) >= (SELECT s FROM win)
        AND f.arrival_sec BETWEEN (SELECT s FROM win) AND (SELECT e FROM win)
        OR
        (SELECT e FROM win) < (SELECT s FROM win)
        AND (f.arrival_sec >= (SELECT s FROM win) OR f.arrival_sec <= (SELECT e FROM win))
      )
    ),
    events AS (
      SELECT
        f.feed_id, f.route_id, f.direction_id, f.service_id, f.trip_id, f.stop_id, f.stop_sequence,
        LAG(f.stop_id)  OVER (PARTITION BY f.feed_id, f.trip_id ORDER BY f.stop_sequence) AS prev_stop_id,
        LEAD(f.stop_id) OVER (PARTITION BY f.feed_id, f.trip_id ORDER BY f.stop_sequence) AS next_stop_id
      FROM fact_stop_events f
      JOIN trips_in_window t ON f.feed_id = t.feed_id AND f.trip_id = t.trip_id
      WHERE {feed_pred}
    )
    SELECT
      e.feed_id, e.route_id, e.direction_id, e.service_id, e.stop_id,
      e.prev_stop_id, e.next_stop_id,
      s.lat AS stop_lat, s.lon AS stop_lon,
      p.lat AS prev_lat, p.lon AS prev_lon,
      n.lat AS next_lat, n.lon AS next_lon
    FROM events e
    JOIN dim_stops s ON e.feed_id = s.feed_id AND e.stop_id = s.stop_id
    LEFT JOIN dim_stops p ON e.feed_id = p.feed_id AND e.prev_stop_id = p.stop_id
    LEFT JOIN dim_stops n ON e.feed_id = n.feed_id AND e.next_stop_id = n.stop_id
    JOIN near_stops ns ON e.feed_id = ns.feed_id AND e.stop_id = ns.stop_id;
    """

    stop_ctx = con.execute(stop_ctx_sql, params).fetchdf()
    if not stop_ctx.empty:
        stop_side = _stop_side_by_segment_distance(stop_ctx, lat, lon)
        df = df.merge(
            stop_side,
            on=["feed_id", "route_id", "direction_id", "service_id", "stop_id"],
            how="left",
        )
    else:
        df["stop_side"] = "unknown"

    return df

# ---------- Streamlit UI ----------
st.set_page_config(page_title="Bus Counter", layout="wide")
st.title("Bus Counter — stops within radius by route & direction")

if "result_df" not in st.session_state:
    st.session_state["result_df"] = None
if "sites" not in st.session_state:
    st.session_state["sites"] = []   # list of dicts: {name, lat, lon, radius_ft}

con = get_con()
# try:
#     st.write("PARQ_BASE →", PARQ_BASE)
#     st.write(con.execute(f"SELECT COUNT(*) n FROM read_parquet('{parquet_path('dim_routes')}')").fetchdf())
# except Exception as e:
#     st.error(f'Parquet not rechable: {e}')

# ---- set overall parameters
col0, col1, col2, col3,  = st.columns([1,1,1,1])
with col0:
    day_type = st.selectbox("Day type", ["Weekday", "Saturday", "Sunday"], index=0)
    school_choice = "All"
    if day_type == "Weekday":
        school_choice = st.radio(
            "School day filter",
            ["All", "School day only (SDon)", "Non-school day only"],
            index=0,
            help="Filters Weekday trips using service_id that contains 'SDon'."
        )
with col1:
    t_start = st.time_input("Start time", value=time(7,45))
    t_end   = st.time_input("End time", value=time(8,45))  
with col2:
    radius_ft = st.slider("Radius (ft)", 100, 600, 250, 25)
with col3:
    # Discover feeds from Parquet
    feeds = con.execute(f"""
        SELECT DISTINCT feed_id
        FROM read_parquet('{parquet_path('dim_routes')}')
        ORDER BY feed_id
    """).fetchdf()["feed_id"].tolist()
    selected_feeds = st.multiselect("Filter Feeds (all selected by default)", options=feeds, default=feeds)  # default = all

# ----- click multiple sites
st.markdown("**1) Click on the map** to select intersection. **2) Update Site Label** **3) Click Add Site** 4) When done, **Press ‘Run query’.**")
colA, colB, colC= st.columns([1,1,1])
with colA:
    # Keep the last clicked point in session state
    if "clicked_lat" not in st.session_state:
        st.session_state.clicked_lat = 40.7580   # Midtown default
    if "clicked_lon" not in st.session_state:
        st.session_state.clicked_lon = -73.9855

    # --- Clickable map (Leaflet) ---
    radius_m = radius_ft * 0.3048  # for drawing the circle on a web map (meters)

    # Build the map centered on the last clicked point
    m = folium.Map(
        location=[st.session_state.clicked_lat, st.session_state.clicked_lon],
        zoom_start=15,
        control_scale=True,
        tiles="CartoDB positron",
    )
    Geocoder(collapsed=True, add_marker=True, position="topleft").add_to(m) # add search to map
    # add previously clicked points in red
    for s in st.session_state["sites"]:
        folium.Marker([s["lat"], s["lon"]], tooltip=s["name"], icon=folium.Icon(color="red")).add_to(m)
        folium.Circle(radius=s["radius_ft"]*0.3048, location=[s["lat"], s["lon"]],
                    color="red", weight=1, fill=False).add_to(m)

    # Show current/new selection
    folium.Marker(
        [st.session_state.clicked_lat, st.session_state.clicked_lon],
        tooltip="Selected point",
        icon=folium.Icon(color="blue"),
    ).add_to(m)
    folium.Circle(
        radius=radius_m, location=[st.session_state.clicked_lat, st.session_state.clicked_lon],
        color="#3388ff", weight=2, fill=True, fill_opacity=0.05,
    ).add_to(m)

    # Render the map and capture clicks
    out = st_folium(m, height=500, width=None, key="clickmap", returned_objects=["last_clicked"])
    if out and out.get("last_clicked"):
        st.session_state.clicked_lat = out["last_clicked"]["lat"]
        st.session_state.clicked_lon = out["last_clicked"]["lng"]
        st.rerun()

with colB:
    site_name = st.text_input("Site label", value=f"Site {len(st.session_state['sites'])+1}")
    if st.button("Add site"):
        click = out.get("last_clicked") if out else None
        lat = click["lat"] if click else st.session_state.clicked_lat
        lon = click["lng"] if click else st.session_state.clicked_lon
        st.session_state["sites"].append({
            "name": site_name,
            "lat": float(lat),
            "lon": float(lon),
            "radius_ft": int(radius_ft),
        })
with colC:
    # ---- list selected points
    st.write("**Selected sites**")

    sites = st.session_state.get("sites", [])
    sites_df = pd.DataFrame(sites, columns=["name","lat","lon","radius_ft"])

    if sites_df.empty:
        st.info("No sites added yet. Click the map, set a label, and press **Add site**.")
    else:
        # add a delete checkbox column for interactive removal
        if "delete" not in sites_df.columns:
            sites_df["delete"] = False

        edited = st.data_editor(
            sites_df,
            hide_index=True,
            column_config={
                "name": "Site",
                # the column_config lines below are optional; remove if your Streamlit is older
                "lat": st.column_config.NumberColumn("Lat", format="%.6f", disabled=True),
                "lon": st.column_config.NumberColumn("Lon", format="%.6f", disabled=True),
                "radius_ft": st.column_config.NumberColumn("Radius (ft)", disabled=True),
                "delete": st.column_config.CheckboxColumn("Delete?"),
            },
            key="sites_editor",
        )

        col_del, col_clear = st.columns(2)
        with col_del:
            if st.button("Delete selected"):
                keep = edited[~edited["delete"]].drop(columns=["delete"], errors="ignore")
                st.session_state["sites"] = keep.to_dict(orient="records")
                st.rerun()  # if this errors on older Streamlit, use st.experimental_rerun()
        with col_clear:
            if st.button("Clear sites all"):
                st.session_state["sites"] = []
                st.rerun()

if st.button("Run query"):
    frames = []
    sites = st.session_state["sites"] or [{
        "name": "Site 1",
        "lat": st.session_state.clicked_lat,
        "lon": st.session_state.clicked_lon,
        "radius_ft": radius_ft,
    }]
    for s in sites:
        df_site = buses_by_stop_route_dir_within_radius(
            lon=s["lon"], lat=s["lat"],
            start_time=f"{t_start.hour:02d}:{t_start.minute:02d}:{t_start.second:02d}",
            end_time=f"{t_end.hour:02d}:{t_end.minute:02d}:{t_end.second:02d}",
            day_type=day_type,
            radius_ft=s["radius_ft"],
            selected_feeds=selected_feeds,
            con=con,
        )
        df_site.insert(0, "Intersection", s["name"])  # tag rows by site
        frames.append(df_site)
    st.session_state["result_df"] = pd.concat(frames, ignore_index=True) if frames else None

df = st.session_state["result_df"]
if df is not None:
    st.subheader("Results")
    if df.empty:
        st.warning("No scheduled buses in that window for stops within the radius.")
    else:
        # Show totals and table
        stops_total = df["stop_id"].nunique()
        buses_total = int(df["buses_scheduled"].sum())
        st.write(f"**Stops found:** {stops_total}  |  **Total buses (sum of rows):** {buses_total}")
        st.dataframe(df, use_container_width=True)

        # Download
        st.download_button(
            "Download CSV",
            df.to_csv(index=False),
            file_name="bus_counts_by_stop_route_direction.csv",
            mime="text/csv"
        )
        
        if st.button("Clear results"):
            st.session_state["result_df"] = None

        # draw result stops on a separate map
        st.markdown("**Stops within radius (with total buses in window):**")
        # aggregate to one marker per stop (sum across routes/directions)
        stops_markers = (df.groupby(['Intersection',"stop_id","stop_name","stop_lat","stop_lon"], as_index=False)
                           ["buses_scheduled"].sum()
                        )
        m2 = folium.Map(
            location=[st.session_state.clicked_lat, st.session_state.clicked_lon],
            zoom_start=15, control_scale=True, tiles="CartoDB positron",
        )
        folium.Circle(
            radius=radius_m, location=[st.session_state.clicked_lat, st.session_state.clicked_lon],
            color="#3388ff", weight=2, fill=True, fill_opacity=0.05,
        ).add_to(m2)
        for _, row in stops_markers.iterrows():
            folium.Marker(
                [row.stop_lat, row.stop_lon],
                popup=folium.Popup(
                    f"<b>{row.stop_name}</b><br/>Stop ID: {row.stop_id}<br/>Buses in window: {int(row.buses_scheduled)}",
                    max_width=250
                ),
                tooltip=f"{row.Intersection}: {row.stop_name} ({row.stop_id})", 
                icon=folium.Icon(color="green")
            ).add_to(m2)

        st_folium(m2, height=500, width=None, key="resultmap")
