# app.py — Python-only dashboard + reusable function
import duckdb
import pandas as pd
import streamlit as st
from datetime import time
from pyproj import Transformer

import folium
from streamlit_folium import st_folium

DB_PATH = "mta_gtfs.duckdb"

# ---------- helpers ----------
def to_sec(hms: str) -> int:
    hh, mm, *rest = hms.split(":")
    ss = int(rest[0]) if rest else 0
    return int(hh) * 3600 + int(mm) * 60 + ss

@st.cache_resource
def get_con():
    # read-only for analysis; change to read_only=False if you need to modify schema
    return duckdb.connect(DB_PATH, read_only=True)

def buses_by_stop_route_dir_within_radius(
    lon: float,
    lat: float,
    start_time: str,       # "HH:MM" or "HH:MM:SS"
    end_time: str,         # "HH:MM" or "HH:MM:SS"
    day_type: str,         # "Weekday" | "Saturday" | "Sunday"
    radius_ft: int = 250,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """
    Returns one row per (route_id, direction_id, stop_id) within radius,
    with stop name + lat/lon and count of buses in the inclusive time window.
    Handles midnight-spanning windows (e.g., 23:30–00:30).
    """
    close_after = False
    if con is None:
        con = duckdb.connect(DB_PATH, read_only=True)
        close_after = True

    # project the query point to EPSG:2263 (NY state plane feet)
    x0, y0 = Transformer.from_crs("EPSG:4326", "EPSG:2263", always_xy=True).transform(lon, lat)
    s, e = to_sec(start_time), to_sec(end_time)

    sql = """
    WITH svcs AS (
      SELECT DISTINCT service_id
      FROM calendar_base
      WHERE
        (? = 'Weekday'  AND (monday=1 OR tuesday=1 OR wednesday=1 OR thursday=1 OR friday=1))
        OR (? = 'Saturday' AND saturday=1)
        OR (? = 'Sunday'   AND sunday=1)
    ),
    win AS (SELECT ?::INTEGER AS s, ?::INTEGER AS e),
    near_stops AS (
      SELECT stop_id, stop_name, stop_lat, stop_lon
      FROM dim_stops
      WHERE (x2263 - ?)*(x2263 - ?) + (y2263 - ?)*(y2263 - ?) <= ?*?
    )
    SELECT
      r.route_id,
      t.direction_id,
      s.stop_id,
      s.stop_name,
      s.stop_lat  AS stop_lat,
      s.stop_lon  AS stop_lon,
      COUNT(*) AS buses_scheduled
    FROM fact_stop_events f
    JOIN dim_trips  t ON f.trip_id = t.trip_id
    JOIN dim_routes r ON t.route_id = r.route_id
    JOIN svcs       v ON v.service_id = f.service_id
    JOIN near_stops s ON s.stop_id   = f.stop_id
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
    GROUP BY r.route_id, t.direction_id, s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
    ORDER BY s.stop_name, r.route_id, t.direction_id;
    """

    params = [
        day_type, day_type, day_type,
        s, e,
        x0, x0, y0, y0, radius_ft, radius_ft,
    ]
    df = con.execute(sql, params).fetchdf()

    if close_after:
        con.close()
    return df

# ---------- Streamlit UI ----------
st.set_page_config(page_title="MTA Bus Counter", layout="wide")
st.title("MTA Bus Counter — stops within radius by route & direction")

if "result_df" not in st.session_state:
    st.session_state["result_df"] = None
if "sites" not in st.session_state:
    st.session_state["sites"] = []   # list of dicts: {name, lat, lon, radius_ft}

con = get_con()

# # Quick schema sanity check (runs once, lightweight)
# with st.expander("Database status", expanded=False):
#     st.dataframe(con.execute("SHOW ALL TABLES").fetchdf(), width='stretch')

# ---- set overall parameters
col0, col1, col2, col3 = st.columns([1,1,1,1])
with col0:
    day_type = st.selectbox("Day type", ["Weekday", "Saturday", "Sunday"], index=0)
with col1:
    t_start = st.time_input("Start time", value=time(7,45))
    t_end   = st.time_input("End time", value=time(8,45))
# with col2:
    
with col3:
    radius_ft = st.slider("Radius (ft)", 100, 600, 250, 25)

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
            "lat": st.session_state.clicked_lat,
            "lon": st.session_state.clicked_lon,
            "radius_ft": radius_ft,
        })
with colC:
    # ---- list selected points
    import pandas as pd
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
            con=con,
        )
        df_site.insert(0, "site", s["name"])  # tag rows by site
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
        st.dataframe(df, width='stretch')

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
        stops_markers = (df.groupby(['site',"stop_id","stop_name","stop_lat","stop_lon"], as_index=False)
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
                tooltip=f"{row.site}: {row.stop_name} ({row.stop_id})", 
                icon=folium.Icon(color="green")
            ).add_to(m2)

        st_folium(m2, height=500, width=None, key="resultmap")
