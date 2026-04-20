import os
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Must be the first Streamlit command
st.set_page_config(page_title="BTS Airline On-Time Performance", layout="wide")

load_dotenv()

# --- BigQuery client setup ---
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

DATASET = f"{os.environ['GCP_PROJECT_ID']}.bts_dbt"

# --- Data loading functions ---
@st.cache_data(ttl=3600)
def load_delay_by_carrier() -> pd.DataFrame:
    return client.query(f"SELECT * FROM `{DATASET}.mart_delay_by_carrier`").to_dataframe()

@st.cache_data(ttl=3600)
def load_delay_root_causes() -> pd.DataFrame:
    return client.query(f"SELECT * FROM `{DATASET}.mart_delay_root_causes`").to_dataframe()

@st.cache_data(ttl=3600)
def load_airport_bottlenecks() -> pd.DataFrame:
    return client.query(f"SELECT * FROM `{DATASET}.mart_airport_bottlenecks`").to_dataframe()

@st.cache_data(ttl=3600)
def load_flights_by_carrier() -> pd.DataFrame:
    return client.query(f"SELECT * FROM `{DATASET}.mart_flights_by_carrier`").to_dataframe()

@st.cache_data(ttl=3600)
def load_route_performance() -> pd.DataFrame:
    return client.query(f"SELECT * FROM `{DATASET}.mart_route_performance`").to_dataframe()

@st.cache_data(ttl=3600)
def load_airport_detail() -> pd.DataFrame:
    return client.query(f"SELECT * FROM `{DATASET}.mart_airport_detail`").to_dataframe()

# --- Load all data ---
df_carrier   = load_delay_by_carrier()
df_roots     = load_delay_root_causes()
df_airports  = load_airport_bottlenecks()
df_flights   = load_flights_by_carrier()
df_routes    = load_route_performance()
df_ap_detail = load_airport_detail()

# --- Airline name mapping ---
airline_name_map = df_flights.set_index("reporting_airline")["airline_name"].to_dict()

# ============================================================
# HEADER
# ============================================================
st.title("✈️ BTS Airline On-Time Performance Dashboard")
st.caption(
    "Data source: [Bureau of Transportation Statistics](https://www.transtats.bts.gov/) · Last Updated Data: 12/2025"
)

st.divider()

# ============================================================
# YEAR FILTER — page-level segmented control
# ============================================================
st.subheader("Filter by Year")

year_options = ["2023", "2024", "2025", "All"]
selected_year_str = st.segmented_control(
    label="Year",
    options=year_options,
    default="2024",
    label_visibility="collapsed",
)

if selected_year_str == "All":
    df_carrier_filtered = df_carrier.copy()
else:
    df_carrier_filtered = df_carrier[df_carrier["flight_year"] == int(selected_year_str)].copy()

st.divider()

# ============================================================
# KPI CARDS
# ============================================================
st.subheader("Overview")

total_flights = int(df_carrier_filtered["total_flights"].sum())

total_delayed = (
    df_carrier_filtered["total_minor_delayed"]
    + df_carrier_filtered["total_major_delayed"]
    + df_carrier_filtered["total_severe_delayed"]
).sum()
delay_rate = round(total_delayed / total_flights * 100, 1) if total_flights else 0

cancel_rate = round(
    df_carrier_filtered["total_cancelled_flights"].sum() / total_flights * 100, 1
) if total_flights else 0

worst_code = (
    df_carrier_filtered.groupby("reporting_airline")["avg_arr_delay_mins"]
    .mean()
    .idxmax()
)
worst_airline = airline_name_map.get(worst_code, worst_code)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Flights", f"{total_flights:,}")
col2.metric("Delay Rate", f"{delay_rate}%")
col3.metric("Cancellation Rate", f"{cancel_rate}%")
col4.metric("Worst Airline (Arr. Delay)", worst_airline)

st.divider()

# ============================================================
# TILE 1 — Delay Root Cause Distribution
# X axis: airline codes, hover shows airline_name
# ============================================================
st.subheader("Delay Root Cause Distribution by Airline")
st.caption("All years (2023–2025)")

df_roots_sorted = df_roots.sort_values("total_delayed_flights", ascending=False).copy()
# Add airline_name column used only in hover tooltip
df_roots_sorted["airline_name"] = df_roots_sorted["reporting_airline"].map(airline_name_map).fillna(df_roots_sorted["reporting_airline"])

delay_cols = ["carrier_delay_pct", "weather_delay_pct", "nas_delay_pct", "security_delay_pct", "late_aircraft_delay_pct"]

fig_roots = px.bar(
    df_roots_sorted,
    x="reporting_airline",
    y=delay_cols,
    hover_data={"airline_name": True},  # show airline name on hover
    labels={"value": "Delay Share (%)", "reporting_airline": "Airline Code", "variable": "Cause", "airline_name": "Airline"},
    color_discrete_sequence=px.colors.qualitative.Set2,
)
fig_roots.update_layout(barmode="stack", legend_title="Delay Cause", height=420)
st.plotly_chart(fig_roots, use_container_width=True)

st.divider()

# ============================================================
# TILE 2 — Monthly Delay & Cancellation Trend
# ============================================================
st.subheader(f"Monthly Delay & Cancellation Trend — {selected_year_str}")

df_monthly = (
    df_carrier_filtered.groupby("flight_month")
    .agg(
        total_flights=("total_flights", "sum"),
        total_minor=("total_minor_delayed", "sum"),
        total_major=("total_major_delayed", "sum"),
        total_severe=("total_severe_delayed", "sum"),
        total_cancelled=("total_cancelled_flights", "sum"),
    )
    .reset_index()
    .sort_values("flight_month")
)

df_monthly["delay_rate_pct"] = (
    (df_monthly["total_minor"] + df_monthly["total_major"] + df_monthly["total_severe"])
    / df_monthly["total_flights"] * 100
).round(1)

df_monthly["cancel_rate_pct"] = (
    df_monthly["total_cancelled"] / df_monthly["total_flights"] * 100
).round(2)

month_names = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}
df_monthly["month_name"] = df_monthly["flight_month"].map(month_names)

left, right = st.columns(2)

with left:
    fig_delay = px.line(
        df_monthly, x="month_name", y="delay_rate_pct", markers=True,
        title=f"Monthly Delay Rate (%) — {selected_year_str}",
        labels={"month_name": "Month", "delay_rate_pct": "Delay Rate (%)"},
    )
    fig_delay.update_layout(height=380, title_font_size=18)
    st.plotly_chart(fig_delay, use_container_width=True)

with right:
    fig_cancel = px.line(
        df_monthly, x="month_name", y="cancel_rate_pct", markers=True,
        title=f"Monthly Cancellation Rate (%) — {selected_year_str}",
        labels={"month_name": "Month", "cancel_rate_pct": "Cancellation Rate (%)"},
        color_discrete_sequence=["#e45756"],
    )
    fig_cancel.update_layout(height=380, title_font_size=18)
    st.plotly_chart(fig_cancel, use_container_width=True)

st.divider()

# ============================================================
# TILE 3 — Flights by Carrier
# X axis: airline codes, hover shows airline_name, titles above each chart
# ============================================================
st.subheader("Total Flights & Diversion Rate by Airline")
st.caption("All years (2023–2025)")

df_flights_sorted = df_flights.sort_values("total_flights", ascending=False).copy()

left3, right3 = st.columns(2)

with left3:
    fig_flights = px.bar(
        df_flights_sorted,
        x="reporting_airline",
        y="total_flights",
        title="Total Flights by Airline",
        hover_data={"airline_name": True},
        labels={"reporting_airline": "Airline Code", "total_flights": "Total Flights", "airline_name": "Airline"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_flights.update_layout(height=420, title_font_size=16)
    st.plotly_chart(fig_flights, use_container_width=True)

with right3:
    fig_diverted = px.bar(
        df_flights_sorted,
        x="reporting_airline",
        y="diverted_rate_pct",
        title="Diversion Rate (%) by Airline",
        hover_data={"airline_name": True},
        labels={"reporting_airline": "Airline Code", "diverted_rate_pct": "Diversion Rate (%)", "airline_name": "Airline"},
        color_discrete_sequence=["#f58518"],
    )
    fig_diverted.update_layout(height=420, title_font_size=16)
    st.plotly_chart(fig_diverted, use_container_width=True)

st.divider()

# ============================================================
# TILE 4 — Route Performance (map coming soon)
# ============================================================
st.subheader("Route Performance")

origin_options = sorted(df_routes["origin"].unique().tolist())
selected_origin = st.selectbox("Select Origin Airport", options=origin_options)

df_routes_filtered = (
    df_routes[df_routes["origin"] == selected_origin]
    .sort_values("total_flights", ascending=False)
    .head(20)
    .reset_index(drop=True)
)

st.dataframe(
    df_routes_filtered[["origin", "dest", "total_flights", "avg_air_time_mins", "cancellation_rate_pct"]]
    .rename(columns={
        "origin": "Origin", "dest": "Destination",
        "total_flights": "Total Flights",
        "avg_air_time_mins": "Avg Air Time (min)",
        "cancellation_rate_pct": "Cancellation Rate (%)",
    }),
    use_container_width=True,
    hide_index=True,
)

st.divider()

# ============================================================
# TILE 5 — Airport Detail (US domestic airports only)
# ============================================================
st.subheader("Airport Detail")
st.caption("All years (2023–2025) · US domestic airports only")

airport_options = sorted(df_ap_detail["airport_code"].unique().tolist())
selected_airport = st.selectbox("Select Airport", options=airport_options)

ap = df_ap_detail[df_ap_detail["airport_code"] == selected_airport].iloc[0]

st.markdown(f"### {ap['airport_code']} — {ap['city_name']}")

c1, c2, c3 = st.columns(3)
c1.metric("Total Traffic", f"{int(ap['total_traffic']):,}")
c2.metric("Departures", f"{int(ap['total_departures']):,}")
c3.metric("Arrivals", f"{int(ap['total_arrivals']):,}")

c4, c5, c6 = st.columns(3)
c4.metric("Avg Taxi Out (min)", f"{ap['avg_taxi_out_mins']:.1f}")
c5.metric("Avg Taxi In (min)", f"{ap['avg_taxi_in_mins']:.1f}")
c6.metric("Total Diverted", f"{int(ap['total_diverted']):,}")

c7, c8, c9 = st.columns(3)
c7.metric("Avg Dep Delay (min)", f"{ap['avg_dep_delay_mins']:.1f}")
c8.metric("Avg Arr Delay (min)", f"{ap['avg_arr_delay_mins']:.1f}")
top_carrier_name = airline_name_map.get(ap["top_carrier"], ap["top_carrier"])
c9.metric("Top Carrier", top_carrier_name)

st.divider()

# ============================================================
# TILE 6 — All Airports Traffic & Delay Overview (original format)
# Top 10 table + horizontal bar chart side by side
# ============================================================
st.subheader("Top 10 Airports by Traffic")

df_top10 = (
    df_airports.sort_values("total_traffic", ascending=False)
    .head(10)[["airport_code", "city_name", "total_traffic", "avg_taxi_out_mins", "avg_arr_delay_mins"]]
    .reset_index(drop=True)
)

left6, right6 = st.columns(2)

with left6:
    styled = df_top10.style.background_gradient(
        subset=["avg_arr_delay_mins"], cmap="RdYlGn_r"
    ).format({
        "total_traffic": "{:,}",
        "avg_taxi_out_mins": "{:.1f}",
        "avg_arr_delay_mins": "{:.1f}",
    })
    st.dataframe(styled, use_container_width=True, hide_index=True)

with right6:
    fig_airports = px.bar(
        df_top10.sort_values("avg_arr_delay_mins"),
        x="avg_arr_delay_mins",
        y="airport_code",
        orientation="h",
        title="Avg. Arrival Delay by Airport (minutes)",
        labels={"avg_arr_delay_mins": "Avg Arrival Delay (min)", "airport_code": "Airport"},
        color="avg_arr_delay_mins",
        color_continuous_scale="RdYlGn_r",
    )
    fig_airports.update_layout(height=380, coloraxis_showscale=False)
    st.plotly_chart(fig_airports, use_container_width=True)