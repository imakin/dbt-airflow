import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Configuration
DB_PATH = "data/dwib.duckdb"
DB_SCHEMA = "analytics_analytics"
TABLE_AGG_DAILY_STATS = "agg_daily_stats"
TABLE_FCT_TRIPS = "fct_trips"
TABLE_DIM_ZONES = "dim_zones"

# Page config
st.set_page_config(
    page_title="NYC Taxi Analytics Dashboard",
    layout="wide"
)

# Title
st.title("UAS DWIB")
st.markdown("**3.3 Visualisasi Data NYC Taxi**")

# semua load dengan caching dan semua langsung buka/tutup koneksi,
# agar tidak mengunci db
@st.cache_data
def load_kpi_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = f"""
    SELECT 
        SUM(total_trips) as total_trips,
        SUM(total_revenue) as total_revenue,
        AVG(avg_revenue_per_trip) as avg_fare
    FROM {DB_SCHEMA}.{TABLE_AGG_DAILY_STATS}
    """
    result = conn.execute(query).fetchone()
    conn.close()
    return result

@st.cache_data
def load_trend_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = f"""
    SELECT trip_date, total_trips, total_revenue
    FROM {DB_SCHEMA}.{TABLE_AGG_DAILY_STATS}
    WHERE trip_date >= '2023-01-01'
    ORDER BY trip_date
    """
    result = conn.execute(query).fetchdf()
    conn.close()
    return result

@st.cache_data
def load_top_zones():
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = f"""
    SELECT 
        z.zone_name,
        z.borough,
        COUNT(*) as total_trips,
        SUM(f.total_amount) as total_revenue
    FROM {DB_SCHEMA}.{TABLE_FCT_TRIPS} f
    JOIN {DB_SCHEMA}.{TABLE_DIM_ZONES} z ON f.pickup_location_id = z.location_id
    GROUP BY z.zone_name, z.borough
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    result = conn.execute(query).fetchdf()
    conn.close()
    return result

@st.cache_data
def load_heatmap_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = f"""
    SELECT 
        f.pickup_hour,
        z.borough,
        COUNT(*) as trip_count
    FROM {DB_SCHEMA}.{TABLE_FCT_TRIPS} f
    JOIN {DB_SCHEMA}.{TABLE_DIM_ZONES} z ON f.pickup_location_id = z.location_id
    WHERE z.borough != 'EWR'
    GROUP BY f.pickup_hour, z.borough
    ORDER BY f.pickup_hour, z.borough
    """
    result = conn.execute(query).fetchdf()
    conn.close()
    return result

@st.cache_data
def load_payment_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    query = f"""
    SELECT 
        CASE payment_type 
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            ELSE 'Unknown'
        END as payment_method,
        COUNT(*) as trip_count
    FROM {DB_SCHEMA}.{TABLE_FCT_TRIPS}
    GROUP BY payment_type
    ORDER BY trip_count DESC
    """
    result = conn.execute(query).fetchdf()
    conn.close()
    return result

kpi_data = load_kpi_data()
trend_df = load_trend_data()
top_zones_df = load_top_zones()
heatmap_df = load_heatmap_data()
payment_df = load_payment_data()

# 1. KPI Summary
st.header("KPI Summary")
total_trips, total_revenue, avg_fare = kpi_data

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Trips", f"{total_trips:,.0f}")
with col2:
    st.metric("Total Revenue", f"${total_revenue:,.2f}")
with col3:
    st.metric("Average Fare", f"${avg_fare:.2f}")

st.markdown("---")

# 2. Trend Analysis
st.header("Trend Analysis")

col1, col2 = st.columns(2)

with col1:
    fig_trips = px.line(
        trend_df, 
        x='trip_date', 
        y='total_trips',
        title='Daily Trips Trend',
        labels={'trip_date': 'Date', 'total_trips': 'Total Trips'}
    )
    fig_trips.update_traces(line_color='#1f77b4')
    st.plotly_chart(fig_trips, use_container_width=True)

with col2:
    fig_revenue = px.line(
        trend_df, 
        x='trip_date', 
        y='total_revenue',
        title='Daily Revenue Trend',
        labels={'trip_date': 'Date', 'total_revenue': 'Total Revenue ($)'}
    )
    fig_revenue.update_traces(line_color='#2ca02c')
    st.plotly_chart(fig_revenue, use_container_width=True)

st.markdown("---")

# 3. Top Zones
st.header("Top 10 Zones by Revenue")

fig_zones = px.bar(
    top_zones_df,
    x='total_revenue',
    y='zone_name',
    color='borough',
    orientation='h',
    title='Top 10 Pickup Zones by Revenue',
    labels={'total_revenue': 'Total Revenue ($)', 'zone_name': 'Zone'},
    text='total_revenue'
)
fig_zones.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
fig_zones.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig_zones, use_container_width=True)

st.markdown("---")

# 4. Demand Heatmap
st.header("Demand Heatmap - Trips by Hour and Borough")

# Pivot for heatmap
heatmap_pivot = heatmap_df.pivot(index='borough', columns='pickup_hour', values='trip_count')

fig_heatmap = px.imshow(
    heatmap_pivot,
    labels=dict(x="Hour of Day", y="Borough", color="Trip Count"),
    x=heatmap_pivot.columns,
    y=heatmap_pivot.index,
    color_continuous_scale='YlOrRd',
    title='Trip Demand by Hour and Borough'
)
fig_heatmap.update_layout(height=400)
st.plotly_chart(fig_heatmap, use_container_width=True)

st.markdown("---")

# 5. Payment Breakdown
st.header("Payment Method Breakdown")
col1, col2 = st.columns([2, 1])

with col1:
    fig_payment = px.pie(
        payment_df,
        values='trip_count',
        names='payment_method',
        title='Payment Method Distribution',
        hole=0.4
    )
    fig_payment.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_payment, use_container_width=True)

with col2:
    st.dataframe(
        payment_df.style.format({'trip_count': '{:,.0f}'}),
        hide_index=True,
        use_container_width=True
    )
