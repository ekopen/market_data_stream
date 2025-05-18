# dashboard.py
# creating a streamlit dashboard to visualize the data
# streamlit run dashboard.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from storage_hot import get_client
from streamlit_autorefresh import st_autorefresh
from datetime import timedelta, datetime, timezone

st.title("ETH Price Dashboard")
st_autorefresh(interval=1000, key="refresh_live_data") # refresh live data

def plot_price(df, title, height=350):
    df = df.sort_values("timestamp")

    min_price = df['price'].min()
    max_price = df['price'].max()
    y_range = [min_price * 0.9985, max_price * 1.0015]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['price'],
        mode='lines',
        line=dict(width=2),
        name="Price"
    ))
    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Price",
        height=height,
        margin=dict(l=40, r=40, t=40, b=40),
        yaxis=dict(tickformat=".2f", range=y_range)
    )
    return fig

# Load data
cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=60)
cutoff_ms = int(cutoff_time.timestamp() * 1000)

def load_hot_data():
    ch_client = get_client()
    df = ch_client.query_df(f'''
        SELECT * FROM price_ticks
        WHERE timestamp_ms >= {cutoff_ms}
        ORDER BY timestamp DESC
    ''')
    return df.sort_values("timestamp")

def load_warm_data():
    return pd.read_parquet("data/warm_data.parquet")
def load_cold_data():
    return pd.read_parquet("data/cold_data.parquet")

# --- Display sections
st.subheader("Hot Data")
hot_df = load_hot_data()
hot_fig = plot_price(hot_df, "Last Minute", height=300)
st.plotly_chart(hot_fig, use_container_width=True)

st.subheader("Warm Data")
warm_df = load_warm_data()
warm_fig = plot_price(warm_df, "Last 5 Minutes", height=300)
st.plotly_chart(warm_fig, use_container_width=True)

st.subheader("Cold Data")
cold_df = load_cold_data()
cold_fig = plot_price(cold_df, "Last 5 Hours", height=300)
st.plotly_chart(cold_fig, use_container_width=True)
