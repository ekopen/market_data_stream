# dashboard.py
# creating a streamlit dashboard to visualize the data
# streamlit run dashboard.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import os
from datetime import datetime, timedelta, timezone

from storage_hot import get_client
from storage_warm import conn

from config import HOT_DURATION, WARM_DURATION, COLD_DURATION

refresh_counter = st_autorefresh(interval=1000, key="refresh_counter")

st.title("ETH Price Dashboard")

def load_hot_data(HOT_DURATION):
    cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=HOT_DURATION)
    cutoff_ms = int(cutoff_time.timestamp() * 1000)

    ch_client = get_client()
    df = ch_client.query_df(f'''
        SELECT * FROM price_ticks
        WHERE timestamp_ms >= {cutoff_ms}
        ORDER BY timestamp DESC
    ''')
    return df.sort_values("timestamp")

def load_warm_data(WARM_DURATION, conn):
    cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=WARM_DURATION)
    cutoff_ms = int(cutoff_time.timestamp() * 1000)

    query = '''
        SELECT * FROM price_ticks
        WHERE timestamp_ms >= %s
        ORDER BY timestamp DESC
    '''
    df = pd.read_sql(query, conn, params=(cutoff_ms,))
    return df.sort_values("timestamp")

def load_cold_data(COLD_DURATION):
    cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=COLD_DURATION)
    cutoff_ms = int(cutoff_time.timestamp() * 1000)

    df = pd.DataFrame()

    for filename in os.listdir('cold_data'):
        file_ms = int(filename.split('.')[0]) #get the cutoff time the file
        if file_ms >= cutoff_ms:
            file_path = os.path.join('cold_data', filename)
            temp_df = pd.read_parquet(file_path)
            df = pd.concat([df, temp_df], ignore_index=True)

    return df.sort_values("timestamp")

def plot_price(df, title, height=350):

    if df.empty or 'timestamp' not in df.columns or 'price' not in df.columns:
        st.warning(f"No data available for {title}")
        return go.Figure().update_layout(title=title, height=height)

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

# --- Load & Display Hot Data (every second)
st.subheader("Hot Data")
hot_df = load_hot_data(HOT_DURATION)
st.plotly_chart(plot_price(hot_df, "Hot data (up to five minutes old)"), use_container_width=True)

# --- Load & Display Warm Data every WARM_DURATION seconds
if refresh_counter % WARM_DURATION == 0:
    st.session_state["warm_df"] = load_warm_data(WARM_DURATION, conn)
warm_df = st.session_state.get("warm_df", pd.DataFrame())
st.subheader("Warm Data")
st.plotly_chart(plot_price(warm_df, "Warm data (up to 30 minutes old)"), use_container_width=True)

# --- Load & Display Cold Data every COLD_DURATION seconds
if refresh_counter % COLD_DURATION == 0:
    st.session_state["cold_df"] = load_cold_data(COLD_DURATION)
cold_df = st.session_state.get("cold_df", pd.DataFrame())
st.subheader("Cold Data")
st.plotly_chart(plot_price(cold_df, "Cold data (up to 24 hours old)"), use_container_width=True)