# dashboard.py
# creating a streamlit dashboard to visualize the data
# streamlit run dashboard.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import os
from datetime import datetime, timedelta, timezone

from storage_hot import get_client as get_client_hot
from storage_warm import get_client as get_client_warm

from config import HOT_DURATION, WARM_DURATION, COLD_DURATION

refresh_counter = st_autorefresh(interval=1000, key="refresh_counter")

st.title("ETH Price Dashboard")

def load_hot_data(HOT_DURATION):
    cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=HOT_DURATION)
    cutoff_ms = int(cutoff_time.timestamp() * 1000)

    ch_client_hot = get_client_hot()
    df = ch_client_hot.query_df(f'''
        SELECT * FROM price_ticks_hot
        WHERE timestamp_ms >= {cutoff_ms}
        ORDER BY timestamp DESC
    ''')
    return df.sort_values("timestamp")

def load_warm_data(WARM_DURATION):
    cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=WARM_DURATION)
    cutoff_ms = int(cutoff_time.timestamp() * 1000)

    ch_client_warm = get_client_warm()
    df = ch_client_warm.query_df(f'''
        SELECT * FROM price_ticks_warm
        WHERE timestamp_ms >= {cutoff_ms}
        ORDER BY timestamp DESC
    ''')
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
    y_price_range = [min_price * 0.9999, max_price * 1.0001]

    fig = go.Figure()

    # Price Line
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['price'],
        mode='lines',
        line=dict(width=2),
        name="Price"
    ))

    # Volume Bar
    fig.add_trace(go.Bar(
        x=df['timestamp'],
        y=df['volume'],
        name='Volume',
        opacity=0.4,
        yaxis='y2'
    ))

    fig.update_layout(
        title=title,
        height=height,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis=dict(title='Time'),
        yaxis=dict(
            title='Price',
            tickformat=".2f",
            range=y_price_range,
            side='left'
        ),
        yaxis2=dict(
            title='Volume',
            overlaying='y',
            side='right',
            showgrid=False
        ),
        legend=dict(x=0, y=1.1, orientation='h')
    )

    return fig

# Load & Display Hot Data (every second)
st.subheader("Hot Data")
hot_df = load_hot_data(HOT_DURATION)
st.plotly_chart(plot_price(hot_df, f"Hot data (up to {HOT_DURATION/60} minutes old)"), use_container_width=True)

# Load & Display Warm Data every WARM_DURATION seconds
if refresh_counter % WARM_DURATION == 0:
    st.session_state["warm_df"] = load_warm_data(WARM_DURATION)
warm_df = st.session_state.get("warm_df", pd.DataFrame())
st.subheader("Warm Data")
st.plotly_chart(plot_price(warm_df, f"Warm data (up to {WARM_DURATION/60} minutes old)"), use_container_width=True)

# Load & Display Cold Data every COLD_DURATION seconds
if refresh_counter % COLD_DURATION == 0:
    st.session_state["cold_df"] = load_cold_data(COLD_DURATION)
cold_df = st.session_state.get("cold_df", pd.DataFrame())
st.subheader("Cold Data")
st.plotly_chart(plot_price(cold_df, f"Cold data (up to {COLD_DURATION/60/60} hours old)"), use_container_width=True)