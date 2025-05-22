# dashboard.py
# creating a streamlit dashboard to visualize the data
# streamlit run dashboard.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import os
from datetime import datetime, timedelta, timezone
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

from storage_hot import get_client
from storage_warm import conn

def reduceTickFreq(df, increment):
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.set_index("timestamp").resample(increment).agg({
    "price": "mean",
    "volume": "sum"
        }).dropna().reset_index()
    return df

def load_hot_data():
    ch_client = get_client()
    df = ch_client.query_df(f'''
        SELECT * FROM price_ticks
        ORDER BY timestamp DESC
    ''')

    df_display = reduceTickFreq(df,"1s")
    return df, df_display.sort_values("timestamp")

def load_warm_data(conn):
    query = '''
        SELECT * FROM price_ticks
        ORDER BY timestamp DESC
    '''
    df = pd.read_sql(query, conn)
    df_display = reduceTickFreq(df,"5s")
    return df, df_display.sort_values("timestamp")

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

st.title("ETH Price Dashboard")

refresh_counter = st_autorefresh(interval=1000, key="refresh_counter")

hot_df, hot_df_display = load_hot_data()
warm_df, warm_df_display = load_warm_data(conn)

tab1, tab2 = st.tabs(["Hot", "Warm"])

with tab1:
    st.subheader("Hot Data")
    st.write(f"Rows: {len(hot_df):,} | Memory: {hot_df.memory_usage(deep=True).sum() / 1_048_576:.2f} MB")
    st.plotly_chart(plot_price(hot_df_display, "Hot Data"), use_container_width=True)
with tab2:
    st.subheader("Warm Data")
    st.write(f"Rows: {len(warm_df):,} | Memory: {warm_df.memory_usage(deep=True).sum() / 1_048_576:.2f} MB")
    st.plotly_chart(plot_price(warm_df_display, "Warm Data"), use_container_width=True)
