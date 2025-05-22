# dashboard.py
# creating a streamlit dashboard to visualize the data
# streamlit run dashboard.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
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

def load_consumer_metrics():
    query = '''
        SELECT * FROM consumer_metrics
        ORDER BY timestamp DESC
        LIMIT 100
    '''
    return pd.read_sql(query, conn)

def load_producer_metrics():
    query = '''
        SELECT * FROM producer_metrics
        ORDER BY timestamp DESC
        LIMIT 100
    '''
    return pd.read_sql(query, conn)


st.title("Ticker Data Dashboard")

refresh_counter = st_autorefresh(interval=1000, key="refresh_counter")

hot_df, hot_df_display = load_hot_data()
warm_df, warm_df_display = load_warm_data(conn)

tab1, tab2, tab3 = st.tabs(["Hot Data", "Warm Data", "Diagnostics"])

with tab1:
    st.subheader("Hot Data")
    st.write(f"Rows: {len(hot_df):,} | Memory: {hot_df.memory_usage(deep=True).sum() / 1_048_576:.2f} MB")
    st.plotly_chart(plot_price(hot_df_display, "Hot Data"), use_container_width=True)

with tab2:
    st.subheader("Warm Data")
    st.write(f"Rows: {len(warm_df):,} | Memory: {warm_df.memory_usage(deep=True).sum() / 1_048_576:.2f} MB")
    st.plotly_chart(plot_price(warm_df_display, "Warm Data"), use_container_width=True)

with tab3:
    st.subheader("Diagnostics")

    consumer_df = load_consumer_metrics()
    producer_df = load_producer_metrics()

    # st.markdown("### Kafka Consumer Metrics (Last 100)")
    # st.dataframe(consumer_df)

    # st.markdown("### WebSocket Producer Metrics (Last 100)")
    # st.dataframe(producer_df)

    # === Avg Lag Bar Chart ===
    st.markdown("### Avg Lag Over Time (Consumer)")
    if not consumer_df.empty:
        fig = px.bar(
            consumer_df.sort_values("timestamp"),
            x="timestamp",
            y="avg_lag_sec",
            title="Kafka Consumer - Average Lag (seconds)",
            labels={"avg_lag_sec": "Average Lag (s)", "timestamp": "Timestamp"},
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Messages Over Time")

    # Round timestamps to nearest minute for alignment
    producer_df['timestamp_rounded'] = pd.to_datetime(producer_df['timestamp']).dt.floor('min')
    consumer_df['timestamp_rounded'] = pd.to_datetime(consumer_df['timestamp']).dt.floor('min')

    # Aggregate messages per minute
    producer_agg = producer_df.groupby('timestamp_rounded')['messages'].sum().reset_index(name='producer_messages')
    consumer_agg = consumer_df.groupby('timestamp_rounded')['messages'].sum().reset_index(name='consumer_messages')

    # Merge and compute difference
    message_diff_df = pd.merge(producer_agg, consumer_agg, on='timestamp_rounded', how='outer').fillna(0)
    message_diff_df['message_diff'] = message_diff_df['producer_messages'] - message_diff_df['consumer_messages']

    # Line chart: Producer and Consumer messages
    fig = px.line(
        message_diff_df.sort_values("timestamp_rounded"),
        x="timestamp_rounded",
        y=["producer_messages", "consumer_messages"],
        title="Producer vs Consumer Message Count (per Minute)",
        labels={"value": "Messages", "timestamp_rounded": "Timestamp", "variable": "Source"},
        markers=True
    )
    st.plotly_chart(fig, use_container_width=True)

    # Bar chart: Message difference
    st.markdown("### Message Difference Over Time (Producer - Consumer)")
    fig_diff = px.bar(
        message_diff_df.sort_values("timestamp_rounded"),
        x="timestamp_rounded",
        y="message_diff",
        title="Message Count Difference Over Time",
        labels={"message_diff": "Difference", "timestamp_rounded": "Timestamp"},
    )
    st.plotly_chart(fig_diff, use_container_width=True)