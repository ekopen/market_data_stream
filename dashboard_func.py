# dashboard func.py
# all the functions for dashboard.py to improve readability

from config import DIAGNOSTIC_FREQUENCY

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from market_ticks import new_client
import clickhouse_connect

def get_client_diagnostics():
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='mysecurepassword'
    )

def reduceTickFreq(df, increment):
    if 'timestamp' not in df.columns:
        return pd.DataFrame()  # Return empty if column is missing

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.set_index("timestamp").resample(increment).agg({
        "price": "mean",
        "volume": "sum"
    }).dropna().reset_index()
    return df

def load_ticks_db():
    ch_client = new_client()
    df = ch_client.query_df("SELECT * FROM ticks_db ORDER BY timestamp DESC")
    if df.empty or 'timestamp' not in df.columns:
        return df, pd.DataFrame()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    
    return df, reduceTickFreq(df, "1s").sort_values("timestamp")

def load_diagnostics(table, limit=100, order_col="timestamp"):
    ch_client = get_client_diagnostics()
    query = f"""
        SELECT *
        FROM {table}
        ORDER BY {order_col} DESC
        LIMIT {limit}
    """
    df = ch_client.query_df(query)
    for col in ['timestamp', 'received_at', 'processed_timestamp', 'transfer_start', 'transfer_end']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)
    return df

def plot_price(df, title, height=350):

    if df.empty or 'timestamp' not in df.columns or 'price' not in df.columns:
        st.warning(f"No data available for {title}")
        return go.Figure().update_layout(title=title, height=height)

    df = df.sort_values("timestamp")

    min_price = df['price'].min()
    max_price = df['price'].max()
    y_price_range = [min_price * .9999, max_price * 1.0001]


    fig = go.Figure()

    # Price Line
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['price'],
        mode='lines',
        line=dict(width=2, color="maroon"),
        name="Price"
    ))

    # Volume Bar — matching color
    fig.add_trace(go.Bar(
        x=df['timestamp'],
        y=df['volume'],
        name='Volume',
        marker=dict(color="maroon"),
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

def plot_ticks_per_second(df, title, height=350, freq=15):
    if df.empty or 'timestamp' not in df.columns or 'message_count' not in df.columns:
        st.warning(f"No diagnostics data available for {title}")
        return go.Figure().update_layout(title=title, height=height)

    df = df.sort_values("timestamp").copy()
    df['ticks_per_second'] = df['message_count'] / freq

    min_tps = df['ticks_per_second'].min()
    max_tps = df['ticks_per_second'].max()
    y_range = [min_tps * .95, max_tps * 1.05]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['ticks_per_second'],
        mode='lines+markers',
        line=dict(width=2, color="darkgreen"),
        name="Ticks per Second"
    ))

    fig.update_layout(
        title=title,
        height=height,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis=dict(title='Time'),
        yaxis=dict(
            title='Ticks per Second',
            tickformat=".1f",
            range=y_range,
            side='left'
        ),
        legend=dict(x=0, y=1.1, orientation='h')
    )

    return fig

def plot_websocket_lag(df, title, height=350):
    if df.empty or 'timestamp' not in df.columns or 'websocket_lag' not in df.columns:
        st.warning(f"No diagnostics data available for {title}")
        return go.Figure().update_layout(title=title, height=height)

    df = df.sort_values("timestamp").copy()

    min_lag = df['websocket_lag'].min()
    max_lag = df['websocket_lag'].max()
    y_range = [min_lag * 1.05, max_lag * 1.05]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['websocket_lag'],
        mode='lines+markers',
        line=dict(width=2, color="darkred"),
        name="WebSocket Lag (s)"
    ))

    fig.update_layout(
        title=title,
        height=height,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis=dict(title='Time'),
        yaxis=dict(
            title='WebSocket Lag (seconds)',
            tickformat=".2f",
            range=y_range,
            side='left'
        ),
        legend=dict(x=0, y=1.1, orientation='h')
    )

    return fig

def plot_processing_lag(df, title, height=350):
    if df.empty or 'timestamp' not in df.columns or 'processing_lag' not in df.columns:
        st.warning(f"No diagnostics data available for {title}")
        return go.Figure().update_layout(title=title, height=height)

    df = df.sort_values("timestamp").copy()

    min_lag = df['processing_lag'].min()
    max_lag = df['processing_lag'].max()
    y_range = [min_lag * 1.05, max_lag * 1.05]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['processing_lag'],
        mode='lines+markers',
        line=dict(width=2, color="darkred"),
        name="Processing Lag (s)"
    ))

    fig.update_layout(
        title=title,
        height=height,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis=dict(title='Time'),
        yaxis=dict(
            title='Processing Lag (seconds)',
            tickformat=".2f",
            range=y_range,
            side='left'
        ),
        legend=dict(x=0, y=1.1, orientation='h')
    )

    return fig

