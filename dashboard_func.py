# dashboard func.py
# all the functions for dashboard.py to improve readability

from config import HEARTBEAT_FREQUENCY
from clickhouse import new_client
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import os
import re

# --- put near the top (below imports) ---
MAROON = "maroon"

def ohlcv_from_ticks(df: pd.DataFrame, interval: str = "1s") -> pd.DataFrame:
    """
    Build OHLCV from raw ticks. Expects columns: timestamp, price, volume.
    Returns columns: stats_timestamp, open_price, high_price, low_price, close_price, volume
    """
    if df.empty or not {"timestamp", "price", "volume"}.issubset(df.columns):
        return pd.DataFrame()

    d = df.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"], utc=True, errors="coerce")
    d = d.dropna(subset=["timestamp"]).set_index("timestamp").sort_index()

    out = d.resample(interval).agg(
        open_price=("price", "first"),
        high_price=("price", "max"),
        low_price =("price", "min"),
        close_price=("price", "last"),
        volume=("volume", "sum"),
    ).dropna().reset_index().rename(columns={"timestamp": "stats_timestamp"})

    return out

def _apply_shared_layout(fig, title, height, y_price_range=None):
    """One layout to rule them all: identical axes, margins, legend, etc."""
    fig.update_layout(
        title=title,
        height=height,
        margin=dict(l=40, r=40, t=40, b=40),
        showlegend=True,
        legend=dict(x=0, y=1.1, orientation='h'),
        hovermode="x",
        template=None,   # keep Plotly defaults consistent with your left chart
        xaxis=dict(
            title="Time",
            type="date",
            showgrid=False,
            rangeslider=dict(visible=False),
            tickformat="%H:%M",   # same time display on both
            tickangle=0
        ),
        yaxis=dict(
            title="Price",
            tickformat=".2f",
            side="left",
            range=y_price_range,
            showgrid=True,
            gridcolor="rgba(0,0,0,0.08)",
            zeroline=False,
        ),
        yaxis2=dict(
            title="Volume",
            overlaying="y",
            side="right",
            showgrid=False
        ),
    )
    return fig

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

def load_ticks_monitoring_db(table, limit=100, order_col="diagnostics_timestamp"):
    ch_client = new_client()
    query = f"""
        SELECT *
        FROM {table}
        ORDER BY {order_col} DESC
        LIMIT {limit}
    """
    df = ch_client.query_df(query)

    return df

def plot_price(df, title, height=420):
    if df.empty or 'timestamp' not in df.columns or 'price' not in df.columns:
        st.warning(f"No data available for {title}")
        return go.Figure().update_layout(title=title, height=height)

    df = df.sort_values("timestamp")
    min_price = df['price'].min()
    max_price = df['price'].max()
    y_price_range = [min_price * .9999, max_price * 1.0001]

    fig = go.Figure()

    # Price line
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['price'],
        mode='lines',
        line=dict(width=2, color=MAROON),
        name="Price"
    ))

    # Embedded volume (right axis)
    fig.add_trace(go.Bar(
        x=df['timestamp'],
        y=df['volume'],
        name='Volume',
        marker=dict(color=MAROON),
        opacity=0.4,
        yaxis='y2'
    ))

    _apply_shared_layout(fig, title, height, y_price_range)
    # tighten bar outlines to match the vibe
    fig.update_traces(marker_line_width=0.5, selector=dict(type='bar'))
    return fig


def _to_datetime_any(series):
    s = pd.to_datetime(series, errors='coerce', utc=True)
    if s.isna().all():
        # handle epoch ms / s automatically
        arr = pd.to_numeric(series, errors='coerce')
        # heuristic: if values are large (>= 10^12), they’re probably ms
        unit = 'ms' if np.nanmedian(arr) >= 1e12 else 's'
        s = pd.to_datetime(arr, unit=unit, errors='coerce', utc=True)
    return s

def plot_pricing_stats(df, title, height=420):
    required = {'stats_timestamp','open_price','high_price','low_price','close_price','volume'}
    if df.empty or not required.issubset(df.columns):
        st.warning(f"No pricing statistics data available for {title}")
        return go.Figure().update_layout(title=title, height=height)

    d = df.copy()
    d['stats_timestamp'] = _to_datetime_any(d['stats_timestamp'])
    d = d.dropna(subset=['stats_timestamp']).sort_values('stats_timestamp')

    # hover text (older Plotly candlestick builds)
    hover_text = (
        "Time: " + d['stats_timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S UTC") +
        "<br>Open: "  + d['open_price'].map(lambda x: f"{x:.4f}") +
        "<br>High: "  + d['high_price'].map(lambda x: f"{x:.4f}") +
        "<br>Low: "   + d['low_price'].map(lambda x: f"{x:.4f}") +
        "<br>Close: " + d['close_price'].map(lambda x: f"{x:.4f}")
    )

    pr_min = float(min(d['low_price'].min(), d[['open_price','close_price']].min().min()))
    pr_max = float(max(d['high_price'].max(), d[['open_price','close_price']].max().max()))
    y_pad = (pr_max - pr_min) * 0.0008 if pr_max > pr_min else (pr_max or 0) * 0.0008
    y_price_range = [pr_min - y_pad, pr_max + y_pad]

    fig = go.Figure()

    # Candles
    fig.add_trace(go.Candlestick(
        x=d['stats_timestamp'],
        open=d['open_price'],
        high=d['high_price'],
        low=d['low_price'],
        close=d['close_price'],
        name="Price",
        increasing_line_color="#2ca02c",
        decreasing_line_color="#d62728",
        hoverinfo="text",
        text=hover_text
    ))

    # Embedded volume (right axis) — exact match to tick chart styling
    fig.add_trace(go.Bar(
        x=d['stats_timestamp'],
        y=d['volume'],
        name="Volume",
        marker=dict(color=MAROON),
        opacity=0.4,
        yaxis="y2",
        hoverinfo="text",
        text=("Time: " + d['stats_timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
              + "<br>Volume: " + d['volume'].map(lambda x: f"{x:,.0f}"))
    ))

    _apply_shared_layout(fig, title, height, y_price_range)
    fig.update_traces(marker_line_width=0.5, selector=dict(type='bar'))
    return fig


def plot_ticks_per_second(df, title, height=350, freq=15):
    if df.empty or 'diagnostics_timestamp' not in df.columns or 'message_count' not in df.columns:
        st.warning(f"No diagnostics data available for {title}")
        return go.Figure().update_layout(title=title, height=height)

    df = df.sort_values("diagnostics_timestamp").copy()
    df['ticks_per_second'] = df['message_count'] / freq

    min_tps = df['ticks_per_second'].min()
    max_tps = df['ticks_per_second'].max()
    y_range = [min_tps * .95, max_tps * 1.05]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['diagnostics_timestamp'],
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
    if df.empty or 'avg_timestamp' not in df.columns or 'avg_websocket_lag' not in df.columns:
        st.warning(f"No diagnostics data available for {title}")
        return go.Figure().update_layout(title=title, height=height)

    df = df.sort_values("avg_timestamp").copy()

    min_lag = df['avg_websocket_lag'].min()
    max_lag = df['avg_websocket_lag'].max()
    y_range = [min_lag * 1.05, max_lag * 1.05]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['avg_timestamp'],
        y=df['avg_websocket_lag'],
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
    if df.empty or 'avg_timestamp' not in df.columns or 'avg_processing_lag' not in df.columns:
        st.warning(f"No diagnostics data available for {title}")
        return go.Figure().update_layout(title=title, height=height)

    df = df.sort_values("avg_timestamp").copy()

    min_lag = df['avg_processing_lag'].min()
    max_lag = df['avg_processing_lag'].max()
    y_range = [min_lag * 1.05, max_lag * 1.05]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['avg_timestamp'],
        y=df['avg_processing_lag'],
        mode='lines+markers',
        line=dict(width=2, color="darkblue"),
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

def load_monitoring_messages(limit=20):
    ch_client = new_client()
    query = f"""
        SELECT monitoring_timestamp, message
        FROM monitoring_db
        ORDER BY monitoring_timestamp DESC
        LIMIT {limit}
    """
    return ch_client.query_df(query)



def load_log_snippets(log_file="log_data/app.log", levels=("ERROR", "WARNING"), limit=20):
    log_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \| (\w+) \| ([^|]+) \| (.*)")
    entries = []
    current_entry = None

    with open(log_file, encoding="utf-8") as f:
        for line in f:
            match = log_pattern.match(line)
            if match:
                # start of a new log entry
                if current_entry:
                    entries.append(current_entry)
                timestamp, level, logger, message = match.groups()
                current_entry = {
                    "timestamp": timestamp,
                    "level": level,
                    "logger": logger,
                    "message": message.strip()
                }
            elif current_entry:
                # continuation of the previous message (multiline stack trace)
                current_entry["message"] += "\n" + line.strip()

    if current_entry:
        entries.append(current_entry)

    df = pd.DataFrame(entries)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f", errors="coerce", utc=True)
    df = df[df["level"].isin(levels)]
    df = df.sort_values("timestamp", ascending=False).head(limit)
    return df

