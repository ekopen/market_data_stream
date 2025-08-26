# dashboard.py
# streamlit run dashboard.py

import streamlit as st
import pandas as pd
import numpy as np
from streamlit_autorefresh import st_autorefresh
from dashboard_func import (
    load_ticks_db,
    plot_price,
    plot_pricing_stats,
    plot_ticks_per_second,
    plot_websocket_lag,
    plot_processing_lag,
    load_ticks_monitoring_db,
    ohlcv_from_ticks,  # <- you added this in func file
    load_monitoring_messages,
    load_log_snippets
)
from config import HEARTBEAT_FREQUENCY


# ---------- Streamlit Config ----------
st.set_page_config(page_title="Real Time Ethereum Price Feed", layout="wide")
st.title("Real Time Ethereum Price Feed")

# Auto-refresh every 5 seconds
_ = st_autorefresh(interval=5000, key="refresh_counter")


# ---------- Helpers ----------
def _to_datetime_any(series):
    """Robust timestamp parsing: datetime-like, epoch s, or epoch ms."""
    s = pd.to_datetime(series, errors="coerce", utc=True)
    if s.isna().all():
        arr = pd.to_numeric(series, errors="coerce")
        unit = "ms" if np.nanmedian(arr) >= 1e12 else "s"
        s = pd.to_datetime(arr, unit=unit, errors="coerce", utc=True)
    return s


def _fmt_num(x, n=2):
    try:
        return f"{float(x):,.{n}f}"
    except Exception:
        return "-"


# ---------- Data ----------
pricing_df, pricing_df_display = load_ticks_db()

# Compute OHLCV stats directly from raw ticks
pricing_stats_df = ohlcv_from_ticks(pricing_df, interval="5s")

stats_view = pricing_stats_df.copy()
if not stats_view.empty and "stats_timestamp" in stats_view.columns:
    stats_view["stats_timestamp"] = _to_datetime_any(stats_view["stats_timestamp"])
    stats_view = stats_view.dropna(subset=["stats_timestamp"]).sort_values("stats_timestamp", ascending=False)


# ---------- Tabs ----------
tab_pricing, tab_diag = st.tabs(["Pricing", "Diagnostics"])

# ===== Pricing (side-by-side) =====
with tab_pricing:
    st.caption(
        "Kafka streams raw pricing ticks from an exchange websocket into ClickHouse. "
        "OHLCV statistics are computed directly from raw ticks on the fly. "
        "All data is eventually archived as Parquet on AWS."
    )

    # ---- Summary metrics ----
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric(label="Current Price", value=_fmt_num(pricing_df['price'].iloc[0]))
    with c2:
        st.metric(label="High Price", value=_fmt_num(pricing_df['price'].max()))
    with c3:
        st.metric(label="Low Price", value=_fmt_num(pricing_df['price'].min()))

    left, right = st.columns([1.5, 1.5])

    # ---- Left: Live Tick Line + Volume ----
    with left:
        st.subheader("Pricing Data (Ticks)")

        st.plotly_chart(
            plot_price(pricing_df_display, "Tick Line + Volume"),
            use_container_width=True
        )

    # ---- Right: OHLC Candles + Volume ----
    with right:
        st.subheader("Pricing Statistics (OHLCV)")

        st.plotly_chart(
            plot_pricing_stats(pricing_stats_df, "OHLC (Candles) + Volume"),
            use_container_width=True
        )

    st.write(
        f"Rows: {len(pricing_df):,} · "
        f"Memory: {pricing_df.memory_usage(deep=True).sum() / 1_048_576:.2f} MB"
    )
    dl_col, _ = st.columns([1, 5])
    with dl_col:
        st.download_button(
            label="Download Pricing Data (CSV)",
            data=pricing_df.to_csv(index=False).encode("utf-8"),
            file_name="tick_data.csv",
            mime="text/csv",
        )

    st.markdown("**Recent Pricing Data Sample**")
    st.dataframe(pricing_df.head(10), use_container_width=True)

# ===== Diagnostics =====
with tab_diag:
    st.subheader("Diagnostics")
    st.caption("Websocket throughput and pipeline latency.")

    # Example: still pulling monitoring diagnostics from DB
    websocket_avg_df = load_ticks_monitoring_db("websocket_diagnostics", limit=100, order_col="diagnostics_timestamp")

    st.write("How much data are we receiving?")
    if not websocket_avg_df.empty:
        ticks_per_sec = websocket_avg_df['message_count'].mean() / HEARTBEAT_FREQUENCY
        st.metric(label="Average Ticks per Second", value=round(ticks_per_sec, 2))
        st.plotly_chart(
            plot_ticks_per_second(websocket_avg_df, "Average Ticks per Second"),
            use_container_width=True
        )

    st.write("How delayed is the websocket data on arrival?")
    if not websocket_avg_df.empty:
        avg_websocket_lag = websocket_avg_df['avg_websocket_lag'].mean()
        st.metric(label="Average Websocket Lag (s)", value=round(avg_websocket_lag, 2))
        st.plotly_chart(
            plot_websocket_lag(websocket_avg_df, "Average WebSocket Lag (s)"),
            use_container_width=True
        )

    processing_df = load_ticks_monitoring_db("processing_diagnostics", limit=100, order_col="diagnostics_timestamp")

    st.write("How long does our pipeline take to process before it hits the DB?")
    if not processing_df.empty:
        avg_processing_lag = processing_df['avg_processing_lag'].mean()
        st.metric(label="Average Processing Lag (s)", value=round(avg_processing_lag, 2))
        st.plotly_chart(
            plot_processing_lag(processing_df, "Average Processing Lag (s)"),
            use_container_width=True
        )

    st.divider()
    st.subheader("Monitoring Events")
    mon_df = load_monitoring_messages(limit=20)
    if not mon_df.empty:
        st.dataframe(mon_df, use_container_width=True)
    else:
        st.info("No recent monitoring events found.")

    st.divider()
    st.subheader("Recent Log Messages (Warnings and Errors)")
    log_df = load_log_snippets(limit=10)
    if not log_df.empty:
        st.dataframe(log_df, use_container_width=True)
    else:
        st.info("No relevant log messages found.")

    st.divider()
    st.subheader("Recent Log Messages (Info and Debug)")
    log_df = load_log_snippets(levels=("INFO", "DEBUG"),  limit=10)
    if not log_df.empty:
        st.dataframe(log_df, use_container_width=True)
    else:
        st.info("No relevant log messages found.")