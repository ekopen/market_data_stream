# dashboard.py
# creating a streamlit dashboard to visualize the data
# streamlit run dashboard.py

import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh
from dashboard_func import load_ticks_db, load_diagnostics, plot_price, plot_ticks_per_second, plot_websocket_lag, plot_processing_lag

from config import DIAGNOSTIC_FREQUENCY

st.title("Real Time Ethereum Price Feed")

refresh_counter = st_autorefresh(interval=5000, key="refresh_counter") #refresh every 5 seconds

tab1, tab2 = st.tabs(["Ticks Data", "Diagnostics"])

df, df_display = load_ticks_db()

with tab1:

    st.subheader("Ticks Data")

    st.write("After ingestion from the websocket/Kafka, our data gets staged in Clickhouse, a database management system optimized for speed and real time analytics.")
    st.write("Data stays in this table and then is eventually moved to cold storage.")
    st.write("A dashboard featuring that data is shown below, with a CSV available for download.")
    
    st.write(f"Rows: {len(df):,} | Memory: {df.memory_usage(deep=True).sum() / 1_048_576:.2f} MB")
    st.plotly_chart(plot_price(df_display, "Live Data Feed"), use_container_width=True)

    st.download_button(
        label="Download Tick Data as CSV",
        data=df.to_csv(index=False).encode('utf-8'),
        file_name='tick_data.csv',
        mime='text/csv'
    )

    st.markdown("Recent Data Sample")
    st.dataframe(df.head(10))

with tab2:

    st.subheader("Diagnostics")

    websocket_avg_df = load_diagnostics("websocket_diagnostics", limit=100, order_col="avg_timestamp")
    
    st.write("How much data are we receiving?")
    if not websocket_avg_df.empty:
        ticks_per_sec = websocket_avg_df['message_count'].mean() / DIAGNOSTIC_FREQUENCY
        st.metric(label="Average Ticks per Second:", value=round(ticks_per_sec, 2))
        st.plotly_chart(plot_ticks_per_second(websocket_avg_df, "Average Ticks per Second", height=350, freq=DIAGNOSTIC_FREQUENCY), use_container_width=True)

    st.write("How delayed is the websocket data on arrival?")
    if not websocket_avg_df.empty:
        avg_websocket_lag = websocket_avg_df['avg_websocket_lag'].mean()
        st.metric(label="Average Websocket Lag (in Seconds):", value=round(avg_websocket_lag, 2))
        st.plotly_chart(plot_websocket_lag(websocket_avg_df, "Average WebSocket Lag (in Seconds)"), use_container_width=True)

    processing_df = load_diagnostics("processing_diagnostics", limit=100, order_col="avg_processed_timestamp")

    st.write("How long does our pipeline take to process data before it reaches the database?")
    if not processing_df.empty:
        avg_processing_lag = processing_df['avg_processing_lag'].mean()
        st.metric(label="Average Processing Lag (in Seconds):", value=round(avg_processing_lag, 2))
        st.plotly_chart(plot_processing_lag(processing_df, "Average Processing Lag (in Seconds)"), use_container_width=True)




