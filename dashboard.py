# dashboard.py
# creating a streamlit dashboard to visualize the data
# streamlit run dashboard.py

from config import DIAGNOSTIC_FREQUENCY

import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh

import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

from clickhouse_connect import get_client
client = get_client(host='localhost', port=8123, username='default', password='mysecurepassword')

from dashboard_func import load_hot_data, load_warm_data, load_diagnostics, plot_price, plot_ticks_per_second, plot_websocket_lag

st.title("Real Time Ethereum Price Feed")

refresh_counter = st_autorefresh(interval=5000, key="refresh_counter") #refresh every 5 seconds

tab1, tab2, tab3, tab4 = st.tabs(["Hot Data", "Warm Data", "Diagnostics", "Combined Data"])

with tab1:

    st.subheader("Hot Data")

    hot_df, hot_df_display = load_hot_data()

    st.write("After ingestion from the websocket/Kafka, our data gets staged in Clickhouse, a database management system optimized for speed and real time analytics.")
    st.write("Data stays in this table for 5 minutes, and then is moved to the warm table.")
    st.write("A dashboard featuring that data is shown below, with a CSV available for download.")
    
    st.write(f"Rows: {len(hot_df):,} | Memory: {hot_df.memory_usage(deep=True).sum() / 1_048_576:.2f} MB")
    st.plotly_chart(plot_price(hot_df_display, "Live Hot Data Feed"), use_container_width=True)

    st.download_button(
        label="Download Hot Data as CSV",
        data=hot_df.to_csv(index=False).encode('utf-8'),
        file_name='hot_data.csv',
        mime='text/csv'
    )

    st.markdown("Recent Data Sample")
    st.dataframe(hot_df.head(10))

with tab2:

    st.subheader("Warm Data")

    warm_df, warm_df_display = load_warm_data()

    st.write("After 5 minutes in the hot table, data is moved to PostgreSQL for warm storage. PostgreSQL offers reliable, query-friendly access, making it ideal for short-term analysis and dashboards.")
    st.write("Data stays in this table for 30 minutes before being archived as a parquet, which is then uploaded to cold storage (in this case AWS).")
    st.write("A dashboard featuring that data is shown below, with a CSV available for download.")

    st.write(f"Rows: {len(warm_df):,} | Memory: {warm_df.memory_usage(deep=True).sum() / 1_048_576:.2f} MB")
    st.plotly_chart(plot_price(warm_df_display, "Live Warm Data Feed"), use_container_width=True)

    st.download_button(
        label="Download Warm Data as CSV",
        data=warm_df.to_csv(index=False).encode('utf-8'),
        file_name='warm_data.csv',
        mime='text/csv'
    )

    st.markdown("Recent Data Sample")
    st.dataframe(warm_df.head(10))

with tab3:

    st.subheader("Diagnostics")

    # General Diagnostics
    st.markdown("##### General Diagnostics")
    st.write("How much data are we receiving?")
    websocket_avg_df = load_diagnostics("websocket_diagnostics", limit=100, order_col="timestamp")
    if not websocket_avg_df.empty:
        ticks_per_sec = websocket_avg_df['message_count'].mean() / DIAGNOSTIC_FREQUENCY
        st.metric(label="Average Ticks per Second:", value=round(ticks_per_sec, 2))
        st.plotly_chart(plot_ticks_per_second(websocket_avg_df, "Average Ticks per Second", height=350, freq=DIAGNOSTIC_FREQUENCY), use_container_width=True)
    else:
        st.warning("No websocket diagnostics data available.")

    # WebSocket Lag
    st.markdown("##### Websocket Diagnostics")
    st.write("How delayed is the websocket data on arrival?")
    if not websocket_avg_df.empty:
        avg_websocket_lag = websocket_avg_df['websocket_lag'].mean()
        st.metric(label="Average Lag (in Seconds):", value=round(avg_websocket_lag, 2))
        st.plotly_chart(plot_websocket_lag(websocket_avg_df, "Average WebSocket Lag (in Seconds)"), use_container_width=True)

    # Processing Diagnostics
    st.markdown("##### Processing Diagnostics")
    st.write("How long does our pipeline take to process data before it reaches the database?")
    processing_df = load_diagnostics("processing_diagnostics", limit=5, order_col="processed_timestamp")
    st.dataframe(processing_df)

    # Transfer Diagnostics
    st.markdown("##### Transfer Diagnostics")
    st.write("Is the data transferring correctly between tables?")
    transfer_df = load_diagnostics("transfer_diagnostics", limit=5, order_col="transfer_end")
    st.dataframe(transfer_df)

with tab4:

        st.subheader("Combined View")


