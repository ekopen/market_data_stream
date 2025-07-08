# dashboard.py
# creating a streamlit dashboard to visualize the data
# streamlit run dashboard.py

from config import DIAGNOSTIC_FREQUENCY

import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

from diagnostics import conn
from dashboard_func import load_hot_data, load_warm_data, plot_price, plot_ticks_per_second, plot_websocket_lag

st.title("Real Time Ethereum Price Feed")

refresh_counter = st_autorefresh(interval=5000, key="refresh_counter") #refresh every 5 seconds

tab1, tab2, tab3 = st.tabs(["Hot Data", "Warm Data", "Diagnostics"])

with tab1:

    st.subheader("Hot Data")

    hot_df, hot_df_display = load_hot_data()

    st.write("After ingestion from the websocket/Kafka, our data gets staged in Clickhouse, a database management system optimized for speed and real time analytics.")
    st.write("Data stays in this table for 5 minutes, and then is moved to the warm table.")
    st.write("A dashboard featuring that data is shown below, with a CSV available for download.")
    
    st.write(f"Rows: {len(hot_df):,} | Memory: {hot_df.memory_usage(deep=True).sum() / 1_048_576:.2f} MB")
    st.plotly_chart(plot_price(hot_df_display, "Live Hot Data Feed"), use_container_width=True)

    csv_hot = hot_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Hot Data as CSV",
        data=csv_hot,
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

    csv_hot = warm_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Warm Data as CSV",
        data=csv_hot,
        file_name='warm_data.csv',
        mime='text/csv'
    )

    st.markdown("Recent Data Sample")
    st.dataframe(warm_df.head(10))

with tab3:

    st.subheader("Diagnostics")

    st.markdown("##### General Diagnostics")
    st.write("How much data are we recieving?")

    query = '''
        SELECT AVG(message_count) AS avg_message_count
        FROM websocket_diagnostics 
        LIMIT 100
    '''
    ticks_per_sec = pd.read_sql(query, conn).iloc[0]['avg_message_count'] / DIAGNOSTIC_FREQUENCY
    st.metric(label="Average Ticks per Second: ", value=round(ticks_per_sec,2))

    query = ''' 
    SELECT timestamp, message_count
    FROM websocket_diagnostics
    ORDER BY timestamp DESC
    LIMIT 100
    '''
    df = pd.read_sql(query, conn)
    fig = plot_ticks_per_second(df, "Average Ticks per Second", height=350, freq=DIAGNOSTIC_FREQUENCY)
    st.plotly_chart(fig, use_container_width=True)
    


    st.markdown("##### Websocket Diagnostics")
    st.write("How delayed is the websocket data on arrival?") 
    
    query = '''
        SELECT AVG(websocket_lag) AS avg_websocket_lag
        FROM websocket_diagnostics
    '''
    avg_websocket_lag = pd.read_sql(query, conn).iloc[0]['avg_websocket_lag']
    st.metric(label="Average Lag (in Seconds): ", value=round(avg_websocket_lag,2))

    query = '''
    SELECT timestamp, websocket_lag
    FROM websocket_diagnostics
    ORDER BY timestamp DESC
    LIMIT 100
    '''
    df = pd.read_sql(query, conn)
    st.plotly_chart(plot_websocket_lag(df, "Average WebSocket Lag (in Seconds)"), use_container_width=True)

    st.markdown("##### Processing Diagnostics")
    st.write("How long does our pipeline take to process data before it reaches the database?")

    query = '''
        SELECT * FROM processing_diagnostics
        ORDER BY processed_timestamp DESC
        LIMIT 5
    '''
    processing_diagnostics = pd.read_sql(query, conn)
    processing_diagnostics['timestamp'] = pd.to_datetime(processing_diagnostics['timestamp'], utc=True)
    st.dataframe(processing_diagnostics)
    
    st.markdown("##### Transfer Diagnostics")
    st.write("Is the data transferring correctly between tables?")

    query = '''
        SELECT * FROM transfer_diagnostics
        ORDER BY transfer_end DESC
        LIMIT 5
    '''
    transfer_diagnostics = pd.read_sql(query, conn)
    transfer_diagnostics['transfer_start'] = pd.to_datetime(transfer_diagnostics['transfer_start'], utc=True)
    transfer_diagnostics['transfer_end'] = pd.to_datetime(transfer_diagnostics['transfer_end'], utc=True)

    st.dataframe(transfer_diagnostics)


     # st.write("Sample of diagnostics data:")      
    # query = '''
    #     SELECT * FROM websocket_diagnostics
    #     ORDER BY received_at DESC
    #     LIMIT 5
    # '''
    # websocket_diagnostics = pd.read_sql(query, conn)
    # websocket_diagnostics['timestamp'] = pd.to_datetime(websocket_diagnostics['timestamp'], utc=True)
    # st.dataframe(websocket_diagnostics)



