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
    if 'timestamp' not in df.columns:
        return pd.DataFrame()  # Return empty if column is missing

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
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df_display = reduceTickFreq(df,"1s")
    return df, df_display.sort_values("timestamp")

def load_warm_data(conn):
    query = '''
        SELECT * FROM price_ticks
        ORDER BY timestamp DESC
    '''
    df = pd.read_sql(query, conn)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
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

    # Set color based on title
    if "Hot" in title:
        price_color = "maroon"
    elif "Warm" in title:
        price_color = "navy"
    else:
        price_color = "black"

    fig = go.Figure()

    # Price Line
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['price'],
        mode='lines',
        line=dict(width=2, color=price_color),
        name="Price"
    ))

    # Volume Bar — matching color
    fig.add_trace(go.Bar(
        x=df['timestamp'],
        y=df['volume'],
        name='Volume',
        marker=dict(color=price_color),
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

st.title("Real Time Ethereum Price Feed")

refresh_counter = st_autorefresh(interval=1000, key="refresh_counter")

hot_df, hot_df_display = load_hot_data()
warm_df, warm_df_display = load_warm_data(conn)


tab1, tab2, tab3 = st.tabs(["Hot Data", "Warm Data", "Diagnostics"])

with tab1:
    st.subheader("Hot Data")
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
        

    query = '''
        SELECT * FROM websocket_diagnostics
    '''
    websocket_diagnostics = pd.read_sql(query, conn)
    websocket_diagnostics['timestamp'] = pd.to_datetime(websocket_diagnostics['timestamp'], utc=True)


    query = '''
        SELECT * FROM processing_diagnostics
    '''
    processing_diagnostics = pd.read_sql(query, conn)
    processing_diagnostics['timestamp'] = pd.to_datetime(processing_diagnostics['timestamp'], utc=True)

    query = '''
        SELECT * FROM transfer_diagnostics
    '''
    transfer_diagnostics = pd.read_sql(query, conn)
    transfer_diagnostics['transfer_start'] = pd.to_datetime(transfer_diagnostics['transfer_start'], utc=True)
    transfer_diagnostics['transfer_end'] = pd.to_datetime(transfer_diagnostics['transfer_end'], utc=True)


    st.subheader("Diagnostics")
    st.dataframe(websocket_diagnostics)
    st.dataframe(processing_diagnostics)
    st.dataframe(transfer_diagnostics)



