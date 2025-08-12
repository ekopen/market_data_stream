# ticks_db.py
# creates pricing database

import clickhouse_connect

import time, os
from datetime import timedelta, datetime, timezone
import pandas as pd
import threading
import boto3, os
from dotenv import load_dotenv
load_dotenv()  # Load from .env file

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
stop_event = threading.Event()

s3 = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

def new_client():
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='mysecurepassword'
    )

def create_ticks_db():
    ch = new_client()
    ch.command("DROP TABLE IF EXISTS ticks_db")
    ch.command('''
    CREATE TABLE IF NOT EXISTS ticks_db(
        timestamp       DateTime64(3, 'UTC'),
        timestamp_ms    Int64,
        symbol          String,
        price           Float64,
        volume          Float64,
        received_at     DateTime64(3, 'UTC'),
        insert_time     DateTime64(3, 'UTC') DEFAULT now64(3)
    ) 
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(timestamp)
    ORDER BY timestamp_ms
    ''')

def create_diagnostics_db():

    ch_client = new_client()

    ch_client.command("DROP TABLE IF EXISTS websocket_diagnostics")
    ch_client.command(f"""
    CREATE TABLE IF NOT EXISTS websocket_diagnostics (
        avg_timestamp DateTime64(3, 'UTC'),
        avg_received_at DateTime64(3, 'UTC'),
        avg_websocket_lag Float64,
        message_count Float64,
        diagnostics_timestamp    DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(toDate(avg_timestamp))
    ORDER BY avg_timestamp
    """)

    ch_client.command("DROP TABLE IF EXISTS processing_diagnostics")
    ch_client.command(f"""
    CREATE TABLE IF NOT EXISTS processing_diagnostics (
        avg_timestamp DateTime64(3, 'UTC'),
        avg_received_at DateTime64(3, 'UTC'),
        avg_processed_timestamp DateTime64(3, 'UTC'),
        avg_processing_lag Float64,
        message_count Float64,
        diagnostics_timestamp    DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(toDate(avg_timestamp))
    ORDER BY avg_timestamp
    """)

def insert_diagnostics(stop_event,duration):

    time.sleep(duration)

    ch_client = new_client()

    while not stop_event.is_set():
        print("Inserting diagnostics")
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=duration)
            cutoff_ms = int(cutoff_time.timestamp() * 1000)

            diagnostic_rows = ch_client.query(f'''
                SELECT * FROM ticks_db
                WHERE timestamp_ms > {cutoff_ms}
            ''').result_rows

            df = pd.DataFrame(diagnostic_rows, columns=[
                'timestamp', 'timestamp_ms', 'symbol', 'price', 'volume', 'received_at', 'insert_time'
                ])
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['received_at'] = pd.to_datetime(df['received_at'])
            df['insert_time'] = pd.to_datetime(df['insert_time'])

            avg_timestamp = df['timestamp'].mean()
            avg_received_at = df['received_at'].mean()
            avg_insert_time = df['insert_time'].mean()
            message_count = len(df)

            ch_client.insert('websocket_diagnostics',
                [(avg_timestamp, avg_received_at, (avg_received_at - avg_timestamp).total_seconds(), message_count)],
                column_names=['avg_timestamp', 'avg_received_at', 'avg_websocket_lag', 'message_count'])
            
            ch_client.insert('processing_diagnostics',
                [(avg_timestamp, avg_received_at, avg_insert_time, (avg_insert_time - avg_received_at).total_seconds(), message_count)],
                column_names=['avg_timestamp', 'avg_received_at', 'avg_processed_timestamp', 'avg_processing_lag', 'message_count'])
        
        except Exception as e:
            print("[websocket_diagnostics] Exception:", e)

        time.sleep(duration)