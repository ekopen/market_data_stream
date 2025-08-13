# ticks_db.py
# creates pricing database

import clickhouse_connect

import time, os
from datetime import timedelta, datetime, timezone
import pandas as pd
import threading
import boto3, os
from dotenv import load_dotenv
import subprocess
import sys
load_dotenv()  # Load from .env file

import logging
logger = logging.getLogger(__name__)

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
    logger.info("Creating ticks_db table.")
    ch = new_client()
    # ch.command('''DROP TABLE IF EXISTS ticks_db''')  # drop if exists to ensure fresh creation
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
    logger.info("ticks_db table created successfully.")

def create_diagnostics_db():
    logger.info("Creating websocket_diagnostics table.")
    ch = new_client()
    # ch.command('''DROP TABLE IF EXISTS websocket_diagnostics''')  # drop if exists to ensure fresh creation
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS websocket_diagnostics (
        avg_timestamp Nullable(DateTime64(3, 'UTC')),
        avg_received_at Nullable(DateTime64(3, 'UTC')),
        avg_websocket_lag Nullable(Float64),
        message_count Float64,
        diagnostics_timestamp    DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(toDate(diagnostics_timestamp))
    ORDER BY diagnostics_timestamp
    """)
    logger.info("websocket_diagnostics table created successfully.")

    logger.info("Creating processing_diagnostics table.")
    #ch.command('''DROP TABLE IF EXISTS processing_diagnostics''')  # drop if exists to ensure fresh creation
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS processing_diagnostics (
        avg_timestamp Nullable(DateTime64(3, 'UTC')),
        avg_received_at Nullable(DateTime64(3, 'UTC')),
        avg_processed_timestamp Nullable(DateTime64(3, 'UTC')),
        avg_processing_lag Nullable(Float64),
        message_count Float64,
        diagnostics_timestamp    DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(toDate(diagnostics_timestamp))
    ORDER BY diagnostics_timestamp
    """)
    logger.info("processing_diagnostics table created successfully.")

def insert_diagnostics(stop_event,duration,empty_limit):

    time.sleep(duration)
    ch = new_client()
    empty_streak = 0
    test_stop = 0

    while not stop_event.is_set():
        logger.debug("Starting diagnostics insert cycle.")
        try:
            current_time = datetime.now(timezone.utc)
            current_time_ms = int(current_time.timestamp() * 1000)
            cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=duration)
            cutoff_time_ms = int(cutoff_time.timestamp() * 1000)

            diagnostic_rows = ch.query(f'''
                SELECT * FROM ticks_db
                WHERE toUnixTimestamp64Milli(insert_time) > {cutoff_time_ms} AND toUnixTimestamp64Milli(insert_time) <= {current_time_ms}
            ''').result_rows

            df = pd.DataFrame(diagnostic_rows, columns=[
                'timestamp', 'timestamp_ms', 'symbol', 'price', 'volume', 'received_at', 'insert_time'
                ])
            
            test_stop += 1
            
            # if test_stop > 2:
                # df = pd.DataFrame() #FOR TESTING PURPOSES ONLY, TO SIMULATE EMPTY DIAGNOSTICS

            #if the system is down, we still want to record diagnostics data to show lag
            if df.empty:

                avg_timestamp = None
                avg_received_at = None
                avg_insert_time = None
                message_count = 0
                ws_lag = None
                proc_lag = None

                empty_streak += 1
                logger.debug("Diagnostics has returned an empty dataframe. Occurrence count: {empty_streak}")

                if empty_streak >= empty_limit:
                    logger.warning(f"Diagnostics has been empty for {empty_streak} consecutive cycles. Restarting system.")
                    subprocess.Popen([sys.executable] + sys.argv)
                    os._exit(0)

            else:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df['received_at'] = pd.to_datetime(df['received_at'])
                df['insert_time'] = pd.to_datetime(df['insert_time'])

                avg_timestamp = df['timestamp'].mean()
                avg_received_at = df['received_at'].mean()
                avg_insert_time = df['insert_time'].mean()
                ws_lag = (avg_received_at - avg_timestamp).total_seconds()
                proc_lag = (avg_insert_time - avg_received_at).total_seconds()
                message_count = len(df)
                
                empty_streak = 0

            ch.insert('websocket_diagnostics',
                [(avg_timestamp, avg_received_at, ws_lag, message_count)],
                column_names=['avg_timestamp', 'avg_received_at', 'avg_websocket_lag', 'message_count'])
            
            ch.insert('processing_diagnostics',
                [(avg_timestamp, avg_received_at, avg_insert_time, proc_lag, message_count)],
                column_names=['avg_timestamp', 'avg_received_at', 'avg_processed_timestamp', 'avg_processing_lag', 'message_count'])
        
            logger.info(f"Inserted diagnostics for {message_count} messages.")

        except Exception as e:
            logger.exception(f"Error inserting diagnostics.")

        time.sleep(duration)