# initialize_db.py
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

def cold_upload(file_name=None, bucket=BUCKET_NAME, s3_key=None):
    s3.upload_file(file_name, BUCKET_NAME, s3_key)
    print(f"Uploaded {file_name} to S3 bucket '{BUCKET_NAME}' at '{s3_key}'.")

def cold_storage(stop_event,duration): #duration in seconds   
    time.sleep(duration*2) #pause before beginning the migration

    ch_client = new_client() 

    while not stop_event.is_set():
        print("Migrating to cold storage") 
        try:
            # gets the current time and subtracts the  duration to get the cutoff time
            cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=duration)
            cutoff_ms = int(cutoff_time.timestamp() * 1000)

            print("Cutoff timestamp in ms:", cutoff_ms)

            cold_rows = ch_client.query(f'''
                SELECT * FROM ticks_db
                WHERE timestamp_ms < {cutoff_ms}
            ''').result_rows

            df = pd.DataFrame(cold_rows, columns=[
                'timestamp', 'timestamp_ms', 'symbol', 'price', 'volume', 'received_at', 'insert_time'
                ])

            # aggregating to 1 second intervals
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
            df['second'] = df['timestamp'].dt.floor('1s')
            df = df.groupby('second').agg({
                'timestamp_ms': 'last',
                'symbol': 'last',
                'price': 'last',
                'volume': 'sum',
                'received_at': 'last',
                'insert_time': 'last'
            }).reset_index()
            df.rename(columns={'second': 'timestamp'}, inplace=True)

            filename = f'cold_data/{cutoff_ms}.parquet'
            df.to_parquet(filename, index=False)
            s3_key = f"archived_data/{cutoff_ms}"
            
            try:
                # deactivating the actual upload for now
                # cold_upload(filename, 'cold_storage', s3_key)
                os.remove(filename)
                print(f"Uploaded and removed file: {filename}")
            except Exception as upload_err:
                print(f"[cold_storage] Upload failed: {upload_err}")

            # remove the old rows from ticks_db
            ch_client.command(f'''
                ALTER TABLE ticks_db
                DELETE WHERE timestamp_ms < {cutoff_ms}
            ''')

            print(f"Moved {len(cold_rows)} rows to cold storage.")

        except Exception as e:
            print("[cold_storage] Exception:", e)

        time.sleep(duration) #pause before moving more data