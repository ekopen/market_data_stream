# db_storage.py
# this module handles the migration of old data to cloud/storage

from clickhouse import new_client
import time, os
from datetime import timedelta, datetime, timezone
import pandas as pd
import threading
import boto3, os
from dotenv import load_dotenv
load_dotenv()  # Load from .env file

import logging
logger = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

s3 = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

def clickhouse_to_cloud(stop_event,duration): 

    time.sleep(duration*2) #pause before beginning the migration to let data populate

    ch_client = new_client() 

    while not stop_event.is_set():
        logger.debug("Starting migration cycle.") 
        try:          

            cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=duration)
            cutoff_ms = int(cutoff_time.timestamp() * 1000)

            #--------------------------ticks_db--------------------------#

            # ticks_db data
            old_ticks = ch_client.query(f'''
                SELECT * FROM ticks_db
                WHERE timestamp_ms < {cutoff_ms}
            ''').result_rows
            old_ticks_df = pd.DataFrame(old_ticks, columns=[
                'timestamp', 'timestamp_ms', 'symbol', 'price', 'volume', 'received_at', 'insert_time'
                ])

            # aggregating to 1 second intervals for reduced data size
            old_ticks_df['timestamp'] = pd.to_datetime(old_ticks_df['timestamp'], errors='coerce', utc=True)
            old_ticks_df['second'] = old_ticks_df['timestamp'].dt.floor('1s')
            old_ticks_df = old_ticks_df.groupby('second').agg({
                'timestamp_ms': 'last',
                'symbol': 'last',
                'price': 'last',
                'volume': 'sum',
                'received_at': 'last',
                'insert_time': 'last'
            }).reset_index()
            old_ticks_df.rename(columns={'second': 'timestamp'}, inplace=True)

            filename = f'parquet_data/ticks_{cutoff_ms}.parquet'
            logger.info(f"Written Parquet file: {filename}")
            old_ticks_df.to_parquet(filename, index=False)
            ch_client.command(f'''
                ALTER TABLE ticks_db
                DELETE WHERE timestamp_ms < {cutoff_ms}
            ''')
            logger.debug("Deleted migrated records from ticks_db.")

            #--------------------------diagnostics_db--------------------------#

            # websocket diagnostics data
            old_ws = ch_client.query(f'''
                SELECT * FROM websocket_diagnostics
                WHERE toUnixTimestamp64Milli(diagnostics_timestamp) < {cutoff_ms}
            ''').result_rows
            ws_df = pd.DataFrame(old_ws, columns=[
                'avg_timestamp', 'avg_received_at', 'avg_websocket_lag', 'message_count', 'diagnostics_timestamp'
                ])
            filename = f'parquet_data/ws_diagnostics_{cutoff_ms}.parquet'
            ws_df.to_parquet(filename, index=False)
            logger.info(f"Written Parquet file: {filename}")
            ch_client.command(f'''
                ALTER TABLE websocket_diagnostics
                DELETE WHERE toUnixTimestamp64Milli(diagnostics_timestamp) < {cutoff_ms}
            ''')
            logger.debug("Deleted migrated records from websocket_diagnostics.")

            # processing diagnostics data
            old_proc = ch_client.query(f'''
                SELECT * FROM processing_diagnostics
                WHERE toUnixTimestamp64Milli(diagnostics_timestamp) < {cutoff_ms}
            ''').result_rows
            proc_df = pd.DataFrame(old_proc, columns=[
                'avg_timestamp', 'avg_received_at', 'avg_processed_timestamp', 'avg_processing_lag', 'message_count', 'diagnostics_timestamp'
                ])
            filename = f'parquet_data/proc_diagnostics_{cutoff_ms}.parquet'
            logger.info(f"Written Parquet file: {filename}")
            proc_df.to_parquet(filename, index=False)
            ch_client.command(f'''
                ALTER TABLE processing_diagnostics
                DELETE WHERE toUnixTimestamp64Milli(diagnostics_timestamp) < {cutoff_ms}
            ''')
            logger.debug("Deleted migrated records from processing_diagnostics.")

            #--------------------------cloud upload--------------------------#

            for parquet in os.listdir('parquet_data/'):
                file_name = os.path.join('parquet_data/', parquet)
                s3_key = f"archived_data/{parquet}"
                s3.upload_file(file_name, BUCKET_NAME, s3_key)
                print(f"Uploaded {file_name} to S3 bucket '{BUCKET_NAME}' at '{s3_key}'.")
                os.remove(file_name)
                logger.info(f"Uploaded {file_name} to S3 bucket {BUCKET_NAME} as {s3_key} and removed local file.")

        except Exception as e:
            logger.exception("Error during clickhouse_to_cloud")

        time.sleep(duration) #pause before rerunning
