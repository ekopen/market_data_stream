#migration.py
#moves data from table to table depending on storage requirements

import time, os
from datetime import timedelta, datetime, timezone
from storage_hot import get_client as get_client_hot
from storage_warm import get_client as get_client_warm
from old.storage_cold import cold_upload
import pandas as pd
import threading
from old.diagnostics_db import insert_transfer_diagnostics
from pympler import asizeof

stop_event = threading.Event()

def hot_to_warm(stop_event,hot_duration): #duration in seconds   
    time.sleep(hot_duration*2) #pause before beginning the migration

    ch_client_hot = get_client_hot() #initiate a new clickhouse client for hot storage
    ch_client_warm = get_client_warm() #initiate a new clickhouse client for warm storage

    while not stop_event.is_set():
        print("Migrating from hot to warm") 
        try:
            # gets the current time and subtracts the hot duration to get the cutoff time
            cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=hot_duration)
            cutoff_ms = int(cutoff_time.timestamp() * 1000)

            transfer_start_time = datetime.now(timezone.utc)

            # gets all data past the cutoff time
            warm_rows = ch_client_hot.query(f'''
                SELECT * FROM price_ticks_hot
                WHERE timestamp_ms < {cutoff_ms}
            ''').result_rows

            # insert the warm rows into the warm storage table
            ch_client_warm.insert('price_ticks_warm', warm_rows)

            # removes the warm data from clickhouse
            ch_client_hot.command(f'''
                ALTER TABLE price_ticks_hot
                DELETE WHERE timestamp_ms < {cutoff_ms}
            ''')

            print(f"Moved {len(warm_rows)} rows from hot to warm storage.")

            transfer_end_time = datetime.now(timezone.utc)
            message_count = len(warm_rows)
            transfer_size = asizeof.asizeof(warm_rows) / (1024 * 1024) # converting to MB

            insert_transfer_diagnostics("hot_to_warm", transfer_start_time, transfer_end_time, message_count, transfer_size)

        except Exception as e:
            print("[hot_to_warm] Exception:", e)

        time.sleep(hot_duration) #pause before moving more data

def warm_to_cold(stop_event,warm_duration): #duration in seconds   
    time.sleep(warm_duration*2) #pause before beginning the migration

    ch_client_warm = get_client_warm() #initiate a new clickhouse client for warm storage

    while not stop_event.is_set():
        print("Migrating from warm to cold") 
        try:
            # gets the current time and subtracts the warm duration to get the cutoff time
            cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=warm_duration)
            cutoff_ms = int(cutoff_time.timestamp() * 1000)

            print("Cutoff timestamp in ms:", cutoff_ms)

            cold_rows = ch_client_warm.query(f'''
                SELECT * FROM price_ticks_warm
                WHERE timestamp_ms < {cutoff_ms}
            ''').result_rows

            df = pd.DataFrame(cold_rows, columns=[
                'timestamp', 'timestamp_ms', 'symbol', 'price', 'volume', 'received_at'
                ])

            # aggregating to 1 second intervals
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
            df['second'] = df['timestamp'].dt.floor('1s')
            df = df.groupby('second').agg({
                'timestamp_ms': 'last',
                'symbol': 'last',
                'price': 'last',
                'volume': 'sum',
                'received_at': 'last'
            }).reset_index()
            df.rename(columns={'second': 'timestamp'}, inplace=True)

            filename = f'cold_data/{cutoff_ms}.parquet'
            df.to_parquet(filename, index=False)
            s3_key = f"archived_data/{cutoff_ms}"
            
            try:
                # deactivating the actualu upload for now
                # cold_upload(filename, 'cold_storage', s3_key)
                os.remove(filename)
                print(f"Uploaded and removed file: {filename}")
            except Exception as upload_err:
                print(f"[warm_to_cold] Upload failed: {upload_err}")

            # remove the old rows from warm storage
            ch_client_warm.command(f'''
                ALTER TABLE price_ticks_warm
                DELETE WHERE timestamp_ms < {cutoff_ms}
            ''')

            print(f"Moved {len(cold_rows)} rows from warm to cold storage.")

        except Exception as e:
            print("[warm_to_cold] Exception:", e)

        time.sleep(warm_duration) #pause before moving more data

