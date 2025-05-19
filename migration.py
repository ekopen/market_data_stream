#migration.py
#moves data from table to table depending on storage requirements

import time, os
from datetime import timedelta, datetime, timezone
from storage_hot import get_client
from storage_warm import cursor, conn
from storage_cold import cold_upload
import pandas as pd
import threading

stop_event = threading.Event()

def hot_to_warm(stop_event,hot_duration): #duration in seconds   
    time.sleep(hot_duration*2) #pause before beginning the migration
    ch_client = get_client() #initiate a new clickhouse client

    while not stop_event.is_set():
        print("Migrating from hot to warm") 
        try:
            # gets the current time and subtracts the hot duration to get the cutoff time
            cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=hot_duration)
            cutoff_ms = int(cutoff_time.timestamp() * 1000)

            # gets all data past the cutoff time
            warm_rows = ch_client.query(f'''
                SELECT * FROM price_ticks
                WHERE timestamp_ms < {cutoff_ms}
            ''').result_rows

            #inserts the old data in postgres
            insert_query = '''
                    INSERT INTO price_ticks (timestamp, timestamp_ms, symbol, price, volume, received_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                '''
            cursor.executemany(insert_query, warm_rows)

            # removes the warm data from clickhouse
            ch_client.command(f'''
                ALTER TABLE price_ticks
                DELETE WHERE timestamp_ms < {cutoff_ms}
            ''')

            print(f"Moved {len(warm_rows)} rows from hot to warm storage.")

        except Exception as e:
            print("[hot_to_warm] Exception:", e)

        time.sleep(hot_duration) #pause before moving more data

def warm_to_cold(stop_event,warm_duration): #duration in seconds   
    time.sleep(warm_duration*2) #pause before beginning the migration

    while not stop_event.is_set():
        print("Migrating from warm to cold") 
        try:
            # gets the current time and subtracts the warm duration to get the cutoff time
            cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=warm_duration)
            cutoff_ms = int(cutoff_time.timestamp() * 1000)

            print("Cutoff timestamp in ms:", cutoff_ms)

            # get rows older than the cutoff
            cursor.execute('''
                SELECT * FROM price_ticks
                WHERE timestamp_ms < %s
            ''', (cutoff_ms,))
            cold_rows = cursor.fetchall()

            df = pd.DataFrame(cold_rows, columns=['timestamp', 'timestamp_ms', 'symbol', 'price', 'volume', 'received_at'])

            # aggregating to 1 second intervals
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
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

            # remove the old rows from warm storage
            cursor.execute(f'''
                DELETE FROM price_ticks
                WHERE timestamp_ms < %s
            ''', (cutoff_ms,))

            print(f"Moved {len(cold_rows)} rows from warm to cold storage.")

        except Exception as e:
            print("[warm_to_cold] Exception:", e)

        time.sleep(warm_duration) #pause before moving more data

def cold_to_cloud(stop_event,cold_duration): #duration in seconds   
    time.sleep(cold_duration*2) #pause before beginning the migration
    
    while not stop_event.is_set():
        print("Migrating from cold to cloud") 
        # upload all cold data older than the cutoff time to cloud
        try:
            # find cutoff time
            cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=cold_duration)
            cutoff_ms = int(cutoff_time.timestamp() * 1000)

            # loop through every parquet file in the cold_data directory
            for filename in os.listdir('cold_data'):
                file_ms = int(filename.split('.')[0]) #get the cutoff time the file
                print(f"Cold file found: {file_ms}")
                if file_ms < cutoff_ms:
                    full_path = os.path.join('cold_data', filename)
                    print(f"Full path: {full_path}")
                    s3_key = f"archived_data/{full_path}"
                    cold_upload(full_path, 'cold_storage', s3_key)
                    os.remove(full_path)
                    print(f"Uploaded and removed file: {filename}")

        except Exception as e:
            print("[cold_to_cloud] Exception:", e)

        time.sleep(cold_duration)
