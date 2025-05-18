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

def hot_to_warm(stop_event,hot_duration=60): #duration in seconds   
    time.sleep(hot_duration*2) #pause before beginning the migration
    ch_client = get_client() #initiate a new clickhouse client
    os.makedirs("data", exist_ok=True)

    while not stop_event.is_set():
        print("Migrating from hot to cold") 
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

            # get the new hot table
            hot_rows = ch_client.query(f'''
                SELECT * FROM price_ticks
            ''').result_rows

            print(f"Moved {len(warm_rows)} rows from hot to warm storage. There are {len(hot_rows)} rows remaining in the hot table.")

            #export hot rows to parquet file
            hot_df = pd.DataFrame(hot_rows, columns=['timestamp', 'timestamp_ms', 'symbol', 'price', 'volume', 'received_at'])
            hot_df.to_parquet("data/hot_data.parquet", index=False)


        except Exception as e:
            print("[hot_to_warm] Exception:", e)

        time.sleep(hot_duration) #pause before moving more data

def warm_to_cold(stop_event,warm_duration=300): #duration in seconds   
    time.sleep(warm_duration*2) #pause before beginning the migration
    last_local_file = None
    while not stop_event.is_set():
        print("Migrating from warm to hot") 
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
            df['second'] = df['timestamp'].dt.floor('1s')
            df = df.groupby('second').agg({
                'timestamp_ms': 'last',
                'symbol': 'last',
                'price': 'last',
                'volume': 'sum',
                'received_at': 'last'
            }).reset_index()
            df.rename(columns={'second': 'timestamp'}, inplace=True)

            filename = f'data/cold_data_until_{cutoff_ms}.parquet'
            viz_filename = 'data/cold_data.parquet'
            df.to_parquet(filename, index=False)
            df.to_parquet(viz_filename, index=False) #dup cold data for viz

            #get rid of the of cold file
            if last_local_file and os.path.exists(last_local_file):
                s3_key = f"archived_data/{os.path.basename(last_local_file)}"
                cold_upload(last_local_file, 'cold_data', s3_key)
                os.remove(last_local_file)
                print(f"Archived and deleted old local file: {last_local_file}")

            last_local_file = filename

            # remove the old rows from warm storage
            cursor.execute(f'''
                DELETE FROM price_ticks
                WHERE timestamp_ms < %s
            ''', (cutoff_ms,))

            # get the new warm table
            cursor.execute('''
                SELECT * FROM price_ticks
            ''')
            warm_rows = cursor.fetchall()

            print(f"Moved {len(cold_rows)} rows from warm to cold storage. There are {len(warm_rows)} rows remaining in the warm table.")

            #export warm rows to parquet file
            warm_df = pd.DataFrame(warm_rows, columns=['timestamp', 'timestamp_ms', 'symbol', 'price', 'volume', 'received_at'])
            warm_df.to_parquet("data/warm_data.parquet", index=False)

        except Exception as e:
            print("[warm_to_cold] Exception:", e)

        time.sleep(warm_duration) #pause before moving more data
