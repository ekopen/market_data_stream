#migration.py
#moves data from table to table depending on storage requirements

import time
from datetime import timedelta, datetime, timezone
from storage_hot import get_client
from storage_warm import cursor, conn
from storage_cold import cold_upload
import pandas as pd

def hot_to_warm(hot_duration=20): #duration in seconds   
    time.sleep(hot_duration) #pause before beginning the migration
    ch_client = get_client() #initiate a new clickhouse client
    while True:
        print("Migrating from hot to cold") 
        try:
            #using the api stream as an anchor for data "freshness", since it seems to be outdated data
            first_row = ch_client.query('''
                SELECT max(timestamp_ms) FROM price_ticks
            ''').result_rows

            first_timestamp_ms = first_row[0][0]
            cutoff_ms = first_timestamp_ms - (hot_duration * 1000)

            # if you want to use the current time as the cutoff, uncomment this
            # cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=hot_duration)
            # cutoff_ms = int(cutoff_time.timestamp() * 1000)

            # gets all data past the cutoff time
            old_rows = ch_client.query(f'''
                SELECT * FROM price_ticks
                WHERE timestamp_ms < {cutoff_ms}
            ''').result_rows

            #inserts the old data in postgres
            insert_query = '''
                    INSERT INTO price_ticks (timestamp, timestamp_ms, symbol, price, volume, received_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                '''
            cursor.executemany(insert_query, old_rows)

            # removes the old data from clickhouse
            ch_client.command(f'''
                ALTER TABLE price_ticks
                DELETE WHERE timestamp_ms < {cutoff_ms}
            ''')

            remaining_rows = ch_client.query(f'''
                SELECT * FROM price_ticks
            ''').result_rows

            print(f"Moved {len(old_rows)} rows from hot to warm storage. There are {len(remaining_rows)} rows remaining in the hot table.")

        except Exception as e:
            print("[hot_to_warm] Exception:", e)

        time.sleep(hot_duration) #pause before moving more data

def warm_to_cold(warm_duration=45): #duration in seconds   
    time.sleep(warm_duration) #pause before beginning the migration

    while True:
        print("Migrating from warm to hot") 
        try:
            # get the first timestamp from the warm storage to serve as an anchor for the cutoff
            cursor.execute('''
                SELECT max(timestamp_ms) FROM price_ticks
            ''')
            first_row = cursor.fetchone()
            first_timestamp_ms = first_row[0]

            cutoff_ms = first_timestamp_ms - (warm_duration * 1000)

            # get rows older than the cutoff
            cursor.execute(f'''
                SELECT * FROM price_ticks
                WHERE timestamp_ms < {cutoff_ms}
            ''', (cutoff_ms))
            old_rows = cursor.fetchall()

            df = pd.DataFrame(old_rows, columns=['timestamp', 'timestamp_ms', 'symbol', 'price', 'volume', 'received_at'])

            filename = f'cold_storage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet'
            df.to_parquet(filename, index=False)
            s3_key = f"archived_data/{filename}"

            # upload to cold storage
            cold_upload(filename, 'cold_storage',s3_key)

            # remove the old rows from warm storage
            cursor.execute(f'''
                DELETE FROM price_ticks
                WHERE timestamp_ms < {cutoff_ms}
            ''', (cutoff_ms))
            print(f"Moved {len(old_rows)} rows from warm to cold storage. There are {cursor.rowcount} rows remaining in the warm table.")

        except Exception as e:
            print("[warm_to_cold] Exception:", e)

        time.sleep(warm_duration) #pause before moving more data