#migration.py
#moves data from table to table depending on storage requirements

import time
from datetime import timedelta, datetime
from clickhouse import get_client
from postgres import cursor, conn

def hot_to_warm(hot_duration=30):   
    ch_client = get_client() #initiate a new clickhouse client
    while True:
        print("Migrating from hot to cold") 
        try:
            #find most recent timestamp in the hot table

            first_row = ch_client.query('''
                SELECT max(timestamp_ms) FROM price_ticks
            ''').result_rows

            first_timestamp_ms = first_row[0][0]
            cutoff_ms = first_timestamp_ms - (hot_duration * 1000)

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

        time.sleep(hot_duration) #pase before moving more data