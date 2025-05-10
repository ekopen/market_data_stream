#migration.py
#moves data from table to table depending on storage requirements

import time
from datetime import timedelta
from clickhouse import get_client
from postgres import cursor, conn

def hot_to_warm(hot_duration=60):   
    ch_client = get_client() #initiate a new clickhouse client
    while True:
        print("Migrating from hot to cold") 
        try:

            #find most recent timestamp in the hot table
            first_row = ch_client.query('''
                SELECT max(timestamp) FROM price_ticks
            ''').result_rows

            first_timestamp = first_row[0][0]
            cutoff = first_timestamp - timedelta(seconds=hot_duration) #identifies the cutoff time for moving data

            # gets all data past the cutoff time
            old_rows = ch_client.query(f'''
                SELECT * FROM price_ticks
                WHERE timestamp < toDateTime('{cutoff}')
            ''').result_rows

            print("Rows to move:", len(old_rows))

            #inserts the old data in postgres
            insert_query = '''
                    INSERT INTO price_ticks (timestamp, timestamp_ms, symbol, price, volume, received_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                '''
            cursor.executemany(insert_query, old_rows)
            print(f"Moved {len(old_rows)} rows from hot (clickhouse) to warm (postgres).")

            # removes the old data from clickhouse
            ch_client.command(f'''
                ALTER TABLE price_ticks
                DELETE WHERE timestamp < '{cutoff}'
            ''')
            print(f"Deleted {len(old_rows)} rows from hot (clickhouse).")


        except Exception as e:
            print("[hot_to_warm] Exception:", e)

        time.sleep(hot_duration) #move data once a minute