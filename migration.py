#migration.py

import time
from datetime import datetime, timedelta, timezone
from clickhouse import get_client

def hot_to_warm():   
    ch_client = get_client()
    while True:
        print("Migrating from hot to cold") 
        try:

            first_row = ch_client.query('''
                SELECT max(timestamp) FROM price_ticks
            ''').result_rows

            first_timestamp = first_row[0][0]

            cutoff = first_timestamp - timedelta(seconds=5)


            old_rows = ch_client.query(f'''
                SELECT * FROM price_ticks
                WHERE timestamp < toDateTime('{cutoff}')
            ''').result_rows

            print("Rows to move:", len(old_rows))

        except Exception as e:
            print("[hot_to_warm] Exception:", e)

        time.sleep(1)


# def hot_to_warm():

#     hot_client = clickhouse_connect.get_client(
#     host='localhost',
#     port=8123,
#     username='default',
#     password='mysecurepassword'
#     )
#     while True:
#         try:
#             # Move data from hot to warm (clickhouse to postgres)
#             cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=5)
#             formatted_cutoff = cutoff_time.strftime('%Y-%m-%d %H:%M:%S')

#             # get hot data from clickhouse
#             rows = hot_client.query(
#                 f'''
#                 SELECT * FROM price_ticks
#                 WHERE timestamp < '{formatted_cutoff}'
#                 '''
#             ).result_rows

#             if not rows:
#                 print("No rows to move from hot to warm.")
#             else:
#                 # insert hot data into postgres
#                 insert_query = '''
#                     INSERT INTO price_ticks (timestamp, timestamp_ms, symbol, price, volume, received_at)
#                     VALUES (%s, %s, %s, %s, %s, %s)
#                 '''
#                 cursor.executemany(insert_query, rows)
#                 print(f"Moved {len(rows)} rows from hot (clickhouse) to warm (postgres).")

#                 #delete the moved data from clickhouse
#                 hot_client.command(f'''
#                     ALTER TABLE price_ticks
#                     DELETE WHERE timestamp < '{formatted_cutoff}'
#                 ''')
#                 print(f"Deleted {len(rows)} rows from hot (clickhouse).")
    
#         except Exception as e:
#             print("[hot_to_warm] Exception:", e)

#         time.sleep(60)