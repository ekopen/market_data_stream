# storage_warm.py
# enables storing warm data to clickhouse

import clickhouse_connect

# def create_warm_table():
#     #creating a table if it does not exist
#     cursor.execute('''
#     CREATE TABLE IF NOT EXISTS price_ticks(
#         timestamp TIMESTAMPTZ,
#         timestamp_ms BIGINT,
#         symbol VARCHAR(255),
#         price FLOAT8,
#         volume FLOAT8,
#         received_at TIMESTAMPTZ
#     )''')

#calling concurrent client function
def get_client():
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='mysecurepassword'
    )

#initialize clickhouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='mysecurepassword',) #TEMPORARY PASSWORD

def create_warm_table():
    #creating a table if it does not exist
    client.command('''
    CREATE TABLE IF NOT EXISTS price_ticks_warm(
        timestamp DateTime,
        timestamp_ms Int64,
        symbol String,
        price Float64,
        volume Float64,
        received_at DateTime
    ) 
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(timestamp)
    ORDER BY timestamp_ms
    TTL timestamp + INTERVAL 560 MINUTE DELETE
    ''')
