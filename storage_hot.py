# storage_hot.py
# enables storing hot data to clikhouse
import clickhouse_connect

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

def create_hot_table():
    client.command("DROP TABLE IF EXISTS price_ticks_hot")
    client.command('''
    CREATE TABLE IF NOT EXISTS price_ticks_hot(
        timestamp DateTime,
        timestamp_ms Int64,
        symbol String,
        price Float64,
        volume Float64,
        received_at DateTime('UTC')
    ) 
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(timestamp)
    ORDER BY timestamp_ms
    TTL timestamp + INTERVAL 10 MINUTE DELETE
    ''')

