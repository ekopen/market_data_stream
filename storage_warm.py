# storage_warm.py
# enables storing warm data to clickhouse

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
client = get_client()

def create_warm_table():
    client.command("DROP TABLE IF EXISTS price_ticks_warm")
    client.command('''
    CREATE TABLE IF NOT EXISTS price_ticks_warm(
        timestamp       DateTime64(3, 'UTC'),
        timestamp_ms    Int64,
        symbol          String,
        price           Float64,
        volume          Float64,
        received_at     DateTime64(3, 'UTC')
    ) 
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(timestamp)
    ORDER BY timestamp_ms
    TTL toDateTime(timestamp) + INTERVAL 560 MINUTE DELETE
    ''')
