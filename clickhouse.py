# clickhouse.py
# enables storing hot data to clikhouse
import clickhouse_connect

#initialize clickhouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='mysecurepassword',) #TEMPORARY PASSWORD

#creating a table if it does not exist
client.command('''
CREATE TABLE IF NOT EXISTS price_ticks(
    timestamp DateTime,
    timestamp_ms Int64,
    symbol String,
    price Float64,
    volume Float64,
    received_at DateTime
) 
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, symbol)
TTL timestamp + INTERVAL 2 HOUR DELETE
''')

#deleting existing rows (EVENTUALLY GET RID OF THIS)
client.command('TRUNCATE TABLE price_ticks') 