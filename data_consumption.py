# data_consumption.py
# Kafka consumer that reads the data and then appends to a clickhouse table
from kafka import KafkaConsumer
import json
import clickhouse_connect
from datetime import datetime


def parse_clickhouse_datetime(dt_str):
    # Strip the ' UTC' suffix and convert to datetime object
    return datetime.strptime(dt_str.replace(' UTC', ''), "%Y-%m-%d %H:%M:%S")

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

#deleting existing rows
client.command('TRUNCATE TABLE price_ticks')       

def start_consumer():
    consumer = KafkaConsumer(
        'price_ticks', #connecting to our price ticks topic
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("Kafka consumer connected. Waiting for messages...")

    for message in consumer:
            data = message.value
            try:
                parsed_data = {
                    'timestamp': parse_clickhouse_datetime(data['timestamp']),
                    'timestamp_ms': int(data['timestamp_ms']),
                    'symbol': data['symbol'],
                    'price': float(data['price']),
                    'volume': float(data['volume']),
                    'received_at': parse_clickhouse_datetime(data['received_at']),
                }

                print("Data types being inserted:", {k: type(v) for k, v in parsed_data.items()})
                client.insert('price_ticks', [parsed_data])
                print("Appended to Clickhouse", parsed_data)

            except Exception as e:
                print("Error inserting into Clickhouse:", e)

