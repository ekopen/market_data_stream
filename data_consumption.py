# data_consumption.py
# Kafka consumer that reads the data and then appends to a clickhouse table
from kafka import KafkaConsumer
import json
import clickhouse_connect
from datetime import datetime, timezone

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

def validate_and_parse(data):

    timestamp_dt = datetime.fromisoformat(data['timestamp'])
    timestamp_dt = timestamp_dt.astimezone(timezone.utc).replace(tzinfo=None)

    received_at_dt = datetime.fromisoformat(data['received_at'])
    received_at_dt = received_at_dt.astimezone(timezone.utc).replace(tzinfo=None)

    # return a tuple in the exact order of table schema:
    return (
        timestamp_dt,                  # DateTime
        data['timestamp_ms'],          # Int64
        data['symbol'],                # String
        float(data['price']),          # Float64
        float(data['volume']),         # Float64
        received_at_dt                 # DateTime
    )


def start_consumer():
    consumer = KafkaConsumer(
        'price_ticks', #connecting to our price ticks topic
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("Kafka consumer connected. Waiting for messages...")

    for message in consumer:
        data = message.value
        print("Received message for ClickHouse:", data)

        try:
            validated_row = validate_and_parse(data)
            client.insert('price_ticks', [validated_row])
            print("Appended to ClickHouse:", validated_row)

        except Exception as e:
            print("Full exception:", repr(e), e.args)
            raise



