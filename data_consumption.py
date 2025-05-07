# data_consumption.py
# Kafka consumer that reads the data and then appends to a clickhouse table
from kafka import KafkaConsumer
import json
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
        data = message.value #get the message output
        try: #attempt to insert into clickhouse
            print("Received message for clickhouse:", data)
            client.insert('price_ticks', [{
                'timestamp': data['timestamp'].replace(' UTC', ''),
                'timestamp_ms': data['timestamp_ms'],
                'symbol': data['symbol'],
                'price': data['price'],
                'volume': data['volume'],
                'received_at': data['received_at'].replace(' UTC', '')
            }]) #insert into the table

            print("Appended to Clickhouse", data)
        except Exception as e: #backup message in vase of error
            print("Error inserting into Clickhouse:", e)



