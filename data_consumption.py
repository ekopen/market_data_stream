# data_consumption.py
# Kafka consumer that reads the data and then starts appending it to the hot/warm/cold tables
# handles moving from hot to warm to cold as well (via batch processes)
from kafka import KafkaConsumer
import json
from datetime import datetime, timezone
from storage_hot import get_client
from storage_warm import cursor, conn
import threading

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

    ch_client = get_client() #get client

    consumer = KafkaConsumer(
        'price_ticks', #connecting to our price ticks topic
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("Kafka consumer connected. Waiting for messages...")

    for message in consumer:
        data = message.value
        #print("Received message:", data)

        try:
            validated_row = validate_and_parse(data)
            ch_client.insert('price_ticks', [validated_row])
            #print("Appended to ClickHouse:", validated_row)

        except Exception as e:
            print("Full exception:", repr(e), e.args)
            raise




