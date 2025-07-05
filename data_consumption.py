# data_consumption.py
# Kafka consumer that reads the data and then starts appending it to the hot/warm/cold tables
# handles moving from hot to warm to cold as well (via batch processes)
from kafka import KafkaConsumer
import json
from datetime import datetime, timezone
from storage_hot import get_client
import threading
import time
import statistics
from diagnostics import insert_consumer_metric, cursor


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

def start_consumer(stop_event):

    # creating variables for consumer metrics
    message_count = 0
    lag_list = []
    last_log_time = time.time()

    ch_client = get_client() #get client

    consumer = KafkaConsumer(
        'price_ticks', #connecting to our price ticks topic
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("Kafka consumer connected. Waiting for messages...")

    #using batching to improve performance
    batch = []
    last_flush = time.time()
    BATCH_SIZE = 300
    FLUSH_INTERVAL = 1.5  # seconds

    for message in consumer:

        #logging conusmer metrics
        timestamp_dt = datetime.fromisoformat(message.value['timestamp'])
        received_at_dt = datetime.now(timezone.utc)
        lag = (received_at_dt - timestamp_dt).total_seconds()
        message_count += 1
        lag_list.append(lag)

        if stop_event.is_set():
            print("Stop event received, breaking consumer loop.")
            break
        try:
            validated_row = validate_and_parse(message.value)
            batch.append(validated_row)

            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) > FLUSH_INTERVAL:
                ch_client.insert('price_ticks_hot', batch)
                print(f"Inserted {len(batch)} rows.")
                batch.clear()
                last_flush = time.time()

        except Exception as e:
            print("Full exception:", repr(e), e.args)

        # flush ingestion metrics every 60 seconds
        if time.time() - last_log_time > 60:
            avg_lag = statistics.mean(lag_list) if lag_list else 0
            max_lag = max(lag_list) if lag_list else 0

            insert_consumer_metric(cursor, message_count, avg_lag, max_lag)

            message_count = 0
            lag_list = []
            last_log_time = time.time()




