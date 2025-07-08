# data_consumption.py
# Kafka consumer that reads the data and then starts appending it to the hot/warm/cold tables
# handles moving from hot to warm to cold as well (via batch processes)

from kafka import KafkaConsumer
import json, time, statistics
from datetime import datetime, timezone
from storage_hot import get_client as get_client_hot
from diagnostics import insert_processing_diagnostics
from config import DIAGNOSTIC_FREQUENCY
import queue
from statistics import mean

#diagnostics tools
diagnostics_queue = queue.Queue()

def processing_diagnostics_worker(cursor, stop_event):
    print("Processing diagnostics worker started.")
    while not stop_event.is_set():
        time.sleep(DIAGNOSTIC_FREQUENCY)

        all_rows = []
        insert_times = []

        while not diagnostics_queue.empty():
            try:
                batch, insert_time = diagnostics_queue.get_nowait()
                all_rows.extend(batch)  # Flatten all rows from each batch
                insert_times.append(insert_time)
            except queue.Empty:
                break

        if not all_rows or not insert_times:
            continue

        received_times = [row[5] for row in all_rows]
        timestamps = [row[0] for row in all_rows]

        avg_timestamp = datetime.fromtimestamp(mean([dt.timestamp() for dt in timestamps]))
        avg_received = datetime.fromtimestamp(mean([dt.timestamp() for dt in received_times]))
        avg_insert_time = datetime.fromtimestamp(mean([dt.timestamp() for dt in insert_times]))
        message_count = len(all_rows)

        insert_processing_diagnostics(
            cursor,
            avg_timestamp,
            avg_received,
            avg_insert_time,
            message_count
        )

        print(f"Inserted processing diagnostics for {message_count} messages.")

# prepares the data for clickhouse
def validate_and_parse(data):

    timestamp_dt = datetime.fromisoformat(data['timestamp']).astimezone(timezone.utc)
    received_at_dt = datetime.fromisoformat(data['received_at']).astimezone(timezone.utc)

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

    ch_client_hot = get_client_hot()

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

        if stop_event.is_set():
            print("Stop event received, breaking consumer loop.")
            break
        try:
            validated_row = validate_and_parse(message.value)
            batch.append(validated_row)

            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) > FLUSH_INTERVAL:
                ch_client_hot.insert('price_ticks_hot', batch)
                insert_time = datetime.utcnow().replace(tzinfo=timezone.utc)

                print(f"Inserted {len(batch)} rows.")

                #insert for diagnostics
                diagnostics_queue.put((batch.copy(), insert_time))
                
                batch.clear()
                last_flush = time.time()

        except Exception as e:
            print("Full exception:", repr(e), e.args)





