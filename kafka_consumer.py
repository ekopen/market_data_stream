# data_consumption.py
# Kafka consumer that reads the data and then starts appending it to the ticks db

from kafka import KafkaConsumer
import json, time
from datetime import datetime, timezone
from clickhouse import new_client

import logging
logger = logging.getLogger("consumer")

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

    ch_client = new_client()

    consumer = KafkaConsumer(
        'price_ticks', #connecting to our price ticks topic
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True,
        consumer_timeout_ms=1000  # makes poll() return periodically
    )
    logger.info("Kafka consumer started.")

    #using batching to improve performance
    batch = []
    last_flush = time.time()
    BATCH_SIZE = 300
    FLUSH_INTERVAL = 1.5  # seconds

    try:
        while not stop_event.is_set():
            records = consumer.poll(timeout_ms=500)
            if not records:
                continue

            for tp, messages in records.items():
                for message in messages:
                    try:
                        validated_row = validate_and_parse(message.value)
                        batch.append(validated_row)

                        if len(batch) >= BATCH_SIZE or (time.time() - last_flush) > FLUSH_INTERVAL:
                            ch_client.insert(
                                'ticks_db',
                                batch,
                                column_names=['timestamp','timestamp_ms','symbol','price','volume','received_at']
                            )
                            logger.info(f"Inserted {len(batch)} rows.")
                            batch.clear()
                            last_flush = time.time()
                    except Exception as e:
                        logger.exception("Consumer parse/insert error")
    finally:
        try:
            if batch:
                ch_client.insert(
                    'ticks_db',
                    batch,
                    column_names=['timestamp','timestamp_ms','symbol','price','volume','received_at']
                )
                logger.info(f"Inserted final {len(batch)} rows.")
        except Exception:
            logger.exception("Final batch insert failed")
        logger.info("Closing consumer.")
        try:
            consumer.close()
        except Exception:
            pass






