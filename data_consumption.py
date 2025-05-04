# Kafka consumer that reads the data
from kafka import KafkaConsumer
import json

def start_consumer():
    consumer = KafkaConsumer(
        'price_ticks',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        print("Received:", message.value)
