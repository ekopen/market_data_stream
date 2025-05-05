# data_consumption.py
# Kafka consumer that reads the data and then appends to SQLite table
from kafka import KafkaConsumer
import json
import sqlite3

#initialize the SQL database
conn = sqlite3.connect("db/pricing_data.db", check_same_thread=False)
c = conn.cursor()

c.execute('''
CREATE TABLE IF NOT EXISTS price_ticks 
        (
        timestamp TEXT,
        timestamp_ms INTEGER,
        symbol TEXT,
        price REAL,
        volume REAL,
        received_at TEXT
        ) 
''')

c.execute("DELETE FROM price_ticks")

def start_consumer():
    consumer = KafkaConsumer(
        'price_ticks', #connecting to our price ticks topic
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("Kafka consumer connected. Waiting for messages...")

    for message in consumer:
        data = message.value #get the message output
        try: #attempt to insert into the SQL table
            c.execute('''
                INSERT INTO price_ticks (timestamp, timestamp_ms, symbol, price, volume, received_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                data['timestamp'],
                data['timestamp_ms'],
                data['symbol'],
                data['price'],
                data['volume'],
                data['received_at']
            ))
            conn.commit()
            print("Appended to database", data)
        except Exception as e: #backup message in vase of error
            print("Error inserting:", e)



