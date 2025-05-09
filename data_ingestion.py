# data_ingestion.py
# starts the kafka producer and integrates it with Finnhub's API/Websocket

from kafka import KafkaProducer
import json, websocket, atexit, time
from datetime import datetime, timezone

# producer class
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# producer close function
def close_producer():
    print("Closing Kafka producer...")
    producer.close()
atexit.register(close_producer) #ensures a complete closing

def start_producer(SYMBOL, API_KEY):
    last_sent_time = [0]

    def on_message(ws, message):
        data = json.loads(message)
        if data.get('type') == 'trade': #checks to make sure the data is trade data

            for t in data['data']:
                trade_time = datetime.fromtimestamp(t['t'] / 1000, tz=timezone.utc)
                received_at = datetime.now(timezone.utc)
                # schema for passing to Kafka (even though technically Kafka is schemaless)
                payload = {
                    'timestamp': trade_time.isoformat(),
                    'timestamp_ms': t['t'],
                    'symbol': t['s'],
                    'price': t['p'],
                    'volume': t['v'],
                    'received_at': received_at.isoformat()
                }
                #print("Sending payload to Kafka:", payload)
                producer.send('price_ticks', payload) #sends to the Kafka price_ticks topic

    #the rest of this code initializes the websocket
    def on_open(ws):
        print("WebSocket connected")
        ws.send(json.dumps({"type": "subscribe", "symbol": SYMBOL}))

    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={API_KEY}",
                                on_message=on_message,
                                on_open=on_open)
    ws.run_forever()