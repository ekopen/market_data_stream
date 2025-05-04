# data_ingestion.py
# starts the kafka producer and integrates it with Finnhub's API/Websocket

from kafka import KafkaProducer
import json, websocket, atexit
from datetime import datetime, timezone

API_KEY = 'd0amcgpr01qm3l9meas0d0amcgpr01qm3l9measg'
SYMBOL = 'BINANCE:ETHUSDT'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def close_producer():
    print("Closing Kafka producer...")
    producer.close()
atexit.register(close_producer)

def start_producer():
    def on_message(ws, message):
        data = json.loads(message)
        if data.get('type') == 'trade':
            received_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
            for t in data['data']:
                trade_time = datetime.fromtimestamp(t['t'] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
                payload = {
                    'timestamp': trade_time,
                    'timestamp_ms': t['t'],
                    'symbol': t['s'],
                    'price': t['p'],
                    'volume': t['v'],
                    'received_at': received_at
                }
                producer.send('price_ticks', payload)
                # print("Sent:", payload)

    def on_open(ws):
        print("WebSocket connected")
        ws.send(json.dumps({"type": "subscribe", "symbol": SYMBOL}))

    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={API_KEY}",
                                on_message=on_message,
                                on_open=on_open)
    ws.run_forever()