# data_ingestion.py
# starts the kafka producer and integrates it with Finnhub's API/Websocket

from kafka import KafkaProducer
import json, websocket, atexit, time
from datetime import datetime, timezone
import threading
from diagnostics import insert_producer_metric

# queue for producer metrics
from queue import Queue 
counter_queue = Queue()

def counter_worker():
    count = 0
    last_log_time = time.time()

    while True:
        item = counter_queue.get()
        if item is None:
            break  # Graceful shutdown
        count += 1
        if time.time() - last_log_time > 60:
            print(f"[Producer] Sent {count} messages in last 60s")
            count = 0
            last_log_time = time.time()

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

def start_producer(SYMBOL, API_KEY, stop_event):

    print("Producer thread started.")

    # start a thread for the metrics counter
    metrics_thread = threading.Thread(target=counter_worker, daemon=True)
    metrics_thread.start()
    
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

                counter_queue.put(1)

    #the rest of this code initializes the websocket
    def on_open(ws):
        print("WebSocket connected")
        ws.send(json.dumps({"type": "subscribe", "symbol": SYMBOL}))

    def on_close(ws, close_status_code, close_msg):
        print("WebSocket closed:", close_status_code, close_msg)

    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={API_KEY}",
                                on_message=on_message,
                                on_open=on_open,
                                on_close=on_close)

    # start the producer in a separate thread
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()

    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        print("Shutting down producer WebSocket.")
        ws.close()

        # additionally stop logging thread
        counter_queue.put(None)
        metrics_thread.join()