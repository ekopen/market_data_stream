# data_ingestion.py
# starts the kafka producer and integrates it with Finnhub's API/Websocket

from kafka import KafkaProducer
import json, websocket, atexit, time
from datetime import datetime, timezone
import threading


import logging
logger = logging.getLogger("producer")

# producer class
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=5,
    retries=1,
    request_timeout_ms=2000,
    max_block_ms=2000
)


def start_producer(SYMBOL, API_KEY, stop_event):

    logger.info("Producer thread started.")

    def on_message(ws, message):
        data = json.loads(message)
        if data.get('type') == 'trade': #checks to make sure the data is trade data
            for t in data['data']:
                trade_time = datetime.fromtimestamp(t['t'] / 1000, tz=timezone.utc)
                received_at = datetime.now(timezone.utc)

                # schema for passing to Kafka (even though technically Kafka is schemless)
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
        logger.info("WebSocket connected")
        ws.send(json.dumps({"type": "subscribe", "symbol": SYMBOL}))

    def on_close(ws, close_status_code, close_msg):
        logger.info("WebSocket closed:", close_status_code, close_msg)

    def on_error(ws, err):
        logger.exception(f"WebSocket error: {err}")

    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={API_KEY}",
                                on_message=on_message,
                                on_open=on_open,
                                on_close=on_close)

    # start the producer in thread
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()

    try:
        while not stop_event.is_set():
            time.sleep(0.1)
    finally:
        logger.info("Shutting down producer.")
        try:
            ws.close()
        except Exception:
            pass
        try:
            producer.flush(timeout=2)
        except Exception:
            pass
        try:
            producer.close(timeout=2)
        except Exception:
            pass

    
