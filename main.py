# main.py
# calls our Kafka producer and consumer to being the data collection process

import threading
from data_ingestion import start_producer
from data_consumption import start_consumer

API_KEY = 'd0amcgpr01qm3l9meas0d0amcgpr01qm3l9measg'
SYMBOL = 'BINANCE:BTCUSDT'

if __name__ == "__main__":
    #using threading, as we need the producer to run in the background as we consume and store data
    producer_thread = threading.Thread(target=start_producer, args=(SYMBOL, API_KEY))
    producer_thread.daemon = True
    producer_thread.start()

    print("Producer thread started.")

    # start consumer
    start_consumer()
