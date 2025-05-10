# main.py
# calls our Kafka producer and consumer to being the data collection process

import threading
import time
from clickhouse import create_clickhouse_table
from postgres import create_postgres_table
from migration import hot_to_warm
from data_ingestion import start_producer
from data_consumption import start_consumer

API_KEY = 'd0amcgpr01qm3l9meas0d0amcgpr01qm3l9measg'
SYMBOL = 'BINANCE:ETHUSDT'

if __name__ == "__main__":
    try:
        #create the tables if they do not exist
        create_clickhouse_table()
        create_postgres_table()

        #start migrating data between tables
        threading.Thread(target=hot_to_warm, daemon=True).start() #start the hot to warm thread

        #start ingesting data from the websocket and feed to kafka
        producer_thread = threading.Thread(target=start_producer, args=(SYMBOL, API_KEY))
        producer_thread.daemon = True
        producer_thread.start()

        #start processing data from kakfa and store to tables
        start_consumer()

    except KeyboardInterrupt:
        print("Keyboard interrupt received. Exiting...")
    # Let atexit handle closing Kafka producer
        exit(0)
