# main.py
# controls all sub functions

from data_ingestion import start_producer
from data_consumption import start_consumer
from storage_hot import create_hot_table, delete_hot_table
from storage_warm import create_warm_table, cursor, delete_warm_table
from migration import hot_to_warm, warm_to_cold
from diagnostics import create_consumer_metrics_table, create_producer_metrics_table, create_system_errors_table

from config import SYMBOL, API_KEY, HOT_DURATION, WARM_DURATION, COLD_DURATION

import os, threading, time, sys
from dotenv import load_dotenv
load_dotenv()  # Load from .env file

stop_event = threading.Event()

if "--stop" in sys.argv:
    stop_event.set()
    exit(0)

if __name__ == "__main__":
    try:
        #create the tables if they do not exist
        delete_hot_table()
        delete_warm_table()

        create_hot_table()
        create_warm_table()
        create_consumer_metrics_table(cursor)
        create_producer_metrics_table(cursor)
        create_system_errors_table(cursor)

        #start migrating data between tables
        threading.Thread(target=hot_to_warm, args=(stop_event,HOT_DURATION), daemon=True).start() #start the hot to warm thread
        threading.Thread(target=warm_to_cold, args=(stop_event,WARM_DURATION), daemon=True).start() #start the warm to cold thread

        #start ingesting data from the websocket and feed to kafka
        producer_thread = threading.Thread(target=start_producer, args=(SYMBOL, API_KEY, stop_event))
        producer_thread.daemon = True
        producer_thread.start()

        #start processing data from kakfa and store to tables
        consumer_thread = threading.Thread(target=start_consumer, args=(stop_event,))
        consumer_thread.daemon = True
        consumer_thread.start()

        while not stop_event.is_set():
            time.sleep(1)

    except KeyboardInterrupt:
        print("Keyboard interrupt received. Exiting...")
    # Let atexit handle closing Kafka producer
        stop_event.set()

