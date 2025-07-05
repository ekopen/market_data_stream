# main.py
# controls all sub functions

from data_ingestion import start_producer
from data_consumption import start_consumer
from storage_hot import create_hot_table
from storage_warm import create_warm_table, cursor
from migration import hot_to_warm, warm_to_cold, cold_to_cloud
from diagnostics import create_consumer_metrics_table, create_producer_metrics_table, create_system_errors_table

from config import SYMBOL, API_KEY, HOT_DURATION, WARM_DURATION, COLD_DURATION

import os, threading, time, sys
from dotenv import load_dotenv
load_dotenv()  # Load from .env file

from data_ingestion import start_producer, websocket_diagnostics_worker
from data_consumption import start_consumer, processing_diagnostics_worker
from storage_hot import create_hot_table
from storage_warm import create_warm_table, cursor
from diagnostics import create_diagnostics_tables
from migration import hot_to_warm, warm_to_cold


# shutdown functions
stop_event = threading.Event()

if "--stop" in sys.argv:
    stop_event.set()
    exit(0)

if __name__ == "__main__":
    try:
        #create hot warm/tables
        create_hot_table()
        create_warm_table()

        #start diagnostics
        create_diagnostics_tables(cursor)
        websocket_diagnostics_thread = threading.Thread(target=websocket_diagnostics_worker, args=(cursor, stop_event))
        websocket_diagnostics_thread.start()

        kafka_diagnostics_thread = threading.Thread(target=processing_diagnostics_worker, args=(cursor, stop_event))
        kafka_diagnostics_thread.start()


        #start ingesting data from the websocket and feed to kafka
        producer_thread = threading.Thread(target=start_producer, args=(SYMBOL, API_KEY, stop_event))
        producer_thread.daemon = True
        producer_thread.start()

        #start processing data from processing and store to tables
        consumer_thread = threading.Thread(target=start_consumer, args=(stop_event,))
        consumer_thread.daemon = True
        consumer_thread.start()

        #start migrating data between tables
        threading.Thread(target=hot_to_warm, args=(stop_event,HOT_DURATION), daemon=True).start() 
        threading.Thread(target=warm_to_cold, args=(stop_event,WARM_DURATION), daemon=True).start()


        while not stop_event.is_set():
            time.sleep(1)

    except KeyboardInterrupt:
        print("Keyboard interrupt received. Exiting...")
    # Let atexit handle closing Kafka producer
        stop_event.set()

