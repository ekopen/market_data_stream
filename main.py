# main.py
# controls all sub functions

from storage_hot import create_clickhouse_table, delete_clickhouse_table
from storage_warm import create_postgres_table, delete_postgres_table
from migration import hot_to_warm, warm_to_cold, cold_to_cloud
from data_ingestion import start_producer
from data_consumption import start_consumer
from migration import hot_to_warm, warm_to_cold, cold_to_cloud

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
        #delete the tables if they exist
        delete_clickhouse_table()
        delete_postgres_table()
        
        #create the tables if they do not exist
        create_clickhouse_table()
        create_postgres_table()

        #start migrating data between tables
        threading.Thread(target=hot_to_warm, args=(stop_event,HOT_DURATION), daemon=True).start() #start the hot to warm thread
        threading.Thread(target=warm_to_cold, args=(stop_event,WARM_DURATION), daemon=True).start() #start the warm to cold thread
        threading.Thread(target=cold_to_cloud, args=(stop_event,COLD_DURATION), daemon=True).start() #start the cold to cloud thread


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

