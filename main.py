# main.py
# controls all sub functions

import threading, time, sys
from dotenv import load_dotenv
load_dotenv()  # Load from .env file
from config import SYMBOL, API_KEY, DURATION, DIAGNOSTIC_FREQUENCY

from market_ticks import create_ticks_db, cold_storage
from diagnostics import create_diagnostics_db, insert_diagnostics

from kafka_producer import start_producer
from kafka_consumer import start_consumer

# shutdown functions
stop_event = threading.Event()

if "--stop" in sys.argv:
    stop_event.set()
    exit(0)

if __name__ == "__main__":
    try:

        create_ticks_db()
        create_diagnostics_db()

        #start ingesting data from the websocket and feed to kafka
        producer_thread = threading.Thread(target=start_producer, args=(SYMBOL, API_KEY, stop_event))
        producer_thread.daemon = True
        producer_thread.start()

        #start processing data from processing and store to tables
        consumer_thread = threading.Thread(target=start_consumer, args=(stop_event,))
        consumer_thread.daemon = True
        consumer_thread.start()

        #start processing diagnostics
        threading.Thread(target=insert_diagnostics, args=(stop_event,DIAGNOSTIC_FREQUENCY), daemon=True).start()

        #start moving data to cold storage
        threading.Thread(target=cold_storage, args=(stop_event,DURATION), daemon=True).start() 

        while not stop_event.is_set():
            time.sleep(1)

    except KeyboardInterrupt:
        print("Keyboard interrupt received. Exiting...")
    # Let atexit handle closing Kafka producer
        stop_event.set()