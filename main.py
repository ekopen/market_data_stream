# main.py
# starts and stops the data pipeline

import threading, time, sys
from dotenv import load_dotenv
load_dotenv()  # Load from .env file
from config import SYMBOL, API_KEY, CLICKHOUSE_DURATION, DIAGNOSTIC_FREQUENCY

from clickhouse import create_ticks_db, create_diagnostics_db, insert_diagnostics
from db_storage import clickhouse_to_cloud
from kafka_producer import start_producer
from kafka_consumer import start_consumer

# logging setup
import logging, sys
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
fh = logging.FileHandler("app.log", encoding="utf-8")
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
logging.getLogger().addHandler(fh)
logger = logging.getLogger("main")

# shutdown setup if we want to pass python main.py --stop
stop_event = threading.Event()
if "--stop" in sys.argv:
    stop_event.set()
    exit(0)

if __name__ == "__main__":
    
    try:

        logger.info("Application starting.")

        create_ticks_db()
        create_diagnostics_db()

        # start ingesting data from the websocket, feed to kafka, and insert to clickhouse
        producer_thread = threading.Thread(target=start_producer, args=(SYMBOL, API_KEY, stop_event))
        consumer_thread = threading.Thread(target=start_consumer, args=(stop_event,))
        #need to start these seperately for graceful shutdown
        producer_thread.start() 
        consumer_thread.start()

        # misc daemon aka background threads for diagnostics and cloud migration
        threading.Thread(target=insert_diagnostics, args=(stop_event,DIAGNOSTIC_FREQUENCY), daemon=True).start() 
        threading.Thread(target=clickhouse_to_cloud, args=(stop_event,CLICKHOUSE_DURATION), daemon=True).start() 

        try:
            while not stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping application via KeyboardInterrupt.")
            stop_event.set()
        finally:
            # give non-daemon threads a moment to shut down cleanly
            producer_thread.join(timeout=3)
            consumer_thread.join(timeout=3)
            logger.info("Application shutdown complete.")

    except KeyboardInterrupt:
        logger.info("Exiting application")