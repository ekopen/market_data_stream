# main.py
# starts and stops the data pipeline

import threading, time, sys, os
from dotenv import load_dotenv
load_dotenv()  # Load from .env file
from config import SYMBOL, API_KEY, CLICKHOUSE_DURATION, ARCHIVE_FREQUENCY, HEARTBEAT_FREQUENCY, EMPTY_LIMIT, WS_LAG_THRESHOLD, PROC_LAG_THRESHOLD

from clickhouse import create_ticks_db, create_diagnostics_db, create_diagnostics_monitoring_db, create_uptime_db, new_client
from cloud_migration import migration_to_cloud
from kafka_producer import start_producer
from kafka_consumer import start_consumer
from monitoring import ticks_monitoring, diagnostics_monitoring

# logging setup
import logging
from logging.handlers import TimedRotatingFileHandler

formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

# File handler with daily rotation
file_handler = TimedRotatingFileHandler(
    filename=os.path.join("log_data/app.log"),
    when="midnight",
    backupCount=7,
    encoding="utf-8"
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Console (stdout) handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Root logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers = []  # Clear default handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

# shutdown setup if we want to pass python main.py --stop
stop_event = threading.Event()

if __name__ == "__main__":
    
    try:

        logger.info("System starting.")

        create_ticks_db()
        create_diagnostics_db()
        create_diagnostics_monitoring_db()
        create_uptime_db()

        ch = new_client()
        ch.insert('monitoring_db',[("System started",)],column_names=['message'])

        # start ingesting data from the websocket, feed to kafka, and insert to clickhouse
        producer_thread = threading.Thread(target=start_producer, args=(SYMBOL, API_KEY, stop_event))
        consumer_thread = threading.Thread(target=start_consumer, args=(stop_event,))
        #need to start these seperately for graceful shutdown
        producer_thread.start() 
        consumer_thread.start()

        # misc daemon aka background threads for diagnostics and cloud migration
        threading.Thread(target=ticks_monitoring, args=(stop_event,HEARTBEAT_FREQUENCY), daemon=True).start()
        threading.Thread(target=diagnostics_monitoring, args=(stop_event, HEARTBEAT_FREQUENCY, EMPTY_LIMIT, WS_LAG_THRESHOLD, PROC_LAG_THRESHOLD), daemon=True).start() 
        threading.Thread(target=migration_to_cloud, args=(stop_event,CLICKHOUSE_DURATION, ARCHIVE_FREQUENCY), daemon=True).start() 


        try:
            while not stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping application via KeyboardInterrupt.")
            ch.insert('monitoring_db',[("System closing",)],column_names=['message'])
            stop_event.set()
        finally:
            # give non-daemon threads a moment to shut down cleanly
            producer_thread.join(timeout=3)
            consumer_thread.join(timeout=3)
            logger.info("System shutdown complete.")

    except KeyboardInterrupt:
        logger.info("Exiting application")
        ch.insert('monitoring_db',[("System closing",)],column_names=['message'])
        