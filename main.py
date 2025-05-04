# Entry point to run producer and consumer together
import threading
from data_ingestion import start_producer
from data_consumption import start_consumer

if __name__ == "__main__":
    producer_thread = threading.Thread(target=start_producer)
    producer_thread.daemon = True
    producer_thread.start()

    print("Producer thread started.")

    # Start consumer in main thread (or you can also run it in another thread if preferred)
    start_consumer()
