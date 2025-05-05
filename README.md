# Real-Time Data Strean

This project is to start a real time data feed and build infrastructure needed to maintain it.

The subject of our data stream will be financial data, specifically for Crypto which has as 24/7 market.

## Project Overview


1. **Data Pipeline** – Real-time data ingestion using WebSockets and Kafka.
2. **Fault Tolerance/Error Analysis** - Create procedures and protocol to deal with crashes, bad data, etc.
3. **Data Storage** – Persist data with SQLite, with different tables based on term of data stored.
4. **Analysis/Visualization** – Clean and explore data.
5. **Front End** – Create a front end website version of the project that continiously runs.

---

## Docker/Kafka Setup Instructions

### Prerequisites

- **Download Docker Desktop**  

- **Install Python dependencies**
  ```bash
  pip install kafka-python websocket-client
  ```

- **Launch Kafka and ZooKeeper with Docker Compose**
  ```bash
  docker-compose up -d
  ```

- **Create Kafka Topic**
  
  First, open a terminal inside your running Kafka container:
  ```bash
  docker exec -it 02project2-kafka-1 bash
  ```

  Then, inside the container shell, run:
  ```bash
  kafka-topics --create --topic price_ticks \
    --bootstrap-server localhost:9092 \
    --partitions 1 --replication-factor 1
  ```

- **Run the Data Pipeline**
  ```bash
  python main.py
  ```

  This will start the Kafka producer (WebSocket ingestion) and consumer (SQLite storage) in a multi-threaded setup.









