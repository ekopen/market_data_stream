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

## Kafka Setup Instructions

### 1. Downloads

- Kafka: [kafka.apache.org/downloads](https://kafka.apache.org/downloads)  
  > Use the **Scala 2.13** release: `kafka_2.13-3.5.1.tgz`  
  > (Later versions may lack Zookeeper compatibility)

- Java (Required): [Adoptium Temurin Java](https://adoptium.net/temurin/releases/?os=windows)
  > Use Version 17

---

### 2. Starting Kafka Locally

In **three separate terminal windows**, run the following:

#### Zookeeper
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --topic price_ticks --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
May need to use WSL if there are Java issues.

### 3. Installations

pip install kafka-python
