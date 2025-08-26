# Real-Time Market Data Pipeline

## Overview
This project implements a real-time market data infrastructure designed for ingestion, processing, storage, monitoring, and visualization of financial tick data.  
It simulates a simplified version of pipelines used in trading environments, featuring:

- Kafka for streaming
- ClickHouse for storage
- Grafana for dashboarding
- AWS S3 for archival
- Monitoring and diagnostics for reliability

### Simple Diagram of the Architecture
![Architecture Diagram](assets/architecture_simple.png)
(Detailed view provided below.)

## Architecture
The pipeline consists of the following components:

1. **Data Ingestion**  
   A Kafka producer connects to an API and streams live market tick data (`symbol`, `price`, `volume`, `timestamp`) into a Kafka topic.

2. **Data Storage**  
   A Kafka consumer validates, batches, and inserts tick data into ClickHouse tables.

3. **Monitoring & Diagnostics**  
   Metrics are periodically recorded and stored in diagnostics tables. Logging captures detailed system behavior for troubleshooting.

4. **Cloud Archival**  
   All data is automatically archived to AWS S3 for long-term storage.

5. **Dashboard**  
   A Grafana app provides real-time visualizations of tick data, pipeline performance, and logs.

6. **Containerized Deployment**  
   Managed with Docker Compose for reproducibility and production alignment.

### Detailed Diagram of the Architecture
![Architecture Diagram](assets/architecture_complex.png)

## Tech Stack
- **Messaging/Streaming**: Apache Kafka  
- **Database**: ClickHouse  
- **Dashboard/UI**: Grafana  
- **Orchestration**: Docker Compose  
- **Cloud Integration**: AWS S3  
- **Monitoring/Logging**: Python logging + diagnostics tables  

## Getting Started
- WIP

## Future Improvements
- Create external downtime detector
- Deploy to cloud
- Update README/Flowcharts as needed
   - add promtail/loki log info
   - get rid of old dashboard files 
   - add info about tests
- future add ons
   - Add email alerts
   - Add a portfolio/systemized ML strategies
   - Expand to bid/ask data and additional assets
   - Add CI/CD deployment
