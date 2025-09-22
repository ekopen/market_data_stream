# Market Data Stream  
This module is part of my overarching **Live Trading Engine** project. Visit [www.erickopen.com](http://www.erickopen.com) to see my projects running live and to view comprehensive documentation.  

## Overview  
This module configures a Kafka producer that captures real-time market data from a websocket. The producer publishes data to a Kafka topic, which downstream consumers use to update market data tables and retrieve live prices.  

## Details  
- Runs in Docker on an Ubuntu server to enable reproducible builds and dashboard monitoring.  
- Connects to a Finnhub websocket that streams crypto pricing data from Binance for multiple tickers.  
- Retention is capped by both time duration and memory size to optimize storage and keep latency low (configured via Kafka retention policies).  
- Integrated with Prometheus and Grafana to track message throughput, producer health, memory usage, and server metrics.  
- Other modules in the Live Trading Engine subscribe to this Kafka topic. For example, the market_data_module continuously writes streaming data into ClickHouse for historical analysis, while the trading_module consumes the latest prices for use in trading strategy execution.  

## Future Improvements  
- Expand the producer to stream more symbols and include bid/ask price levels and order book depth.   
- Explore partitioning and schema management for scalability as data volume grows.  

## Known Issues  
- Configuration files may contain redundant or overly complex settings; a review and simplification pass is planned to streamline the setup.  
