# config.py
# variables that are used across the project

import os

API_KEY = os.getenv("FINNHUB_API_KEY")
SYMBOL = 'BINANCE:ETHUSDT'

CLICKHOUSE_DURATION = 10  # seconds before moving clickhouse data to parquet
LOG_DURATION = 10  # econds before moving logging data to parquet
DIAGNOSTIC_FREQUENCY = 5 #seconds per diagnostic update

EMPTY_LIMIT = 4 #number of consecutive empty diagnostics records before restarting the system