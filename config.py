# config.py
# variables that are used across the project

import os

API_KEY = os.getenv("FINNHUB_API_KEY")
SYMBOL = 'BINANCE:ETHUSDT'

CLICKHOUSE_DURATION = 600  # seconds before moving clickhouse data to parquet
LOG_DURATION = 600  # seconds before moving logging data to parquet
DIAGNOSTIC_FREQUENCY = 5 #seconds per diagnostic update

EMPTY_LIMIT = 4 #number of consecutive empty diagnostics records before restarting the system

WS_LAG_THRESHOLD = 1 # amount of websocket lag considered a spike
PROC_LAG_THRESHOLD = 1.5 # amount of processing lag considered a spike
