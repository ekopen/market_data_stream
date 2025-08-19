# config.py
# variables that are used across the project

import os

API_KEY = os.getenv("FINNHUB_API_KEY")
SYMBOL = 'BINANCE:ETHUSDT'

DATA_DURATION = 600  # seconds before moving local data to parquet and cloud
HEARTBEAT_FREQUENCY = 5 # seconds per heartbeat, where we record diagnostics and monitoring data

EMPTY_LIMIT = 6 #number of consecutive empty diagnostics records before restarting the system

WS_LAG_THRESHOLD = 1 # amount of websocket lag considered a spike
PROC_LAG_THRESHOLD = 1.5 # amount of processing lag considered a spike
