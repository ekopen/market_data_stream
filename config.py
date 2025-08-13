# config.py
# variables that are used across the project

import os

API_KEY = os.getenv("FINNHUB_API_KEY")
SYMBOL = 'BINANCE:ETHUSDT'

CLICKHOUSE_DURATION = 60  # seconds before moving data to cold storage
DIAGNOSTIC_FREQUENCY = 5 #seconds per diagnostic update