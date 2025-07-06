# config.py
# variables that are used across the project

import os

API_KEY = os.getenv("FINNHUB_API_KEY")
SYMBOL = 'BINANCE:ETHUSDT'
HOT_DURATION = 60  # seconds
WARM_DURATION = 300  # seconds, make sure to adjust clickhouse TTL to match this duration
DIAGNOSTIC_FREQUENCY = 15 #seconds per diagnostic update