# config.py
# variables that are used across the project

import os

API_KEY = os.getenv("FINNHUB_API_KEY")
SYMBOL = 'BINANCE:ETHUSDT'
HOT_DURATION = 300  # seconds
WARM_DURATION = 3600  # seconds
DIAGNOSTIC_FREQUENCY = 5 #seconds per diagnostic update