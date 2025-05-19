# config.py
# variables that are used across the project

import os

API_KEY = os.getenv("FINNHUB_API_KEY")
SYMBOL = 'BINANCE:ETHUSDT'
HOT_DURATION = 15  # seconds
WARM_DURATION = 60  # seconds
COLD_DURATION = 180  # seconds