# config.py
# variables that are used across the project

import os

# KAFKA_BOOTSTRAP_SERVER = "kafka:9092" #local development
KAFKA_BOOTSTRAP_SERVER="kafka:19092" #DNS development

API_KEY = os.getenv("FINNHUB_API_KEY")
SYMBOL = 'BINANCE:ETHUSDT'
