# Diagnostics.py
# Conducting checks to ensure the system is working properly, implemented with Clickhouse

import clickhouse_connect
import threading

def _new_client():
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='mysecurepassword'
    )

# Thread-local storage for a client per thread
_thread_local = threading.local()
def _client():
    if not hasattr(_thread_local, 'ch'):
        _thread_local.ch = _new_client()
    return _thread_local.ch

def create_diagnostics_db():
    ch = _new_client()  # use a one-off client for DDL
    ch.command("DROP TABLE IF EXISTS websocket_diagnostics")
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS websocket_diagnostics (
        timestamp DateTime64(3, 'UTC'),
        received_at DateTime64(3, 'UTC'),
        websocket_lag Float64,
        message_count Float64
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(timestamp)
    ORDER BY timestamp
    """)
    ch.command("DROP TABLE IF EXISTS processing_diagnostics")
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS processing_diagnostics (
        timestamp DateTime64(3, 'UTC'),
        received_at DateTime64(3, 'UTC'),
        processed_timestamp DateTime64(3, 'UTC'),
        processing_lag Float64,
        message_count Float64
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(timestamp)
    ORDER BY timestamp
    """)    

def insert_websocket_diagnostics(avg_timestamp, avg_received, message_count):
    avg_lag = (avg_received - avg_timestamp).total_seconds()
    _client().insert(
        'websocket_diagnostics',
        [(avg_timestamp, avg_received, avg_lag, message_count)],
        column_names=['timestamp', 'received_at', 'websocket_lag', 'message_count']
    )

def insert_processing_diagnostics(avg_timestamp, avg_received, avg_insert_time, message_count):
    processing_lag = (avg_insert_time - avg_received).total_seconds()
    _client().insert(
        'processing_diagnostics',
        [(avg_timestamp, avg_received, avg_insert_time, processing_lag, message_count)],
        column_names=['timestamp', 'received_at', 'processed_timestamp', 'processing_lag', 'message_count']
    )