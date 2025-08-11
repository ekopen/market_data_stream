# Diagnostics.py
# Conducting checks to ensure the system is working properly, implemented with Clickhouse

import clickhouse_connect
import threading

# duration to retain diagnostics data
WEBSOCKET_DIAG_TTL = "1 DAY"
PROCESSING_DIAG_TTL = "1 DAY"
TRANSFER_DIAG_TTL = "1 DAY"

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

def create_diagnostics_tables():
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
    TTL toDateTime(timestamp) + INTERVAL {WEBSOCKET_DIAG_TTL} DELETE
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
    TTL toDateTime(timestamp) + INTERVAL {PROCESSING_DIAG_TTL} DELETE
    """)
    ch.command("DROP TABLE IF EXISTS transfer_diagnostics")
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS transfer_diagnostics (
        transfer_type String,
        transfer_start DateTime64(3, 'UTC'),
        transfer_end DateTime64(3, 'UTC'),
        transfer_lag Float64,
        message_count Float64,
        transfer_size Float64
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(transfer_start)
    ORDER BY transfer_start
    TTL toDateTime(transfer_start) + INTERVAL {TRANSFER_DIAG_TTL} DELETE
    """)

def insert_websocket_diagnostics(timestamp, received_at, message_count):
    lag_seconds = (received_at - timestamp).total_seconds()
    _client().insert(
        'websocket_diagnostics',
        [(timestamp, received_at, lag_seconds, message_count)],
        column_names=['timestamp', 'received_at', 'websocket_lag', 'message_count']
    )

def insert_processing_diagnostics(timestamp, received_at, processed_timestamp, message_count):
    processing_lag = (processed_timestamp - received_at).total_seconds()
    _client().insert(
        'processing_diagnostics',
        [(timestamp, received_at, processed_timestamp, processing_lag, message_count)],
        column_names=['timestamp', 'received_at', 'processed_timestamp', 'processing_lag', 'message_count']
    )

def insert_transfer_diagnostics(transfer_type, transfer_start, transfer_end, message_count, transfer_size):
    transfer_lag = (transfer_end - transfer_start).total_seconds()
    _client().insert(
        'transfer_diagnostics',
        [(transfer_type, transfer_start, transfer_end, transfer_lag, message_count, transfer_size)],
        column_names=['transfer_type', 'transfer_start', 'transfer_end', 'transfer_lag', 'message_count', 'transfer_size']
    )
