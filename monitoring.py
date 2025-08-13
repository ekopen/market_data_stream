# monitoring.py
# records notable events and handles crashes

from clickhouse import new_client
import time
from datetime import datetime, timezone, timedelta
import pandas as pd
import threading
import os, subprocess, sys
import logging
logger = logging.getLogger(__name__)

def insert_diagnostics(stop_event,duration):

    time.sleep(duration)
    ch = new_client()
    empty_streak = 0

    while not stop_event.is_set():
        logger.debug("Starting diagnostics insert cycle.")
        try:
            current_time = datetime.now(timezone.utc)
            current_time_ms = int(current_time.timestamp() * 1000)
            cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=duration)
            cutoff_time_ms = int(cutoff_time.timestamp() * 1000)

            diagnostic_rows = ch.query(f'''
                SELECT * FROM ticks_db
                WHERE toUnixTimestamp64Milli(insert_time) > {cutoff_time_ms} AND toUnixTimestamp64Milli(insert_time) <= {current_time_ms}
            ''').result_rows

            df = pd.DataFrame(diagnostic_rows, columns=[
                'timestamp', 'timestamp_ms', 'symbol', 'price', 'volume', 'received_at', 'insert_time'
                ])

            #if the system is down, we still want to record diagnostics data to show lag
            if df.empty:

                avg_timestamp = None
                avg_received_at = None
                avg_insert_time = None
                message_count = 0
                ws_lag = None
                proc_lag = None

                empty_streak += 1
                logger.debug(f"Diagnostics has returned an empty dataframe. Occurrence count: {empty_streak}")

            else:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df['received_at'] = pd.to_datetime(df['received_at'])
                df['insert_time'] = pd.to_datetime(df['insert_time'])

                avg_timestamp = df['timestamp'].mean()
                avg_received_at = df['received_at'].mean()
                avg_insert_time = df['insert_time'].mean()
                ws_lag = (avg_received_at - avg_timestamp).total_seconds()
                proc_lag = (avg_insert_time - avg_received_at).total_seconds()
                message_count = len(df)
                
                empty_streak = 0

            ch.insert('websocket_diagnostics',
                [(avg_timestamp, avg_received_at, ws_lag, message_count)],
                column_names=['avg_timestamp', 'avg_received_at', 'avg_websocket_lag', 'message_count'])
            
            ch.insert('processing_diagnostics',
                [(avg_timestamp, avg_received_at, avg_insert_time, proc_lag, message_count)],
                column_names=['avg_timestamp', 'avg_received_at', 'avg_processed_timestamp', 'avg_processing_lag', 'message_count'])
        
            logger.info(f"Inserted diagnostics for {message_count} messages.")

        except Exception as e:
            logger.exception(f"Error inserting diagnostics.")

        time.sleep(duration)

def insert_monitoring(stop_event,duration,empty_limit, ws_lag_threshold, proc_lag_threshold):

    time.sleep(duration+1) #delay to allow diagnostics to populate first
    ch = new_client()

    while not stop_event.is_set():

        try:
            logger.debug("Starting monitoring cycle.")

            #--------------------------pipeline down--------------------------#
            
            rows = ch.query(f"""
                SELECT SUM(message_count)
                FROM (
                    SELECT message_count
                    FROM websocket_diagnostics
                    ORDER BY diagnostics_timestamp DESC
                    LIMIT {int(empty_limit)}
                )
            """).result_rows

            total = rows[0][0] if rows and rows[0] else None

            if total is not None and total == 0:
                logger.warning(
                    "Diagnostics has been empty for %d consecutive cycles. Restarting system."
                )
                ch.insert(
                    'monitoring_db',
                    [("System restart",)],
                    column_names=['message']
                )
                
                # Relaunch new process with same interpreter & args, then exit current one
                subprocess.Popen([sys.executable] + sys.argv)
                os._exit(0)

            #--------------------------websocket lag spike--------------------------#
            lag = ch.query(f"""
                SELECT avg_websocket_lag
                FROM websocket_diagnostics
                ORDER BY diagnostics_timestamp DESC
                LIMIT 1
            """).result_rows[0][0]

            if lag is not None and (lag > ws_lag_threshold):
                logger.warning(f"Websocket Lag Spike: {lag}")
                ch.insert(
                    'monitoring_db',
                    [(f"Websocket Lag Spike: {lag}",)],
                    column_names=['message']
                )  

            #--------------------------processing lag spike--------------------------#
            lag = ch.query(f"""
                SELECT avg_processing_lag
                FROM processing_diagnostics
                ORDER BY diagnostics_timestamp DESC
                LIMIT 1
            """).result_rows[0][0]

            if lag is not None and (lag > proc_lag_threshold):
                logger.warning(f"Processing Lag Spike: {lag}")
                ch.insert(
                    'monitoring_db',
                    [(f"Processing Lag Spike: {lag}",)],
                    column_names=['message']
                )                

            logger.info(f"Completed a monitoring cycle.")

        except Exception:
            logger.exception("insert_monitoring error")

        time.sleep(duration)