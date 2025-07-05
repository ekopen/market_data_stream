# Diagnostics.py
# Conducting  checks to ensure the system is working properly

import psycopg2

#setting up the connection to postgres
conn = psycopg2.connect(
    dbname='price_data',
    user='postgres',
    password='mypgpassword',
    host='localhost',
    port=5432
)
conn.autocommit = True
cursor = conn.cursor()

def create_consumer_metrics_table(cursor):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS consumer_metrics (
        timestamp TIMESTAMPTZ PRIMARY KEY,
        messages INT,
        avg_lag_sec FLOAT8,
        max_lag_sec FLOAT8,
        source VARCHAR(50)
    )
    ''')

def insert_consumer_metric(cursor, messages, avg_lag, max_lag, source='kafka_consumer'):
    cursor.execute('''
        INSERT INTO consumer_metrics (timestamp, messages, avg_lag_sec, max_lag_sec, source)
        VALUES (now(), %s, %s, %s, %s)
    ''', (messages, avg_lag, max_lag, source))
    print(f"[Consumer Metrics] {messages} msgs | avg_lag={avg_lag:.2f}s | max_lag={max_lag:.2f}s")

def create_producer_metrics_table(cursor):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS producer_metrics (
        timestamp TIMESTAMPTZ PRIMARY KEY,
        messages INT,
        source VARCHAR(50)
    )
    ''')

def insert_producer_metric(cursor, message_count, source='websocket_producer'):
    cursor.execute('''
        INSERT INTO producer_metrics (timestamp, messages, source)
        VALUES (now(), %s, %s)
    ''', (message_count, source))

# look at message rate mismatches next ebtween the consumer and producer
# then, finish the system error log
def create_system_errors_table(cursor):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS system_errors (
        timestamp TIMESTAMPTZ DEFAULT now(),
        component VARCHAR(50),
        level VARCHAR(10),
        message TEXT,
        stack_trace TEXT
    )
    ''')