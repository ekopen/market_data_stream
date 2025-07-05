# Diagnostics.py
# Conducting checks to ensure the system is working properly
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

def create_diagnostics_tables(cursor):
    
    # websocket diagnostics
    cursor.execute("DROP TABLE IF EXISTS websocket_diagnostics")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS websocket_diagnostics (
        timestamp TIMESTAMPTZ PRIMARY KEY,
        received_at TIMESTAMPTZ,
        websocket_lag FLOAT8,
        message_count FLOAT8
    )
    ''')

    # processing diagnostics
    cursor.execute("DROP TABLE IF EXISTS processing_diagnostics")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS processing_diagnostics (
        timestamp TIMESTAMPTZ,
        received_at TIMESTAMPTZ,
        processed_timestamp TIMESTAMPTZ,
        processing_lag FLOAT8,
        message_count FLOAT8
    )
    ''')

    cursor.execute("DROP TABLE IF EXISTS transfer_diagnostics")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transfer_diagnostics (
        transfer_type TEXT,
        transfer_start TIMESTAMPTZ,
        transfer_end TIMESTAMPTZ,
        transfer_lag FLOAT8,
        message_count FLOAT8,
        transfer_size FLOAT8
    )
    ''')

def insert_websocket_diagnostics(cursor, timestamp, received_at, message_count):
    
    # cursor.execute("""
    #     DELETE FROM websocket_diagnostics
    #     WHERE received_at < NOW() - INTERVAL '1 hour'
    # """)

    lag_seconds = (received_at - timestamp).total_seconds()

    cursor.execute(
        '''
        INSERT INTO websocket_diagnostics (timestamp, received_at, websocket_lag, message_count)
        VALUES (%s, %s, %s, %s)
        ''',
        (timestamp, received_at, lag_seconds, message_count)
    )

def insert_processing_diagnostics(cursor, timestamp, received_at, processed_timestamp, message_count):
    
    # cursor.execute("""
    #     DELETE FROM processing_diagnostics
    #     WHERE received_at < NOW() - INTERVAL '1 hour'
    #     """)
    
    processing_lag = (processed_timestamp - received_at).total_seconds()

    cursor.execute(
        '''
        INSERT INTO processing_diagnostics (timestamp, received_at, processed_timestamp, processing_lag, message_count)
        VALUES (%s, %s, %s, %s, %s)
        ''',
        (timestamp, received_at, processed_timestamp, processing_lag, message_count)
    )

def insert_transfer_diagnostics(cursor, transfer_type, transfer_start, transfer_end, message_count, transfer_size):
    
    cursor.execute("""
        DELETE FROM transfer_diagnostics
        WHERE received_at < NOW() - INTERVAL '1 hour'
        """)
    
    transfer_lag = (transfer_end - transfer_start).total_seconds()

    cursor.execute(
        '''
        INSERT INTO transfer_diagnostics (transfer_type, transfer_start, transfer_end, transfer_lag, message_count, transfer_size)
        VALUES (%s, %s, %s, %s, %s, %s)
        ''',
        (transfer_type, transfer_start, transfer_end, transfer_lag, message_count, transfer_size)
    )


