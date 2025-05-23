# Diagnostics.py
# Conducting checks to ensure the system is working properly

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
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ,
        received_at TIMESTAMPTZ,
        processed_timestamp TIMESTAMPTZ,
        processing_lag FLOAT8,
        message_count FLOAT8
    )
    ''')

def insert_websocket_diagnostics(cursor, timestamp, received_at, message_count):
    
    #this may be broken
    cursor.execute("""
        DELETE FROM websocket_diagnostics
        WHERE received_at < NOW() - INTERVAL '1 hour'
    """)

    lag_seconds = (received_at - timestamp).total_seconds()

    cursor.execute(
        '''
        INSERT INTO websocket_diagnostics (timestamp, received_at, websocket_lag, message_count)
        VALUES (%s, %s, %s, %s)
        ''',
        (timestamp, received_at, lag_seconds, message_count)
    )

def insert_processing_diagnostics(cursor, timestamp, received_at, processed_timestamp, message_count):
    
    cursor.execute("""
        DELETE FROM processing_diagnostics
        WHERE received_at < NOW() - INTERVAL '1 hour'
        """)
    
    processing_lag = (processed_timestamp - received_at).total_seconds()

    cursor.execute(
        '''
        INSERT INTO processing_diagnostics (timestamp, received_at, processed_timestamp, processing_lag, message_count)
        VALUES (%s, %s, %s, %s, %s)
        ''',
        (timestamp, received_at, processed_timestamp, processing_lag, message_count)
    )


