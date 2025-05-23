# Diagnostics.py
# Conducting checks to ensure the system is working properly

def websocket_diagnostics(cursor):
    cursor.execute("DROP TABLE IF EXISTS websocket_diagnostics")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS websocket_diagnostics (
        timestamp TIMESTAMPTZ PRIMARY KEY,
        received_at TIMESTAMPTZ,
        lag FLOAT8,
        message_count FLOAT8
    )
    ''')

def insert_websocket_diagnostics(cursor, timestamp, received_at, message_count):
    lag_seconds = (received_at - timestamp).total_seconds()

    cursor.execute(
        '''
        INSERT INTO websocket_diagnostics (timestamp, received_at, lag, message_count)
        VALUES (%s, %s, %s, %s)
        ''',
        (timestamp, received_at, lag_seconds, message_count)
    )
