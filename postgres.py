#WIP here   

#postgres.py
# enables storing warm data to postgres
import psycopg2
from datetime import datetime, timezone

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

def create_postgres_table():
    #creating a table if it does not exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS price_ticks(
        timestamp TIMESTAMPTZ,
        timestamp_ms BIGINT,
        symbol VARCHAR(255),
        price FLOAT8,
        volume FLOAT8,
        received_at TIMESTAMPTZ
    )''')

#enable BATCH writing to postgres