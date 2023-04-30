import psycopg2
import os

PG_HOST = os.environ.get('PG_HOST')
PG_PORT = os.environ.get('PG_PORT')
PG_DATABASE = os.environ.get('PG_DATABASE')
PG_USERNAME = os.environ.get('PG_USERNAME')
PG_PASSWORD = os.environ.get('PG_PASSWORD')

def postgresConn():
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE,
        user=PG_USERNAME, password=PG_PASSWORD)
    cur = conn.cursor()
    return [
        cur,
        conn
    ]