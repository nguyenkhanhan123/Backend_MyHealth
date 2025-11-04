import psycopg2
from psycopg2.extras import RealDictCursor

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="testMyHealth",
        user="postgres",
        password="1234",
        port=5432
    )
