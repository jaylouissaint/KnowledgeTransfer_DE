
import pandas as pd
import psycopg
import os
import re
import load_data.util_package.credentials as credentials
import load_data.util_package.logging as log

def get_connection():
    """
    Establish and return a PostgreSQL database connection.
    Uses credentials from config/credentials.py.
    This function can be imported and reused throughout the ETL pipeline.
    """
    conn = psycopg.connect(host="debprodserver.postgres.database.azure.com",
                           dbname=credentials.DB_USER,
                           user=credentials.DB_USER,
                           password=credentials.DB_PASSWORD)
    return conn


def query_data(query: str, params: tuple = None) -> pd.DataFrame:
    """
    Execute a SQL query and return the result as a pandas DataFrame.
    """
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn, params=params)
    finally:
        conn.close()
    return df
