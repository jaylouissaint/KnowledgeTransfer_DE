'''Utilities file with functions to help
create, load, update, and delete college score card data'''
import pandas as pd
import psycopg
import credentials as cred


def get_connection():
    """
    Establish and return a PostgreSQL database connection.
    Uses credentials from config/credentials.py.
    This function can be imported and reused throughout the ETL pipeline.
    """
    conn = psycopg.connect(host="debprodserver.postgres.database.azure.com",
                           dbname=cred.DB_USER,
                           user=cred.DB_USER,
                           password=cred.DB_PASSWORD)
    return conn


def load_data(path_file):
    '''
    This function takes in a CSV file and loads it into a pandas DataFrame
    '''
    try:
        data = pd.read_csv(path_file)
        return data
    except Exception as e:
        print(f"Error occured loading data: {e}")
        raise


def create_table(query):
    conn = get_connection()
    table_name = query.split("(")[0].strip().split()[-1]
    try:
        with conn.cursor() as cur:
            # Check if table already exists
            cur.execute("""
                        SELECT EXISTS(SELECT FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = %s);
                        """, (table_name.lower(),))
            exists = cur.fetchone()[0]

            if exists:
                print(f"Table {table_name} already exist. Skipping creation.")
            else:
                cur.execute(query)
                conn.commit()
                print(f"Successfully created the '{table_name}' table.")
    except psycopg.errors.DatabaseError as e:
        print(f"Database error occurred: {e}")
    except Exception as e:
        print(f"Non-Database error occurred: {e}")
    finally:
        cur.close()
        conn.close()


def insert_data(query, df):
    conn = get_connection()
    cur = conn.cursor()
    table_name = query.split("(")[0].strip().split()[-1]
    try:
        with conn.transaction():
            cur.executemany(query, df.values.tolist())
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            rows = cur.fetchone[0]
            print(f"{rows} rows successfully loaded into table.")
    except Exception as e:
        print("Error inserting data", e)
    finally:
        cur.close()
        conn.close()
