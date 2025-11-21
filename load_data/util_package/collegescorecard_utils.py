'''Utilities file with functions to help
create, load, update, and delete college score card data'''
import pandas as pd
import psycopg
from load_data.util_package import credentials as cred
import os
import load_data.util_package.logging as log


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


def load_data(path_file, year):
    '''
    This function takes in a CSV file and year and returns a pandas DataFrame.
    Only rows with non-missing dataframe are returned.
    Rows missing required fields are saved and outputted into csv file.
    '''
    try:
        data = pd.read_csv(path_file, low_memory=False)
        total_rows = data.shape[0]
        print(f"{total_rows} rows read from file.")
        # Add a year column
        data['YEAR'] = year

        # Split data into complete and missing
        # We only care if specific required columns are missing
        required_cols = ["OPEID", "UNITID"]
        complete_data = data.dropna(subset=required_cols)
        missing_data = data[data[required_cols].isna().any(axis=1)]
        missing_rows = missing_data.shape[0]

        if missing_rows != 0:
            # Folder to save missing data
            folder = "Invalid_data"
            os.makedirs(folder, exist_ok=True)

            # Create a descriptive file name
            file_name = f"college_scorecard_missing_{year}.csv"
            file_path = os.path.join(folder, file_name)

            # missing data is saved and outputted
            missing_data.to_csv(file_path, index=False)
            print(f"{missing_rows} missing rows saved to {file_path}.")
            print(f"Proceeding with {complete_data.shape[0]} valid rows.")

        return complete_data
    except Exception as e:
        log.get_logger(__name__).error(f"Loading error: {e}", exc_info=True)
        print(f"Error occured loading data: {e}")
        raise


def create_table(query):
    """
    Runs a CREATE TABLE SQL query from sql_queries.py.

    query: str
        Full SQL statement defining the table structure.
    """
    conn = get_connection()
    table_name = query.split("(")[0].strip().split()[-1]
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
            print(f"{table_name} table created or already exists.")
    except psycopg.errors.DatabaseError as e:
        log.get_logger(__name__).error(
            f"Database error occurred: {e}", exc_info=True)
        print(f"Database error occurred: {e}")
    except Exception as e:
        log.get_logger(__name__).error(
            f"Non-Database error occurred: {e}", exc_info=True)
        print(f"Non-Database error occurred: {e}")
    finally:
        cur.close()
        conn.close()


def insert_data(query, df):
    """
    Insert multiple rows of data from a DataFrame
    into a table using the given SQL query.

    Parameters
    ----------
    query : str
        SQL INSERT statement from sql_queries.py.
    df : pandas.DataFrame
        Clean data to insert; each row corresponds to the placeholders.
    """
    conn = get_connection()
    cur = conn.cursor()
    table_name = query.split("(")[0].strip().split()[-1]
    print(f"====INSERTING TO {table_name} TABLE====")

    nrows = df.shape[0]
    try:
        with conn.transaction():
            cur.executemany(query, df.values.tolist())
            print(f"SUCCESS: {cur.rowcount} / {nrows} rows inserted or",
                  f"updated into {table_name}\n")
    except Exception as e:
        log.get_logger(__name__).error(
            f"Insertion failed at row: {e}", exc_info=True)
        print(f"Insert failed at row: {cur.rowcount}")
        print(f"Error: {e}")
        print(df.iloc[[cur.rowcount], :])
    finally:
        cur.close()
        conn.close()
