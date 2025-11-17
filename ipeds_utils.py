'''Utilities file with functions to help
create, load, update, and delete college score card data'''
import pandas as pd
import psycopg
import credentials
import os
import re


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


def load_data(path_file, year):
    '''
    This function takes in a CSV file and year and returns a pandas DataFrame.
    Only rows with non-missing dataframe are returned.
    Rows missing required fields are saved and outputted into csv file.
    '''
    try:
        data = pd.read_csv(path_file, encoding="latin1", dtype=str)

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
            file_name = f"ipeds_missing_{year}.csv"
            file_path = os.path.join(folder, file_name)

            # missing data is saved and outputted
            missing_data.to_csv(file_path, index=False)
            print(f"{missing_rows} missing rows saved to {file_path}.")

        return complete_data
    except Exception as e:
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
        print(f"Database error occurred: {e}")
    except Exception as e:
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
    try:
        with conn.transaction():
            cur.executemany(query, df.values.tolist())
            print(f"{cur.rowcount} rows loaded or updated into {table_name}\n")
    except Exception as e:
        print(f"Error inserting data into {table_name}: ", e)
    finally:
        cur.close()
        conn.close()

# Carnegie Classification Variable Cleaning 

CARNEGIE_VAR_SUFFIXES = ["BASIC", "IPUG", "UGPRF", "ENPRF", "SZSET"]


def detect_latest_carnegie_year(df: pd.DataFrame) -> int:
    """
    Scan dataframe columns for Carnegie variables of the form:
        CYYBASIC, CYYIPUG, CYYUGPRF, CYYENPRF, CYYSZSET
    Extract the YY as an integer and return the max (latest) year suffix.
    """
    pattern = re.compile(r"^C(\d{2})(BASIC|IPUG|UGPRF|ENPRF|SZSET)$")

    years = set()
    for col in df.columns:
        m = pattern.match(col)
        if m:
            year_suffix = int(m.group(1))
            years.add(year_suffix)

    if not years:
        raise ValueError("No Carnegie classification columns (CYY*) found in dataframe.")

    return max(years)


def get_latest_carnegie_columns(df: pd.DataFrame):
    """
    Return a dict mapping logical Carnegie variable names to the actual
    column names for the latest available Carnegie year in the dataframe.

    Example return:
        {
            "BASIC": "C21BASIC",
            "IPUG":  "C21IPUG",
            "UGPRF": "C21UGPRF",
            "ENPRF": "C21ENPRF",
            "SZSET": "C21SZSET",
            "year":  21
        }
    """
    latest_year = detect_latest_carnegie_year(df)
    prefix = f"C{latest_year}"

    col_map = {"year": latest_year}
    for suffix in CARNEGIE_VAR_SUFFIXES:
        col_name = f"{prefix}{suffix}"
        if col_name not in df.columns:
            raise KeyError(f"Expected Carnegie column '{col_name}' not found in dataframe.")
        col_map[suffix] = col_name

    return col_map
