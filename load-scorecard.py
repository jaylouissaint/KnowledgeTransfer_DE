# Your driver code (including input / output handling) should go here
import sys
import psycopg
import credentials
from cleaning-collegescorecard
from collegescorecard_utils

def main():
    # read inserted file 
    # needs to capture if wrong file is given 
    try:
        filename = sys.argv[1]
    except (IndexError, ValueError):
        print("Missing csv file")
        sys.exit(1)

    try: 
        # Establish a connection to the database
        conn = psycopg.connect(host="debprodserver.postgres.database.azure.com",
                            dbname=credentials.DB_USER,
                            user=credentials.DB_USER,
                            password=credentials.DB_PASSWORD)
        cur = conn.cursor()

        # this persona does not exist in personae table so it will
        # lead to a foreign key constraint violation
        query = "INSERT INTO events (moment, persona, element, score)" \
                "VALUES ('2015-03-22 20:21:28.248253', 2000, 29407, 400);"

        # clean data 
        clean_data = clean_ipeds(data)

        # insert data 
        for index, row in df.iterrows():
            cur.execute(query)

        # commit all changes 
        conn.commit()
        print("Data loaded successfully.")
        
    except psycopg.IntegrityError as e:
        print(f"Foreign key constraint violation detected: {e}")
        conn.rollback()
    except Exception as e:
        # if something fails, undo all the changes 
        conn.rollback()
        # print the error we receive
        print(f"Error: {e}")
    finally: 
        # close the connection
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()