# Your driver code (including input / output handling) should go here
import sys
import time
import re
from load_data.util_package import sql_queries as query
import load_data.cleaning_package.cleaning_collegescorecard as clean_cs
import load_data.util_package.collegescorecard_utils as utils


def main():
    # Get csv filename
    if len(sys.argv) < 2:
        print("Error: Missing CSV file argument.")
        sys.exit(1)
    filename = sys.argv[1]
    # get the year from the filename
    match = re.search(r"(\d{4})_(\d{2})", filename)
    start, end = match.groups()
    # year = f"{start}-{start[:2]}{end}"
    year = start

    try:
        start_time = time.time()
        # Load csv data into a df
        scorecard_data = utils.load_data(filename, year)

        print("Initiniating data cleaning...")
        # clean data
        institutions_clean = clean_cs.clean_institutions(scorecard_data)
        academics_clean = clean_cs.clean_academics(scorecard_data)
        demographics_clean = clean_cs.clean_demographics(scorecard_data)
        financials_clean = clean_cs.clean_financials(scorecard_data)

        print("Data cleaned successfully.\n")

        # create the tables if they do not exist
        utils.create_table(query.CREATE_INSTITUTIONS)
        utils.create_table(query.CREATE_ACADEMICS)
        utils.create_table(query.CREATE_FINANCIALS)
        utils.create_table(query.CREATE_DEMOGRAPHICS)

        print("All necessary tables created or already exists.\n")

        # insert the new data into the tables
        utils.insert_data(query.INSERT_INSTITUTIONS, institutions_clean)
        utils.insert_data(query.INSERT_FINANCIALS, financials_clean)
        utils.insert_data(query.INSERT_DEMOGRAPHICS, demographics_clean)
        utils.insert_data(query.INSERT_ACADEMICS, academics_clean)

        """
        # update the existing data using most recent data
        utils.update_data(query.INSERT_INSTITUTIONS, institutions_clean)
        utils.update_data(query.INSERT_FINANCIALS, financials_clean)
        utils.update_data(query.INSERT_DEMOGRAPHICS, demographics_clean)
        utils.update_data(query.INSERT_ACADEMICS, academics_clean)
        """

        print("\nData loading complete.\n")

        # Calculate time elapsed to load this file
        elapsed_time = time.time() - start_time
        print(f"{elapsed_time} seconds taken to load data file.")

    except Exception as e:
        print("ETL Pipeline failed:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
