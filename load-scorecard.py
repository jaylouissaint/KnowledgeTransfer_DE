# Your driver code (including input / output handling) should go here
import sys
import sql_queries as query
import cleaning_collegescorecard as clean_cs
import collegescorecard_utils as utils
import time
import re


def main():
    # Get csv filename
    if len(sys.argv) < 2:
        print("Error: Missing CSV file argument.")
        sys.exit(1)
    filename = sys.argv[1]
    # get the year from the filename
    match = re.search(r"(\d{4})_(\d{2})", filename)
    start, end = match.groups()
    year = f"{start}-{start[:2]}{end}"

    try:
        start_time = time.time()
        # Load csv data into a df
        scorecard_data = utils.load_data(filename)

        # clean data
        institutions_clean = clean_cs.clean_institutions(scorecard_data, year)
        academics_clean = clean_cs.clean_academics(scorecard_data, year)
        demographics_clean = clean_cs.clean_demographics(scorecard_data, year)
        financials_clean = clean_cs.clean_financials(scorecard_data, year)

        print("Data cleaned successfully.")

        # create the tables if they do not exist
        utils.create_table(query.CREATE_INSTITUTIONS)
        utils.create_table(query.CREATE_ACADEMICS)
        utils.create_table(query.CREATE_FINANCIALS)
        utils.create_table(query.CREATE_DEMOGRAPHICS)

        print("All necessary tables created or already exists.")

        # insert the new data into the tables
        utils.insert_data(query.INSERT_INSTITUTIONS, institutions_clean)
        utils.insert_data(query.INSERT_FINANCIALS, financials_clean)
        utils.insert_data(query.INSERT_DEMOGRAPHICS, demographics_clean)
        utils.insert_data(query.INSERT_ACADEMICS, academics_clean)

        # update the existing data using most recent data
        utils.update_data(query.INSERT_INSTITUTIONS, institutions_clean)
        utils.update_data(query.INSERT_FINANCIALS, financials_clean)
        utils.update_data(query.INSERT_DEMOGRAPHICS, demographics_clean)
        utils.update_data(query.INSERT_ACADEMICS, academics_clean)

        print("Data loading complete.")

        # Calculate time elapsed to load this file
        elapsed_time = time.time() - start_time
        print(f"{elapsed_time} taken to load data file.")

    except Exception as e:
        print("ETL Pipeline failed:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
