# Driver code to load IPEDS data using the shared utilities module
import sys
import time
import re
import sql_queries as query      # <-- your IPEDS CREATE/INSERT SQL here
import cleaning_ipeds as clean_ipeds   # <-- your clean_directory function
import ipeds_utils as utils  # <-- the utilities module you showed


def main():
    # Get csv filename from command-line args
    if len(sys.argv) < 2:
        print("Error: Missing CSV file argument.")
        sys.exit(1)

    filename = sys.argv[1]

    # Extract 4-digit year from filename (e.g., hd2022.csv -> 2022)
    match = re.search(r"(\d{4})", filename)
    if not match:
        print("Error: Could not extract 4-digit year from filename.")
        sys.exit(1)
    year = match.group(1)

    try:
        start_time = time.time()

        # Load CSV data into a DataFrame
        # NOTE: utils.load_data currently requires OPEID + UNITID columns.
        # For pure IPEDS files, you may want to relax that inside utilities.
        ipeds_raw = utils.load_data(filename, year)

        # Clean data for the IPEDS directory table
        directory_clean = clean_ipeds.clean_directory(ipeds_raw)

        print("IPEDS directory data cleaned successfully.\n")

        # Create the directory table if it does not exist
        utils.create_table(query.CREATE_INSTITUTIONS_IPEDS)
        print("IPEDS directory table created or already exists.\n")

        # Insert the cleaned directory data
        utils.insert_data(query.INSERT_INSTITUTIONS_IPEDS, directory_clean)

        print("\nIPEDS directory data loading complete.\n")

        # Calculate time elapsed to load this file
        elapsed_time = time.time() - start_time
        print(f"{elapsed_time} seconds taken to load IPEDS data file.")

    except Exception as e:
        print("IPEDS ETL Pipeline failed:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
