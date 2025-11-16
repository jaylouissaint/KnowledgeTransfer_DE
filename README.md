# College Scorecard Database
These programs will load, extract, clean, and query data two types of 
college scorecards into SQL tables 


## Table Schema
Two tables with institutional data: Institutions and Institutions_IPEDS
Financials (update yearly) REFERENCES Institutions by UNITID
Academics (update yearly) REFERENCES Institudions by UNITID
Demographics (update yearly) REFERENCES Institutions by UNITID

## Installation
Requires Python 3.13, psycopg, pandas, os.  Please see YAML file.


## Usage
Run the program
python load_scorecard.py path/to/MERGEDYYYY_AA_PP.csv
to update tables with data from Collegescorecard "MERGEDYYYY_AA_PP.csv

Run the program
python load_ipeds.py path/to/HDYYYY.csv
to update tables with data from IPEDS file "HDYYYY.csv"


## File Structure
collegescorecard_utils        - Utility package to support other College Scorecard programs
cleaning_collegescorecard.py  - Cleans data specifically from the College Scorecard csv
load_scorecard.py             - Controller for CollegeScorecard extraction, cleaning, operations

ipeds_utils.py                - utility package to support other IPEDS scorecard programs
cleaning_ipeds.py             - cleans data specifically from the IPEDS Scorecard csv
load_ipeds.py                 - Controller for IPEDS extraction, cleaning, operations

database_design.ipynb         - Database & table design
sql_queries.py                - SQL queries to insert, update, and delete data


## Data Sources
The college scorecard database consists of two main sources of data:
1) The IPEDS Sheets 
    a) naming convention "HDYYYY"
        i) YYYY represents the year of the start of the school year
2) College Scorecard Sheets 
    a) naming convention "MERGEDYYYY_AA_PP"
        i) YYYY represents the start year of the school year
        ii) AA represents the end year of the school year

