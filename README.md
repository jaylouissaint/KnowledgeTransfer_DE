# College Scorecard ETL Pipeline
This project develops an end-to-end Extract, Transform, and Load (ETL) pipeline designed to ingesting messy, unstructured higher-education data and producing a clean, well-structured SQL database for analysis. The pipeline processes annual data from two major U.S Department of Education sources: 
1. Department of Education's Colledge Scorecard - provides detailed performance metrics for colleges and universities across the United States, updated annually. 
2. Integrated Postsecondary Data System (IPEDS) - provides annual directory information, survey-based institutional statistics.

## Purpose and Design
The ETL pipeline is structured to support government authorities, policy analysts, and researchers who need to monitor institutional trends over time and make data-driven decisions. The underlying data schema enables longitudinal and cross-institution analysis, including:
* Student enrollment changes
* Graduation and retention rates
* Financial aid distribution and affordability metrics
* Faculty, staffing, and student characteristics
* Multi-year trend comparison across campuses and systems

By integrating, standardizing, and validating multiple datasets, the pipeline ensures analysts can reliably query, analyze, and report on the performance of higher-education institutions nationwide.

## Table Schema
* Institutions: This table stores institution identifiers and high-level characteristics (degree offerings, accreditation) coming from College Scorecard.
* Institutions_IPEDS: This table stores directory information (address, geolocation) and classification fields coming specifically from IPEDS.
* Financials: This table is stores yearly university financial information such as tuition fees, faculty salaries, and so on.
  * Joins to Institutions and Institutions_IPEDS ON UNITID 
* Academics: This table stores yearly university academic information such as admission rate, graduation rate, and so on.
  * Joins to Institutions and Institutions_IPEDS ON UNITID 
* Demographics: This table stores yearly university demographic information such as the percentage of different ethnicities.
  * Joins to Institutions and Institutions_IPEDS ON UNITID 

## Installation
Requires Python 3.13, psycopg, pandas, os.  Please see YAML file.
Or use the provided virtual environment with the following command:
```
conda env create -f environment.college_scorecard.yml 
conda activate scorecard_env
```

## Credentials
Create your own credential file `credentials.py` in the `load_data/util_package` folder. We are assuming the username and database name is the same. The username and password in the credentials file should be stored as: 
```
DB_USER = "yourusername"
DB_PASSWORD = "yourpassword"
```

## Usage
Run the code below to update tables with data from Collegescorecard "MERGEDYYYY_AA_PP.csv file. 
```
python load_scorecard.py path/to/MERGEDYYYY_AA_PP.csv
```

Run the code below to update tables with data from IPEDS "HDYYYY.csv" file. 
```
python load_ipeds.py path/to/HDYYYY.csv
```

## File Structure
### 1. Utility Files
* collegescorecard_utils        - Utility package to support other College Scorecard programs
* ipeds_utils.py                - utility package to support other IPEDS scorecard programs
* database_design.ipynb         - Database & table design
* sql_queries.py                - SQL queries to insert, update, and delete data

### 2. Cleaning Files
* cleaning_ipeds.py             - cleans data specifically from the IPEDS Scorecard csv
* cleaning_collegescorecard.py  - Cleans data specifically from the College Scorecard csv

### 3. Driver Files
* load_ipeds.py                 - Controller for IPEDS extraction, cleaning, operations
* load_scorecard.py             - Controller for CollegeScorecard extraction, cleaning, operations

## Data Sources
The college scorecard database consists of two main sources of data:
1) The IPEDS Sheets 
    *a) naming convention "HDYYYY"
        *i) YYYY represents the year of the start of the school year
2) College Scorecard Sheets 
    *a) naming convention "MERGEDYYYY_AA_PP"
        *i) YYYY represents the start year of the school year
        *ii) AA represents the end year of the school year

