"""
Functions to transform raw dataframe from college scorecard csv
into insert-ready dataframe for different tables.
"""

import pandas as pd


def clean_directory(df):
    """
    Input: Raw dataframe from college-scorecard csv
    Output: Cleaned dataframe containing columns for an InstitutionDirectory table
    (name, address, URLs, contact info, basic location fields).
    """

    # Identifier + main columns based on your snippet
    id_cols = ['UNITID']
    main_cols = [
        'INSTNM',    # Institution name
        'ADDR',      # Street address
        'CITY',
        'STABBR',    # State abbreviation
        'ZIP',
        'LONGITUD',
        'LATITUDE',
        'C21BASIC',
        '21IPUG',
        'C21UGPRF',
        'C21ENPRF',
        'C21SZSET',
        'CSA',
        'CBSA',
        'CBSATYPE',
        'FIPS',
        'COUNTYCD'
    ]
 
    try:
        # Obtain relevant columns
        sub_df = df[id_cols + main_cols].copy()
    except KeyError as e:
        raise KeyError(f"Missing required columns for institution directory: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")
        raise

    # --- Basic cleaning ---

    # 1. Normalize string columns: strip whitespace and turn "" or " " into None
    obj_cols = sub_df.select_dtypes(include="object").columns
    for col in obj_cols:
        sub_df[col] = (
            sub_df[col]
            .astype("string")       # ensure string dtype
            .str.strip()            # remove surrounding spaces
            .replace({"": pd.NA})   # blank strings -> NA
        )

    # 2. Make sure ZIP and OPEID are treated as strings (keep leading zeros)
    for col in ['ZIP', 'OPEID']:
        if col in sub_df.columns:
            sub_df[col] = sub_df[col].astype("string")

    # 3. Convert NA values to None for psycopg2
    sub_df = sub_df.astype(object).where(pd.notnull(sub_df), None)

    return sub_df
