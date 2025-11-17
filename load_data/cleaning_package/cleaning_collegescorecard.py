"""
Functions to transform raw dataframe from college scorecard csv
into insert-ready dataframe for different tables.
"""
import pandas as pd


def clean_institutions(df):
    """
    Input: Raw dataframe from college-scorecard csv
    Output: Cleaned dataframe containing columns for the Institutions table
    """

    # Specify columns that are needed in this table
    main_cols = ['OPEID', 'UNITID', 'ACCREDAGENCY', 'PREDDEG', 'HIGHDEG',
                 'CONTROL', 'REGION']

    # Mapping settings for categorical columns
    mapping = {
        "PREDDEG": {
            0: "Not classified",
            1: "Predominantly certificate-degree granting",
            2: "Predominantly associate's-degree granting",
            3: "Predominantly bachelor's-degree granting",
            4: "Entirely graduate-degree granting"
        },
        "HIGHDEG": {
            0: "Non-degree-granting",
            1: "Certificate degree",
            2: "Associate degree",
            3: "Bachelor's degree",
            4: "Graduate degree"
        },
        "CONTROL": {
            1: "Public",
            2: "Private nonprofit",
            3: "Private for-profit"
        },
        "REGION": {
            0: "U.S. Service Schools",
            1: "New England (CT, ME, MA, NH, RI, VT)",
            2: "Mid East (DE, DC, MD, NJ, NY, PA)",
            3: "Great Lakes (IL, IN, MI, OH, WI)",
            4: "Plains (IA, KS, MN, MO, NE, ND, SD)",
            5: "Southeast (AL, AR, FL, GA, KY, LA, MS, NC, SC, TN, VA, WV)",
            6: "Southwest (AZ, NM, OK, TX)",
            7: "Rocky Mountains (CO, ID, MT, UT, WY)",
            8: "Far West (AK, CA, HI, NV, OR, WA)",
            9: "Outlying Areas (AS, FM, GU, MH, MP, PR, PW, VI)"
        }
    }

    try:
        # Obtain relevant columns
        sub_df = df[main_cols].copy()
        sub_df.loc[:, 'LAST_REPORTED'] = df['YEAR']
    except KeyError as e:
        raise KeyError(f"Missing required columns for institutions: {e}")
    except Exception as e:
        print((f"Unexpected Error: {e}"))
        raise

    # Map categorical variables to relevant strings
    for cat_col in mapping.keys():
        sub_df[cat_col] = sub_df[cat_col].map(mapping[cat_col])

    # Convert NA values to None (for psycopg2)
    sub_df = sub_df.replace({pd.NA: None})

    return sub_df


def clean_financials(df):
    """
    Input: Raw dataframe from college-scorecard csv
    Output: Cleaned dataframe containing columns for the Financials table
    """

    # Specify columns that are needed in this table
    id_cols = ['UNITID', 'YEAR']
    main_cols = ['TUITIONFEE_IN',
                 'TUITIONFEE_OUT', 'TUITIONFEE_PROG', 'TUITFTE',
                 'AVGFACSAL', 'CDR2', 'CDR3']

    try:
        # Obtain relevant columns
        sub_df = df[id_cols + main_cols]
    except KeyError as e:
        raise KeyError(f"Missing required columns for financials: {e}")
    except Exception as e:
        print((f"Unexpected Error: {e}"))
        raise

    # Drop rows where all relevant columns are NULLs
    sub_df = sub_df.dropna(subset=main_cols, how='all')

    # Convert NA values to None (for psycopg2)
    sub_df = sub_df.astype(object).where(pd.notnull(sub_df), None)

    return sub_df


def clean_academics(df):
    """
    Input: Raw dataframe from college-scorecard csv
    Output: Cleaned dataframe containing columns for the Academics table
    """

    # Specify columns that are needed in this table
    id_cols = ['UNITID', 'YEAR']
    main_cols = ['ADM_RATE', 'C100_4', 'C100_L4',
                 'SAT_AVG', 'COUNT_NWNE_3YR', 'COUNT_WNE_3YR',
                 'CNTOVER150_3YR']

    try:
        # Obtain relevant columns
        sub_df = df[id_cols + main_cols]
    except KeyError as e:
        raise KeyError(f"Missing required columns for academics: {e}")
    except Exception as e:
        print((f"Unexpected Error: {e}"))
        raise

    # Drop rows where all relevant columns are NULLs
    sub_df = sub_df.dropna(subset=main_cols, how='all')

    # Convert NA values to None (for psycopg2)
    sub_df = sub_df.astype(object).where(pd.notnull(sub_df), None)

    return sub_df


def clean_demographics(df):
    """
    Input: Raw dataframe from college-scorecard csv
    Output: Cleaned dataframe containing columns for the Demographics table
    """

    # Specify columns that are needed in this table
    id_cols = ['UNITID', 'YEAR']
    main_cols = ['UGDS', 'UGDS_MEN', 'UGDS_WOMEN',
                 'UGDS_WHITE', 'UGDS_BLACK', 'UGDS_HISP', 'UGDS_ASIAN',
                 'UGDS_AIAN', 'UGDS_NHPI', 'UGDS_2MOR', 'UGDS_UNKN',
                 'IRPS_MEN', 'IRPS_WOMEN', 'IRPS_WHITE', 'IRPS_BLACK',
                 'IRPS_HISP', 'IRPS_ASIAN', 'IRPS_AIAN', 'IRPS_NHPI',
                 'IRPS_2MOR', 'IRPS_UNKN']

    try:
        # Obtain relevant columns
        sub_df = df[id_cols + main_cols]
    except KeyError as e:
        raise KeyError(f"Missing required columns for demographics: {e}")
    except Exception as e:
        print((f"Unexpected Error: {e}"))
        raise

    # Drop rows where all relevant columns are NULLs
    sub_df = sub_df.dropna(subset=main_cols, how='all')

    # Convert NA values to None (for psycopg2)
    sub_df = sub_df.astype(object).where(pd.notnull(sub_df), None)

    return sub_df
