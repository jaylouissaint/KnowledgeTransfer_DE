"""
Functions to transform raw dataframe from college scorecard csv\n
into insert-ready dataframe for different tables.
"""

def clean_institutions(df):
    """
    Input: Raw dataframe from college-scorecard csv\n
    Output: Cleaned dataframe containing columns for the Institutions table
    """

    # Specify columns that are needed in this table
    main_cols = ['OPEID', 'UNITID', 'PREDDEG', 'HIGHDEG', 'CONTROL', 'REGION']

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
        "CONTROL":{
            1: "Public",
            2: "Private nonprofit",
            3: "Private for-profit"
        },
        "REGION":{
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
        sub_df = df[main_cols]
    except KeyError as e:
        raise KeyError(f"Missing required columns in institutions dataframe: {e}")
    except Exception as e:
        print((f"Unexpected Error: {e}"))
        raise

    try:
        # Transform into correct types as specified in the table
        sub_df = sub_df.astype({'OPEID': 'Int64', 'UNITID': 'Int64'})
    except Exception as e:
        print(f"Failed to convert OPEID and UNITID to integers: {e}")
        raise

    # Map categorical variables to relevant strings
    for cat_col in mapping.keys():
        sub_df[cat_col] = sub_df[cat_col].map(mapping[cat_col])

    return sub_df


def clean_financials(df, year):
    """
    Input: Raw dataframe from college-scorecard csv and the YEAR of the data\n
    Output: Cleaned dataframe containing columns for the Financials table
    """
    df['YEAR'] = year

    # Specify columns that are needed in this table
    main_cols = ['OPEID', 'YEAR', 'TUITIONFEE_IN',
                 'TUITIONFEE_OUT', 'TUITIONFEE_PROG', 'TUITFTE',
                 'AVGFACSAL', 'CDR2', 'CDR3']

    try:
        # Obtain relevant columns
        sub_df = df[main_cols]
    except KeyError as e:
        raise KeyError(f"Missing required columns in financial dataframe: {e}")
    except Exception as e:
        print((f"Unexpected Error: {e}"))
        raise

    return sub_df


def clean_academics(df, year):
    """
    Input: Raw dataframe from college-scorecard csv and the YEAR of the data\n
    Output: Cleaned dataframe containing columns for the Academics table
    """
    df['YEAR'] = year

    # Specify columns that are needed in this table
    main_cols = ['OPEID', 'YEAR', 'ADM_RATE', 'C100_4', 'C100_L4',
                 'SAT_AVG', 'COUNT_NWNE_3YR', 'COUNT_WNE_3YR',
                 'CNTOVER150_3YR']

    try:
        # Obtain relevant columns
        sub_df = df[main_cols]
    except KeyError as e:
        raise KeyError(f"Missing required columns in financial dataframe: {e}")
    except Exception as e:
        print((f"Unexpected Error: {e}"))
        raise

    return sub_df


def clean_demographics(df, year):
    """
    Input: Raw dataframe from college-scorecard csv and the YEAR of the data\n
    Output: Cleaned dataframe containing columns for the Demographics table
    """
    df['YEAR'] = year

    # Specify columns that are needed in this table
    main_cols = ['OPEID', 'YEAR', 'UGDS', 'UGDS_MEN', 'UGDS_WOMEN',
                 'UGDS_WHITE', 'UGDS_BLACK', 'UGDS_HISP', 'UGDS_ASIAN',
                 'UGDS_AIAN', 'UGDS_NHPI', 'UGDS_2MOR', 'UGDS_UNKN',
                 'IRPS_MEN', 'IRPS_WOMEN', 'IRPS_WHITE', 'IRPS_BLACK',
                 'IRPS_HISP', 'IRPS_ASIAN', 'IRPS_AIAN', 'IRPS_NHPI',
                 'IRPS_2MOR', 'IRPS_UNKN']

    try:
        # Obtain relevant columns
        sub_df = df[main_cols]
    except KeyError as e:
        raise KeyError(f"Missing required columns in financial dataframe: {e}")
    except Exception as e:
        print((f"Unexpected Error: {e}"))
        raise

    return sub_df
