"""
Functions to transform raw dataframe from college scorecard csv
into insert-ready dataframe for different tables.
"""

import pandas as pd


def clean_directory(df):
    """
    Input: Raw dataframe from college-scorecard csv
    Output: Cleaned dataframe containing columns for an
    InstitutionDirectory table
    (name, address, URLs, contact info, basic location fields + Carnegie codes)
    """

    # Identifier + main columns based on your snippet
    main_cols = [
        'UNITID',
        'INSTNM',
        'ADDR',
        'CITY',
        'STABBR',
        'ZIP',
        'LATITUDE',
        'LONGITUD',
        'C21BASIC',
        'C21IPUG',
        'C21UGPRF',
        'C21ENPRF',
        'C21SZSET',
        'COUNTYCD',
        'CSA',
        'CBSA',
        'CBSATYPE'
    ]

    # Mapping settings for categorical columns (Carnegie + profiles)
    mapping = {
        "C21BASIC": {
            -2: "Not applicable",
            0: "(Not classified)",
            1: "Associate's Colleges: High Transfer-High Traditional",
            2: "Associate's Colleges: High Transfer-Mixed"
            "Traditional/Nontraditional",
            3: "Associate's Colleges: High Transfer-High Nontraditional",
            4: "Associate's Colleges: Mixed "
            "Transfer/Career & Technical-High Traditional",
            5: "Associate's Colleges: Mixed "
            "Transfer/Career & Technical-Mixed Traditional/Nontraditional",
            6: "Associate's Colleges: Mixed "
            "Transfer/Career & Technical-High Nontraditional",
            7: "Associate's Colleges: "
            "High Career & Technical-High Traditional",
            8: "Associate's Colleges: "
            "High Career & Technical-Mixed Traditional/Nontraditional",
            9: "Associate's Colleges: "
            "High Career & Technical-High Nontraditional",
            10: "Special Focus Two-Year: Health Professions",
            11: "Special Focus Two-Year: Technical Professions",
            12: "Special Focus Two-Year: Arts & Design",
            13: "Special Focus Two-Year: Other Fields",
            14: "Baccalaureate/Associate's Colleges: Associate's Dominant",
            15: "Doctoral Universities: Very High Research Activity",
            16: "Doctoral Universities: High Research Activity",
            17: "Doctoral/Professional Universities",
            18: "Master's Colleges & Universities: Larger Programs",
            19: "Master's Colleges & Universities: Medium Programs",
            20: "Master's Colleges & Universities: Small Programs",
            21: "Baccalaureate Colleges: Arts & Sciences Focus",
            22: "Baccalaureate Colleges: Diverse Fields",
            23: "Baccalaureate/Associate's Colleges: "
            "Mixed Baccalaureate/Associate's",
            24: "Special Focus Four-Year: Faith-Related Institutions",
            25: "Special Focus Four-Year: Medical Schools & Centers",
            26: "Special Focus Four-Year: Other Health Professions Schools",
            27: "Special Focus Four-Year: Research Institution",
            28: "Special Focus Four-Year: "
            "Engineering and Other Technology-Related Schools",
            29: "Special Focus Four-Year: Business & Management Schools",
            30: "Special Focus Four-Year: Arts, Music & Design Schools",
            31: "Special Focus Four-Year: Law Schools",
            32: "Special Focus Four-Year: Other Special Focus Institutions",
            33: "Tribal Colleges"
        },
        "C21UGPRF": {
            -2: "Not applicable",
            0: "Not classified (Exclusively Graduate)",
            1: "Two-year, higher part-time",
            2: "Two-year, mixed part/full-time",
            3: "Two-year, medium full-time",
            4: "Two-year, higher full-time",
            5: "Four-year, higher part-time",
            6: "Four-year, medium full-time, inclusive, lower transfer-in",
            7: "Four-year, medium full-time, inclusive, higher transfer-in",
            8: "Four-year, medium full-time, selective, lower transfer-in",
            9: "Four-year, medium full-time, selective, higher transfer-in",
            10: "Four-year, full-time, inclusive, lower transfer-in",
            11: "Four-year, full-time, inclusive, higher transfer-in",
            12: "Four-year, full-time, selective, lower transfer-in",
            13: "Four-year, full-time, selective, higher transfer-in",
            14: "Four-year, full-time, more selective, lower transfer-in",
            15: "Four-year, full-time, more selective, higher transfer-in"
        },
        "C21SZSET": {
            -2: "Not applicable",
            0: "(Not classified)",
            1: "Two-year, very small",
            2: "Two-year, small",
            3: "Two-year, medium",
            4: "Two-year, large",
            5: "Two-year, very large",
            6: "Four-year, very small, primarily nonresidential",
            7: "Four-year, very small, primarily residential",
            8: "Four-year, very small, highly residential",
            9: "Four-year, small, primarily nonresidential",
            10: "Four-year, small, primarily residential",
            11: "Four-year, small, highly residential",
            12: "Four-year, medium, primarily nonresidential",
            13: "Four-year, medium, primarily residential",
            14: "Four-year, medium, highly residential",
            15: "Four-year, large, primarily nonresidential",
            16: "Four-year, large, primarily residential",
            17: "Four-year, large, highly residential",
            18: "Exclusively graduate/professional"
        },
        "C21ENPRF": {
            1: "Exclusively undergraduate two-year",
            2: "Exclusively undergraduate four-year",
            3: "Very high undergraduate",
            4: "High undergraduate",
            5: "Majority undergraduate",
            6: "Majority graduate",
            7: "Exclusively graduate",
            8: "(Not classified)",
            9: "Not applicable, not in Carnegie universe "
            "(not accredited or nondegree-granting)"
        },
        "C21IPUG": {
            1: "Associate's Colleges: High Transfer",
            2: "Associate's Colleges: Mixed Transfer/Career & Technical",
            3: "Associate's Colleges: High Career & Technical",
            4: "Special Focus: Two-Year Institution",
            5: "Baccalaureate/Associates Colleges",
            6: "Arts & sciences focus, no graduate coexistence",
            7: "Arts & sciences focus, some graduate coexistence",
            8: "Arts & sciences focus, high graduate coexistence",
            9: "Arts & sciences plus professions, no graduate coexistence",
            10: "Arts & sciences plus professions, some graduate coexistence",
            11: "Arts & sciences plus professions, high graduate coexistence",
            12: "Balanced arts & sciences/professions,"
            " no graduate coexistence",
            13: "Balanced arts & sciences/professions,"
            "some graduate coexistence",
            14: "Balanced arts & sciences/professions,"
            "high graduate coexistence",
            15: "Professions plus arts & sciences, no graduate coexistence",
            16: "Professions plus arts & sciences, some graduate coexistence",
            17: "Professions plus arts & sciences, high graduate coexistence",
            18: "Professions focus, no graduate coexistence",
            19: "Professions focus, some graduate coexistence",
            20: "Professions focus, high graduate coexistence",
            21: "Not Classified (Exclusively Graduate Programs)",
            22: "Not applicable, not in Carnegie universe "
            "(not accredited or nondegree-granting)"
        },
        "CBSA": {
            1: "Metropolitan Statistical Area",
            2: "Micropolitan Statistical Area",
            3: "Not applicable",
            4: "Not available"
        }
    }

    try:
        # Obtain relevant columns
        sub_df = df[main_cols].copy()
    except KeyError as e:
        raise KeyError(
            f"Missing required columns for institution directory: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")
        raise

    # Map categorical variables to relevant strings
    for cat_col, col_map in mapping.items():
        if cat_col in sub_df.columns:
            sub_df[cat_col] = sub_df[cat_col].map(col_map)

    # --- ZIP cleaning (separate) ---
    if 'ZIP' in sub_df.columns:
        # Normalize ZIP: ALWAYS take the first 5 characters
        sub_df['ZIP'] = sub_df['ZIP'].str[:5]

    # COUNTYCD cleaning
    if 'COUNTYCD' in sub_df.columns:
        sub_df['COUNTYCD'] = (
            sub_df['COUNTYCD']
            .astype("string")
            .str.strip()
            .replace({"": pd.NA})
            .str.zfill(5)
        )

    # Convert pandas NA to Python None (for psycopg2)
    sub_df = sub_df.astype(object).where(pd.notnull(sub_df), None)

    return sub_df
