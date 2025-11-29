
import pandas as pd
import numpy as np
import psycopg
import altair as alt
import os
import re
import load_data.util_package.credentials as credentials
import load_data.util_package.logging as log

def get_connection():
    """
    Establish and return a PostgreSQL database connection.
    Uses credentials from config/credentials.py.
    This function can be imported and reused throughout the ETL pipeline.
    """
    conn = psycopg.connect(host="debprodserver.postgres.database.azure.com",
                           dbname=credentials.DB_USER,
                           user=credentials.DB_USER,
                           password=credentials.DB_PASSWORD)
    return conn


def query_data(query: str, params: tuple = None) -> pd.DataFrame:
    """
    Execute a SQL query and return the result as a pandas DataFrame.
    """
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn, params=params)
    finally:
        conn.close()
    return df


def make_tuition_adm_plot(
    df,
    institution_selected=None,
    state_selected=None,
    tuition_type="In-state"
):

    df = df.copy()

    # Determine which tuition column to use
    if tuition_type == "In-state":
        tuition_col = "tuitionfee_in"
    else:
        tuition_col = "tuitionfee_out"

    # Create a boolean column indicating how points should be highlighted
    selected_mask_state = pd.Series(False, index=df.index)
    selected_mask_inst = pd.Series(False, index=df.index)

    if institution_selected:
        selected_mask_inst |= df["unitid"] == institution_selected

    if state_selected:
        selected_mask_state |= df["stabbr"] == state_selected

    # NEW: 3-category column
    df["selection_status"] = np.select(
        [
            selected_mask_inst,      # institution selected
            selected_mask_state      # state selected
        ],
        [
            "institution",           # label 1
            "state"                  # label 2
        ],
        default="none"               # no highlight
    )

    color_scale = alt.Scale(
        domain=["institution", "state", "none"],
        range=["red", "steelblue", "lightgray"]
    )
    # Base scatterplot
    base = (
        alt.Chart(df)
        .mark_circle(size=60)
        .encode(
            x=alt.X(f"{tuition_col}:Q", title=f"{tuition_type} Tuition Fee"),
            y=alt.Y("adm_rate:Q", title="Admission Rate"),
            tooltip=["instnm", "stabbr", tuition_col, "adm_rate"],
            color=alt.Color("selection_status:N", scale=color_scale),
            opacity=alt.condition(
                alt.datum.selection_status != "none",
                alt.value(1.0),
                alt.value(0.5)
            )
        )
    )

    # Labels for selected institutions
    labels = (
        alt.Chart(df[df["selection_status"] == "institution"])
        .mark_text(dx=5, dy=-5)
        .encode(
            x=f"{tuition_col}:Q",
            y="adm_rate:Q"
        )
    )

    return (base + labels).interactive()
