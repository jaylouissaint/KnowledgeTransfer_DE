
import pandas as pd
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

    # Create a boolean column indicating which points are highlighted
    selected_mask = pd.Series(False, index=df.index)

    if institution_selected:
        selected_mask |= df["instnm"] == institution_selected

    if state_selected:
        selected_mask |= df["stabbr"] == state_selected

    # If no filter is selected → show normal scatterplot
    if not selected_mask.any():
        df["selected"] = True
    else:
        df["selected"] = selected_mask

    # Base scatterplot
    base = (
        alt.Chart(df)
        .mark_circle(size=60)
        .encode(
            x=alt.X(f"{tuition_col}:Q", title=f"{tuition_type} Tuition Fee"),
            y=alt.Y("adm_rate:Q", title="Admission Rate"),
            tooltip=["instnm", "stabbr", tuition_col, "adm_rate"],
            color=alt.condition(
                "datum.selected",
                alt.value("steelblue"),
                alt.value("lightgray")
            ),
            opacity=alt.condition(
                "datum.selected",
                alt.value(1.0),
                alt.value(0.5)
            ),
        )
    )

    # Labels for selected institutions
    labels = (
        alt.Chart(df[df["selected"]])
        .mark_text(dx=5, dy=-5)
        .encode(
            x=f"{tuition_col}:Q",
            y="adm_rate:Q"
        )
    )

    return (base + labels).interactive()
