import streamlit as st
import altair as alt
import load_data.util_package.dashboard_utils as utils
import load_data.util_package.sql_queries as queries

st.title("📊 PostgreSQL Data Visualization with Streamlit")


# ---- Global filter(s) ----
st.sidebar.header("Filters")
year = st.sidebar.number_input(
    "LAST_REPORTED year",
    min_value=2022,
    max_value=2025,
    value=2022,
    step=1,
)

"""
Summaries of how many colleges and universities are included in the data
for the selected year, by state and type of institution (private, public, for-profit, and so on
"""

query = queries.year_institute_summary   # your SQL with WHERE LAST_REPORTED = %s
df = utils.query_data(query, params=(year,))

# Optional: rename columns to nicer names
df = df.rename(columns={
    "control": "Type",
    "stabbr": "State",
    "count": "Institution Count"  # make sure your SQL aliases COUNT(*) as n_institutions
})

st.subheader("Summary table – institutions by state & control")
st.dataframe(df, use_container_width=True)

st.subheader("Institutions by State and Type (side-by-side bars)")

chart = (
    alt.Chart(df)
    .mark_bar()
    .encode(
        x=alt.X("State:N", title="State"),
        y=alt.Y("Institution Count:Q", title="Number of Institutions"),
        color=alt.Color("Type:N", title="Institution Type"),
        xOffset="Type:N",  # <-- this makes bars side-by-side instead of stacked
        tooltip=["State", "Type", "Institution Count"]
    )
    .properties(height=400)
)

st.altair_chart(chart, use_container_width=True)

"""
Summaries of current college tuition rates, by state and Carnegie Classification of institution.
"""
st.subheader("Summaries of tuition rate")

"""
A table showing the best- and worst-performing institutions by loan repayment rates.
"""
st.subheader("Summary table of loan repayment performances")


"""
Graphs showing how tuition rates and loan repayment rates have changed over time, 
either in aggregate (such as averages for all institutions by type) or for selected institutions (such as the most expensive).
"""
st.subheader("Tutition rates and loan repayment rates over time")
