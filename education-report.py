import streamlit as st
import altair as alt
import load_data.util_package.dashboard_utils as utils
import load_data.util_package.sql_queries as queries

st.title("📊 PostgreSQL Data Visualization with Streamlit")

# ---- Global filter(s) ----
st.sidebar.header("Filters")

available_years = utils.query_data(queries.get_years, params=())
years = sorted(available_years["year"].unique())
default_index = years.index(max(years))
selected_year = st.sidebar.selectbox(
    "Year (Start of Academic Year)",
    options=years,   # or simply available_years["year"]
    index=default_index  # default selection (you can change this)
)

available_states_in_year = utils.query_data(queries.get_states, params=(selected_year,))
selected_state = st.sidebar.selectbox(
    "State",
    options=available_states_in_year["stabbr"],
    index=0
)

available_institution = utils.query_data(queries.get_institutes, params = (selected_year, selected_state))
selected_institution = st.sidebar.selectbox(
    "Institution",
    options=available_institution["instnm"],
    index=0
)

if selected_institution:
    selected_institution_unitid = available_institution.loc[
        available_institution["instnm"] == selected_institution,
        "unitid"
        ].iloc[0]

# PLOT 1
st.subheader("Institutions by State and Type")
"""
Summaries of how many colleges and universities are included in the data
for the selected year, by state and type of institution (private, public, for-profit, and so on
"""

# Get necessary data
query = queries.year_institute_summary
df = utils.query_data(query, params=(selected_year,))

df = df.rename(columns={
    "control": "Type",
    "stabbr": "State",
    "count": "Institution Count"
})

df["Type"] = df["Type"].fillna("Unknown")

pivot_df = (
    df.pivot_table(
        index="State",
        columns="Type",
        values="Institution Count",
        aggfunc="sum",
        fill_value=0
    )
    .reset_index()
)

pivot_df.columns.name = None

st.dataframe(pivot_df, use_container_width=True, hide_index=True)

# PLOT 2
st.subheader("Summaries of tuition rate")
"""
Summaries of current college tuition rates, by state and Carnegie Classification of institution.
"""

# PLOT 3
st.subheader("Summary table of loan repayment performances")
"""
A table showing the best- and worst-performing institutions by loan repayment rates.
"""

# PLOT 4
st.subheader("Tutition rates and loan repayment rates over time")
"""
Graphs showing how tuition rates and loan repayment rates have changed over time, 
either in aggregate (such as averages for all institutions by type) or for selected institutions (such as the most expensive).
"""

# PLOT 5
st.subheader("Carnegie Classification and Average SAT score")

# PLOT 6
st.subheader("Admission Rates and Tuition Fees")
"""
Scatterplot showing correlation between admission rate and tuition fees
"""

# PLOT 7
st.subheader("Map of Faculty Salaries")
"""
Map showing faculty salaries across the US
"""
