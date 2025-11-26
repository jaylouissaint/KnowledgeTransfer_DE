import streamlit as st
import load_data.util_package.dashboard_utils as utils
import load_data.util_package.sql_queries as queries
import pydeck as pdk

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

available_states_in_year = utils.query_data(queries.get_states,
                                            params=(selected_year,))
selected_state = st.sidebar.selectbox(
    "State",
    options=available_states_in_year["stabbr"],
    index=0
)

available_institution = utils.query_data(queries.get_institutes,
                                         params=(selected_year,
                                                 selected_state))
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
for the selected year, by state and type of institution (private, public,
for-profit, and so on)
"""

# Get necessary data
inst_summary_query = queries.year_institute_summary
df = utils.query_data(inst_summary_query, params=(selected_year,))

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
Summaries of current college tuition rates, by state and Carnegie
Classification of institution.
"""
# Get necessary data
tuition_summary_query = queries.tuition_rate_summary
tuition_summary_df = utils.query_data(tuition_summary_query,
                                      params=(selected_year,))

# map to get "$"
tuition_summary_df["avg_in_state_tuition"] = tuition_summary_df[
    "avg_in_state_tuition"].map("${:,.0f}".format)
tuition_summary_df["avg_out_state_tuition"] = tuition_summary_df[
    "avg_out_state_tuition"].map("${:,.0f}".format)

tuition_summary_df = tuition_summary_df.rename(columns={
    "stabbr": "State",
    "c_basic": "Carnegie Classification",
    "avg_in_state_tuition": "Avg In-State Tuition",
    "avg_out_state_tuition": "Avg Out of State Tuition"
})

st.dataframe(tuition_summary_df, use_container_width=True, hide_index=True)

# PLOT 3
st.subheader("Summary table of loan repayment performances")
"""
A table showing the best- and worst-performing institutions
by loan repayment rates.
"""

# PLOT 4
st.subheader("Tutition rates and loan repayment rates over time")
"""
Graphs showing how tuition rates and loan repayment rates changed over time,
either in aggregate (such as averages for all institutions by type) or for
selected institutions (such as the most expensive).
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
faculty_salary_query = queries.faculty_salary_map
map_faculty_salary_df = utils.query_data(faculty_salary_query,
                                         params=(selected_year,))

# Normalize salary → darker color for higher salary
min_sal = map_faculty_salary_df["avg_faculty_salary"].min()
max_sal = map_faculty_salary_df["avg_faculty_salary"].max()

map_faculty_salary_df["color"] = map_faculty_salary_df["avg_faculty_salary"
                                                       ].apply(
    lambda s: [
        0,
        int(100 + 155 * (s - min_sal) / (max_sal - min_sal)),
        int(180 - 120 * (s - min_sal) / (max_sal - min_sal))
    ]
)

layer = pdk.Layer(
    "ScatterplotLayer",
    map_faculty_salary_df,
    get_position='[longitude, latitude]',
    get_color='color',
    get_radius=25000,
    pickable=True,
)

view_state = pdk.ViewState(
    latitude=37.5,   # Centers on USA
    longitude=-96,
    zoom=3.5,
)

st.pydeck_chart(
    pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "<b>State:</b> {stabbr}<br/>"
                    "<b>Avg Salary:</b> ${avg_faculty_salary}",
        }
    )
)
