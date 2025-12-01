import streamlit as st
import load_data.util_package.dashboard_utils as utils
import load_data.util_package.sql_queries as queries
import pydeck as pdk
import altair as alt
import pandas as pd


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
                                            params=())
selected_state = st.sidebar.selectbox(
    "State",
    options=[""] + available_states_in_year["stabbr"].to_list(),
    index=0
)

if selected_state != "":
    available_institution = utils.query_data(queries.get_institutes_by_state,
                                             params=(selected_state,))
else:
    available_institution = utils.query_data(queries.get_all_institutes,
                                             params=())
    print(available_institution)

selected_institution = st.sidebar.selectbox(
    "Institution",
    options=[""] + available_institution["instnm"].tolist(),
    index=0
)

if selected_institution != "":
    selected_institution_unitid = available_institution.loc[
        available_institution["instnm"] == selected_institution,
        "unitid"
        ].iloc[0]
else:
    selected_institution_unitid = None

# PLOT 1
"""
Summaries of how many colleges and universities are included in the data
for the selected year, by state and type of institution (private, public,
for-profit, and so on)
"""

most_recent_year = queries.get_most_recent_year
max_year = utils.query_data(most_recent_year, params=())['max'].to_list()[0]
st.subheader(f"Institutions by State and Type ({max_year})")

# Get necessary data
inst_summary_query = queries.year_institute_summary
df = utils.query_data(inst_summary_query, params=(max_year,))

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
                                      params=(selected_year,
                                              selected_state,
                                              selected_institution))

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
loan_perf_query = queries.loan_repayment_performance
loan_df = utils.query_data(loan_perf_query, params=(selected_year,))

if loan_df.empty:
    st.info("No loan repayment data available for the selected year.")
else:
    # filter by state if one is selected
    if selected_state:
        loan_df = loan_df[loan_df["stabbr"] == selected_state]

    # Clean up / rename columns
    loan_df = loan_df.rename(columns={
        "instnm": "Institution",
        "stabbr": "State",
        "control": "Type",
        "repayment_rate": "Repayment Rate"
    })

    # If repayment is 0–1, you can convert to %
    if loan_df["Repayment Rate"].max() <= 1.0:
        loan_df["Repayment Rate"] = loan_df["Repayment Rate"] * 100

    # Sort for best / worst
    top_n = 10
    best_df = (
        loan_df.sort_values("Repayment Rate", ascending=False)
        .head(top_n)
    )
    worst_df = (
        loan_df.sort_values("Repayment Rate", ascending=True)
        .head(top_n)
    )

    # Display side by side
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Best-performing institutions (by repayment rate)**")
        st.dataframe(
            best_df[["Institution", "State", "Type", "Repayment Rate"]],
            use_container_width=True,
            hide_index=True
        )

    with col2:
        st.markdown("**Worst-performing institutions (by repayment rate)**")
        st.dataframe(
            worst_df[["Institution", "State", "Type", "Repayment Rate"]],
            use_container_width=True,
            hide_index=True
        )


# PLOT 4
st.subheader("Tutition rates and loan repayment rates over time")
"""
Graphs showing how tuition rates and loan repayment rates changed over time,
in aggregate (such as averages for all institutions by type)
"""
# Controls: choose aggregation level and tuition type
agg_level = st.radio(
    "Aggregate by:",
    options=["All Institutions", "Institution Type (Control)"],
    horizontal=True
)

tuition_type_over_time = st.radio(
    "Tuition to plot:",
    options=["In-state", "Out-of-state"],
    horizontal=True,
    key="tuition_over_time"
)


tuition_repay_query = queries.tuition_repayment_over_time

tuition_repay_df = utils.query_data(
    tuition_repay_query,
    params=()
)


# state filter
if selected_state:
    tuition_repay_df = tuition_repay_df[tuition_repay_df["stabbr"]
                                        == selected_state]

if tuition_repay_df.empty:
    st.info("No tuition/repayment trend data available")
else:
    # Basic cleanup / rename
    tuition_repay_df = tuition_repay_df.rename(columns={
        "year": "Year",
        "control": "Type",
        "avg_in_state_tuition": "Avg In-State Tuition",
        "avg_out_state_tuition": "Avg Out-of-State Tuition",
        "avg_repayment_rate": "Avg Repayment Rate"
    })

    # If Avg Repayment is 0–1, convert to percentage
    if tuition_repay_df["Avg Repayment Rate"].max() <= 1.0:
        tuition_repay_df["Avg Repayment Rate"] = (
            tuition_repay_df["Avg Repayment Rate"] * 100
        )

    # Choose tuition metric
    if tuition_type_over_time == "In-state":
        tuition_col = "Avg In-State Tuition"
    else:
        tuition_col = "Avg Out-of-State Tuition"

    # Decide grouping based on aggregation level
    if agg_level == "All Institutions":
        # Aggregate across all Types if not already aggregated
        group_cols = ["Year"]
        df_agg = (tuition_repay_df
                  .groupby(group_cols, as_index=False)
                  .agg({
                      tuition_col: "mean",
                      "Avg Repayment Rate": "mean"
                  }))
        color_encoding = alt.value("steelblue")  # single color
    else:
        # Group by Year + Type (Public / Private / For-profit)
        group_cols = ["Year", "Type"]
        df_agg = (tuition_repay_df
                  .groupby(group_cols, as_index=False)
                  .agg({
                      tuition_col: "mean",
                      "Avg Repayment Rate": "mean"
                  }))
        color_encoding = "Type:N"

    # Tuition chart over time
    tuition_chart = (
    alt.Chart(df_agg)
    .mark_line(point=True)
    .encode(
        x=alt.X("Year:O", title="Year"),
        y=alt.Y(
            f"{tuition_col}:Q",
            title=tuition_col,
            scale=alt.Scale(domain=[5000, df_agg[tuition_col].max()])
        ),
        color=color_encoding,
        tooltip=(
            ["Year", "Type", tuition_col]
            if "Type" in df_agg.columns
            else ["Year", tuition_col]
        )
    )
    .properties(
        height=250,
        title="Average Tuition Over Time"
    )
)

    # Repayment chart over time
    repay_chart = (
        alt.Chart(df_agg)
        .mark_line(point=True)
        .encode(
            x=alt.X("Year:O", title="Year"),
            y=alt.Y(
                "Avg Repayment Rate:Q",
                title="Avg Repayment Rate (%)",
                scale=alt.Scale(domain=[70, df_agg["Avg Repayment Rate"].max()])
            ),
            color=color_encoding,
            tooltip=(["Year", "Type", "Avg Repayment Rate"]
                    if "Type" in df_agg.columns
                    else ["Year", "Avg Repayment Rate"])
        )
        .properties(
            height=250,
            title="Average Loan Repayment Rate Over Time"
        )
    )

    st.altair_chart(tuition_chart & repay_chart, use_container_width=True)

# PLOT 5
st.subheader("Carnegie Classification and Average SAT score")
""" Table showing the average SAT scores for colleges with
each Carnegie Basic Classification
"""
car_sat_summary_query = queries.SAT_avg_carnegie
car_sat_summary_df = utils.query_data(car_sat_summary_query)

car_sat_summary_df = car_sat_summary_df.rename(
    columns={"carnegie_basic": "Carnegie Classification",
             "avg_sat_score": "Average SAT Score"})
car_sat_summary_df["Average SAT Score"] = (
    car_sat_summary_df["Average SAT Score"].round(0))

st.data_editor(car_sat_summary_df, use_container_width=True, hide_index=True)


# PLOT 6
st.subheader("Admission Rates and Tuition Fees")
"""
Scatterplot showing correlation between admission rate and tuition fees
"""
tuition_type = st.radio(
    "Select tuition type:",
    options=["In-state", "Out-of-state"],
    horizontal=True
)

rate_fee_query = queries.tuition_admrate
df = utils.query_data(rate_fee_query, params=(selected_year,))


chart = utils.make_tuition_adm_plot(
    df,
    institution_selected=selected_institution_unitid,
    state_selected=selected_state,
    tuition_type=tuition_type
)

st.altair_chart(chart, use_container_width=True)

# PLOT 7
st.subheader("Map of Faculty Salaries")
"""
Map showing faculty salaries across the US
"""
faculty_salary_query = queries.faculty_salary_map
map_faculty_salary_df = utils.query_data(faculty_salary_query,
                                         params=(selected_year,
                                                 selected_state,
                                                 selected_institution))
map_faculty_salary_df["avg_faculty_salary"] = pd.to_numeric(
    map_faculty_salary_df["avg_faculty_salary"], errors="coerce")

# Normalize salary → darker color for higher salary
min_sal = map_faculty_salary_df["avg_faculty_salary"].min()
max_sal = map_faculty_salary_df["avg_faculty_salary"].max()


def salary_to_color(s):
    if min_sal == max_sal:
        return [200, 200, 200]   # light grey for missing salary
    return [
        0,
        int(100 + 155 * (s - min_sal) / (max_sal - min_sal)),
        int(180 - 120 * (s - min_sal) / (max_sal - min_sal))
    ]


map_faculty_salary_df["color"] = map_faculty_salary_df["avg_faculty_salary"
                                                       ].apply(salary_to_color)
print(map_faculty_salary_df["color"])

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

low_color = salary_to_color(min_sal)
high_color = salary_to_color(max_sal)

low_rgb = f"rgb({low_color[0]}, {low_color[1]}, {low_color[2]})"
high_rgb = f"rgb({high_color[0]}, {high_color[1]}, {high_color[2]})"

st.markdown(
    f"""
    <div style="margin-top:20px;">
        <b>Legend: Faculty Salary Range</b>
        <div style="
            height: 20px;
            background: linear-gradient(to right, {low_rgb}, {high_rgb});
            border: 1px solid #aaa;
            margin-top: 5px;
        "></div>
        <div style="
            display: flex;
            justify-content: space-between;
            font-size: 15px;
            color: #222;
            font-weight: 600;
            margin-top: 4px;
        ">
            <span>${min_sal:,.0f}</span>
            <span>${(min_sal+max_sal)/2:,.0f}</span>
            <span>${max_sal:,.0f}</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True)