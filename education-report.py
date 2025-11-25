import streamlit as st
import altair as alt
import load_data.util_package.collegescorecard_utils as utils

st.title("📊 PostgreSQL Data Visualization with Streamlit")

# Example query — change to your table
query = "SELECT * FROM financials LIMIT 500;"

df = utils.query_data(query)

st.subheader("Raw Data")
st.dataframe(df)

# Visualization
st.subheader("Data Visualization")

chart = (
    alt.Chart(df)
    .mark_line(point=True)
    .encode(
        x="created_at:T",
        y="value:Q",
        tooltip=["id", "value", "created_at"]
    )
    .interactive()
)

st.altair_chart(chart, use_container_width=True)
