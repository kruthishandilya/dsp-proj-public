import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta

API_URL = "http://model_service:8000"

st.set_page_config(page_title="Past Predictions", layout="wide")
st.header("Past Predictions")

st.caption(
    "View historical predictions made via the webapp or scheduled prediction job."
)

col1, col2, col3 = st.columns(3)

with col1:
    source = st.selectbox(
        "Prediction Source",
        ["all", "webapp", "scheduled"],
        help="'webapp' = manual predictions, 'scheduled' = from prediction DAG"
    )

with col2:
    start_date = st.date_input(
        "Start Date",
        value=datetime.now() - timedelta(days=7),
        help="Show predictions from this date"
    )

with col3:
    end_date = st.date_input(
        "End Date",
        value=datetime.now(),
        help="Show predictions up to this date"
    )

limit = st.slider("Number of records to display", 1, 200, 50)

if st.button("Fetch Predictions"):
    try:
        params = {
            "limit": limit,
            "source": source,
            "start_date": str(start_date),
            "end_date": str(end_date),
        }

        response = requests.get(
            f"{API_URL}/past-predictions", params=params, timeout=10
        )
        response.raise_for_status()

        data = response.json()

        if len(data["results"]) == 0:
            st.info("No predictions found for the selected filters.")
        else:
            df = pd.DataFrame(data["results"])
            st.write(f"Showing {len(df)} predictions")
            st.dataframe(df, use_container_width=True)

    except requests.exceptions.ConnectionError:
        st.error("API not reachable. Is FastAPI running?")
    except Exception as e:
        st.error(f"Error: {e}")
