import streamlit as st

st.set_page_config(page_title="Fraud Detection App", layout="wide")

st.title("Fraud Detection System")

st.markdown("""
Welcome to the Fraud Detection System. Use the sidebar to navigate:

- **Prediction** - Make single or batch fraud predictions
- **Past Predictions** - View historical prediction results
""")
