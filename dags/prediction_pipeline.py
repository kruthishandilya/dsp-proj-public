from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from datetime import datetime
import os
import time
import pandas as pd
import requests


# =========================
# PATHS & CONFIG
# =========================
GOOD_PATH = "/opt/airflow/data/good_data"
TRACKER_FILE = "/opt/airflow/data/.prediction_tracker"
API_URL = "http://model_service:8000/predict"

NUMERIC_COLUMNS = [
    "amount", "account_age_days", "shipping_distance_km",
    "total_transactions_user", "avg_amount_user",
    "transaction_hour", "transaction_day",
    "promo_used", "avs_match", "three_ds_flag", "cvv_result",
]

CATEGORICAL_COLUMNS = [
    "country", "bin_country", "merchant_category", "channel",
]

REQUIRED_COLUMNS = NUMERIC_COLUMNS + CATEGORICAL_COLUMNS


# =========================
# TASK 1: CHECK FOR NEW DATA
# Uses ShortCircuitOperator to skip entire DAG run if no new files
# =========================
def check_for_new_data(**kwargs):
    # Read last processed timestamp
    last_run = 0.0
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE) as f:
            content = f.read().strip()
            if content:
                last_run = float(content)

    # Find files newer than last run
    new_files = []
    for f in os.listdir(GOOD_PATH):
        if f.endswith(".csv"):
            file_path = os.path.join(GOOD_PATH, f)
            if os.path.getmtime(file_path) > last_run:
                new_files.append(f)

    if not new_files:
        print("No new data files found - skipping DAG run")
        return False  # ShortCircuitOperator skips all downstream tasks

    print(f"Found {len(new_files)} new file(s): {new_files}")
    kwargs["ti"].xcom_push(key="files", value=new_files)
    return True


# =========================
# TASK 2: MAKE PREDICTIONS
# =========================
def make_predictions(**kwargs):
    ti = kwargs["ti"]
    files = ti.xcom_pull(task_ids="check_for_new_data", key="files")

    if not files:
        raise ValueError("No files received from check_for_new_data")

    total_predictions = 0

    for file in files:
        file_path = os.path.join(GOOD_PATH, file)

        if not os.path.exists(file_path):
            print(f"File {file} no longer exists, skipping")
            continue

        df = pd.read_csv(file_path)

        # Add missing columns with defaults
        for col in NUMERIC_COLUMNS:
            if col not in df.columns:
                df[col] = 0
        for col in CATEGORICAL_COLUMNS:
            if col not in df.columns:
                df[col] = "unknown"

        # Keep only required columns
        df = df[[c for c in REQUIRED_COLUMNS if c in df.columns]]

        # Fix types
        for col in NUMERIC_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        for col in CATEGORICAL_COLUMNS:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna("unknown")

        # Build payload with source = "scheduled"
        payload = {
            "features": df.to_dict(orient="records"),
            "source": "scheduled",
        }

        try:
            response = requests.post(API_URL, json=payload, timeout=30)

            if response.status_code == 200:
                result = response.json()
                total_predictions += result.get("count", 0)
                print(f"Predictions done for {file}: {result.get('count', 0)} rows")
            else:
                print(f"API error for {file}: {response.status_code} {response.text}")

        except Exception as e:
            print(f"Error calling API for {file}: {e}")

    # Update tracker with current timestamp
    with open(TRACKER_FILE, "w") as f:
        f.write(str(time.time()))

    print(f"Total predictions made: {total_predictions}")


# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="prediction_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["prediction", "ml"],
) as dag:

    t1 = ShortCircuitOperator(
        task_id="check_for_new_data",
        python_callable=check_for_new_data,
    )

    t2 = PythonOperator(
        task_id="make_predictions",
        python_callable=make_predictions,
    )

    t1 >> t2
