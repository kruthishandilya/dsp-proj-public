from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import json
import pandas as pd
import numpy as np
import random
import requests
import sys

sys.path.append("/opt/airflow/includes")

# =========================
# PATHS
# =========================
RAW_PATH = "/opt/airflow/data/raw_data"
GOOD_PATH = "/opt/airflow/data/good_data"
BAD_PATH = "/opt/airflow/data/bad_data"

os.makedirs(RAW_PATH, exist_ok=True)
os.makedirs(GOOD_PATH, exist_ok=True)
os.makedirs(BAD_PATH, exist_ok=True)

TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL", "")


# =========================
# TASK 1: READ DATA
# =========================
def read_data(**kwargs):
    files = [f for f in os.listdir(RAW_PATH) if f.endswith(".csv")]

    if not files:
        raise AirflowSkipException("No files in raw_data")

    file = random.choice(files)
    file_path = os.path.join(RAW_PATH, file)

    df = pd.read_csv(file_path)

    # Delete after reading to avoid re-processing
    os.remove(file_path)

    kwargs["ti"].xcom_push(key="data", value=df.to_json(orient="records"))
    kwargs["ti"].xcom_push(key="file_name", value=file)

    print(f"Read file: {file} ({len(df)} rows)")


# =========================
# TASK 2: VALIDATE DATA (GX v1)
# =========================
def validate_data(**kwargs):
    ti = kwargs["ti"]
    import sys
    sys.path.insert(0, "/opt/airflow/includes")
    from gx_validation import validate_dataframe

    data_json = ti.xcom_pull(task_ids="read_data", key="data")
    file_name = ti.xcom_pull(task_ids="read_data", key="file_name")

    if not data_json:
        raise ValueError("No data received from read_data")

    df = pd.read_json(data_json, orient="records")

    result = validate_dataframe(df, file_name=file_name)

    # Push results via XCom
    ti.xcom_push(key="validation_result", value=result)
    ti.xcom_push(key="data", value=df.to_json(orient="records"))

    print(f"Validation complete: {result['criticality']} "
          f"({result['failed_rows']}/{result['total_rows']} failed)")


# =========================
# TASK 3: SAVE STATS TO DB
# =========================
def save_statistics(**kwargs):
    ti = kwargs["ti"]

    result = ti.xcom_pull(task_ids="validate_data", key="validation_result")
    file_name = ti.xcom_pull(task_ids="read_data", key="file_name")

    total_rows = result.get("total_rows", 0)
    failed_rows = result.get("failed_rows", 0)
    success_rate = ((total_rows - failed_rows) / total_rows) if total_rows > 0 else 0
    criticality = result.get("criticality", "NONE")
    error_details = result.get("error_details", {})

    # Count errors by category from GX expectation types
    null_errors = 0
    range_errors = 0
    type_errors = 0
    categorical_errors = 0
    schema_errors = 0

    for exp_type, count in error_details.items():
        exp_lower = exp_type.lower()
        if "not_be_null" in exp_lower:
            null_errors += count
        elif "between" in exp_lower:
            range_errors += count
        elif "be_of_type" in exp_lower or "type" in exp_lower:
            type_errors += count
        elif "be_in_set" in exp_lower:
            categorical_errors += count
        elif "to_exist" in exp_lower:
            schema_errors += count

    hook = PostgresHook(postgres_conn_id="postgres_default")

    query = """
    INSERT INTO ingestion_stats
        (file_name, total_rows, failed_rows, success_rate, criticality,
         null_errors, range_errors, type_errors, categorical_errors,
         schema_errors, error_details, ingested_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    hook.run(query, parameters=(
        file_name, total_rows, failed_rows, round(success_rate, 4),
        criticality, null_errors, range_errors, type_errors,
        categorical_errors, schema_errors,
        json.dumps(error_details), datetime.utcnow(),
    ))

    print(f"Stats saved to DB for {file_name}")


# =========================
# TASK 4: SEND ALERTS
# =========================
def send_alerts(**kwargs):
    ti = kwargs["ti"]

    result = ti.xcom_pull(task_ids="validate_data", key="validation_result")
    file_name = ti.xcom_pull(task_ids="read_data", key="file_name")

    total = result.get("total_rows", 0)
    failed = result.get("failed_rows", 0)
    criticality = result.get("criticality", "NONE")
    report_name = result.get("report_name", "N/A")
    error_details = result.get("error_details", {})

    # No issues -> no alert
    if criticality == "NONE":
        print("No issues detected - no alert sent")
        return

    # Only send Teams notifications for MEDIUM or HIGH
    summary = (
        f"File: {file_name} | "
        f"Failed: {failed}/{total} rows | "
        f"Errors: {json.dumps(error_details)}"
    )

    print(f"ALERT [{criticality}] {summary} | Report: {report_name}")

    if criticality in ("MEDIUM", "HIGH") and TEAMS_WEBHOOK_URL:
        message = {
            "@type": "MessageCard",
            "summary": f"Data Quality Alert - {criticality}",
            "themeColor": "FF0000" if criticality == "HIGH" else "FFA500",
            "sections": [{
                "activityTitle": f"Data Quality Alert [{criticality}]",
                "facts": [
                    {"name": "File", "value": file_name},
                    {"name": "Criticality", "value": criticality},
                    {"name": "Failed Rows", "value": f"{failed}/{total}"},
                    {"name": "Error Summary", "value": json.dumps(error_details)},
                    {"name": "Report", "value": report_name},
                ],
            }],
        }
        try:
            resp = requests.post(TEAMS_WEBHOOK_URL, json=message, timeout=10)
            print(f"Teams notification sent (status {resp.status_code})")
        except Exception as e:
            print(f"Failed to send Teams notification: {e}")
    elif criticality == "LOW":
        print("LOW criticality - Teams notification skipped (alert fatigue)")


# =========================
# TASK 5: SPLIT AND SAVE DATA
# =========================
def split_and_save_data(**kwargs):
    ti = kwargs["ti"]

    data_json = ti.xcom_pull(task_ids="validate_data", key="data")
    result = ti.xcom_pull(task_ids="validate_data", key="validation_result")
    file_name = ti.xcom_pull(task_ids="read_data", key="file_name")

    if not data_json or not result:
        print("Missing data or validation result")
        return

    df = pd.read_json(data_json, orient="records")

    failed_rows = result.get("failed_rows", 0)
    total_rows = result.get("total_rows", len(df))
    failed_indices = result.get("failed_indices", [])

    # CASE 1: No issues -> all data goes to good_data
    if failed_rows == 0:
        df.to_csv(os.path.join(GOOD_PATH, f"good_{file_name}"), index=False)
        print(f"{file_name} -> ALL GOOD ({total_rows} rows)")
        return

    # CASE 2: All rows bad -> all data goes to bad_data
    if failed_rows >= total_rows:
        df.to_csv(os.path.join(BAD_PATH, f"bad_{file_name}"), index=False)
        print(f"{file_name} -> ALL BAD ({total_rows} rows)")
        return

    # CASE 3: Mixed -> split using actual failed indices from GX
    if failed_indices:
        valid_indices = [i for i in failed_indices if i < len(df)]
        bad_df = df.loc[df.index.isin(valid_indices)]
        good_df = df.loc[~df.index.isin(valid_indices)]
    else:
        # Fallback: split proportionally if indices not available
        bad_count = int(len(df) * (failed_rows / total_rows))
        bad_df = df.iloc[:bad_count]
        good_df = df.iloc[bad_count:]

    if not good_df.empty:
        good_df.to_csv(
            os.path.join(GOOD_PATH, f"good_{file_name}"), index=False
        )
    if not bad_df.empty:
        bad_df.to_csv(
            os.path.join(BAD_PATH, f"bad_{file_name}"), index=False
        )

    print(f"{file_name} -> SPLIT ({len(good_df)} good, {len(bad_df)} bad)")


# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="data_ingestion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
    tags=["ingestion"],
) as dag:

    t1 = PythonOperator(task_id="read_data", python_callable=read_data)
    t2 = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t3 = PythonOperator(task_id="save_statistics", python_callable=save_statistics)
    t4 = PythonOperator(task_id="send_alerts", python_callable=send_alerts)
    t5 = PythonOperator(
        task_id="split_and_save_data", python_callable=split_and_save_data
    )

    # save_statistics, send_alerts, split_and_save_data run in parallel after validate
    t1 >> t2 >> [t3, t4, t5]
