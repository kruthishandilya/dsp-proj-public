"""
Great Expectations v1.x validation module.
Uses: Expectation Suite, Validation Definition, Checkpoint, Data Docs.
"""

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
import os
import shutil
import glob
from datetime import datetime


REPORTS_DIR = "/opt/airflow/data/gx_reports"
GX_PROJECT_DIR = "/opt/airflow/data/gx"


def build_expectation_suite(suite, df):
    """Add all validation expectations to the suite."""

    required_columns = [
        "amount", "account_age_days", "shipping_distance_km",
        "total_transactions_user", "avg_amount_user",
        "transaction_hour", "transaction_day",
        "promo_used", "avs_match", "three_ds_flag", "cvv_result",
        "country", "bin_country", "merchant_category", "channel",
    ]

    # --- Schema: required columns must exist ---
    for col in required_columns:
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))

    # --- Completeness: null checks on present columns ---
    for col in required_columns:
        if col in df.columns:
            suite.add_expectation(
                gxe.ExpectColumnValuesToNotBeNull(column=col)
            )

    # --- Validity: range checks ---
    range_checks = {
        "amount": (0, 100000),
        "account_age_days": (0, 36500),
        "transaction_hour": (0, 23),
        "transaction_day": (0, 6),
        "shipping_distance_km": (0, 50000),
    }
    for col, (min_val, max_val) in range_checks.items():
        if col in df.columns:
            suite.add_expectation(
                gxe.ExpectColumnValuesToBeBetween(
                    column=col, min_value=min_val, max_value=max_val
                )
            )

    # --- Consistency: categorical value checks ---
    if "channel" in df.columns:
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(
                column="channel",
                value_set=["web", "mobile", "in_store", "phone"]
            )
        )
    if "country" in df.columns:
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(
                column="country",
                value_set=[
                    "US", "UK", "CA", "FR", "DE", "JP", "AU", "IN", "BR",
                    "MX", "IT", "ES", "NL", "SE", "NO", "DK", "FI", "CH",
                    "AT", "BE", "PT", "IE", "NZ", "SG", "HK", "KR", "TW",
                    "CN", "RU", "ZA",
                ]
            )
        )

    return suite


def persist_data_docs(context, file_name):
    """
    Build Data Docs and copy the generated HTML report
    to a persistent directory for serving (nginx in Defense 2).
    Returns the saved report file name.
    """
    context.build_data_docs()

    report_name = (
        f"report_{file_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    )
    report_path = os.path.join(REPORTS_DIR, report_name)

    # Find generated Data Docs index.html from the context
    # GX v1 file context stores Data Docs under:
    #   <project_root>/uncommitted/data_docs/local_site/index.html
    docs_glob = os.path.join(
        GX_PROJECT_DIR, "uncommitted", "data_docs", "local_site", "**", "*.html"
    )
    html_files = glob.glob(docs_glob, recursive=True)

    if html_files:
        # Copy the most recently modified HTML as the report
        latest = max(html_files, key=os.path.getmtime)
        shutil.copy2(latest, report_path)
        print(f"Data Docs report saved: {report_path}")
    else:
        # Fallback: search in temp dirs for ephemeral contexts
        print("Warning: Could not locate Data Docs HTML files")

    return report_name


def validate_dataframe(df, file_name="unknown"):
    """
    Validate a dataframe using GX v1.
    Uses: File Data Context, Expectation Suite, Validation Definition,
          Checkpoint, Data Docs (persisted to gx_reports/).

    Returns a dict with validation stats and report name.
    """
    os.makedirs(REPORTS_DIR, exist_ok=True)
    os.makedirs(GX_PROJECT_DIR, exist_ok=True)

    # --- File-based context (Data Docs persist to disk) ---
    context = gx.get_context(mode="file", project_root_dir=GX_PROJECT_DIR)

    # --- Data Source + Batch Definition ---
    # Use unique names per run to avoid conflicts on repeated DAG runs
    run_ts = datetime.now().strftime("%Y%m%d%H%M%S")

    try:
        data_source = context.data_sources.add_pandas(f"pandas_{run_ts}")
    except Exception:
        data_source = context.data_sources.add_pandas(
            f"pandas_{run_ts}_{os.getpid()}"
        )

    data_asset = data_source.add_dataframe_asset(f"data_{run_ts}")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"batch_{run_ts}"
    )

    # --- Expectation Suite ---
    suite = gx.ExpectationSuite(name=f"ingestion_suite_{run_ts}")
    suite = build_expectation_suite(suite, df)
    suite = context.suites.add(suite)

    # --- Validation Definition ---
    validation_def = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name=f"validation_{run_ts}",
    )
    validation_def = context.validation_definitions.add(validation_def)

    # --- Checkpoint with Data Docs action ---
    checkpoint = gx.Checkpoint(
        name=f"checkpoint_{run_ts}",
        validation_definitions=[validation_def],
        actions=[
            gx.checkpoint.UpdateDataDocsAction(name="update_data_docs"),
        ],
    )
    checkpoint = context.checkpoints.add(checkpoint)

    # --- Run validation ---
    checkpoint_result = checkpoint.run(
        batch_parameters={"dataframe": df}
    )

    # --- Extract results ---
    total_rows = len(df)
    failed_indices = set()
    error_details = {}
    schema_error = False

    for run_result in checkpoint_result.run_results.values():
        for exp_result in run_result.results:
            if not exp_result.success:
                exp_type = exp_result.expectation_config.type
                unexpected_count = exp_result.result.get(
                    "unexpected_count", 0
                )

                error_details[exp_type] = (
                    error_details.get(exp_type, 0) + unexpected_count
                )

                # Collect row-level failure indices
                idx_list = exp_result.result.get(
                    "unexpected_index_list", []
                )
                if idx_list:
                    failed_indices.update(idx_list)

                if "column_to_exist" in exp_type.lower():
                    schema_error = True

    failed_rows = (
        len(failed_indices) if failed_indices
        else min(sum(error_details.values()), total_rows)
    )
    failure_ratio = failed_rows / total_rows if total_rows > 0 else 0

    # --- Criticality ---
    if schema_error or failure_ratio > 0.5:
        criticality = "HIGH"
    elif failure_ratio > 0.1:
        criticality = "MEDIUM"
    elif failure_ratio > 0:
        criticality = "LOW"
    else:
        criticality = "NONE"

    # --- Persist Data Docs report to gx_reports/ ---
    report_name = persist_data_docs(context, file_name)

    return {
        "total_rows": total_rows,
        "failed_rows": failed_rows,
        "failure_ratio": round(failure_ratio, 4),
        "criticality": criticality,
        "error_details": error_details,
        "failed_indices": list(failed_indices),
        "report_name": report_name,
    }
