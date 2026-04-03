"""Tests for stats computation from validation results."""


def compute_stats(total_rows, failed_rows):
    """Mirrors the stats computation from ingestion_pipeline.py."""
    success_rate = ((total_rows - failed_rows) / total_rows) if total_rows > 0 else 0
    return round(success_rate, 4)


def categorize_errors(error_details):
    """Mirrors the error categorization from ingestion_pipeline.py."""
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

    return {
        "null_errors": null_errors,
        "range_errors": range_errors,
        "type_errors": type_errors,
        "categorical_errors": categorical_errors,
        "schema_errors": schema_errors,
    }


def test_success_rate_all_pass():
    assert compute_stats(100, 0) == 1.0


def test_success_rate_all_fail():
    assert compute_stats(100, 100) == 0.0


def test_success_rate_partial():
    assert compute_stats(100, 25) == 0.75


def test_success_rate_zero_rows():
    assert compute_stats(0, 0) == 0


def test_categorize_null_errors():
    details = {"expect_column_values_to_not_be_null": 15}
    result = categorize_errors(details)
    assert result["null_errors"] == 15
    assert result["range_errors"] == 0


def test_categorize_range_errors():
    details = {"expect_column_values_to_be_between": 8}
    result = categorize_errors(details)
    assert result["range_errors"] == 8


def test_categorize_categorical_errors():
    details = {"expect_column_values_to_be_in_set": 12}
    result = categorize_errors(details)
    assert result["categorical_errors"] == 12


def test_categorize_schema_errors():
    details = {"expect_column_to_exist": 1}
    result = categorize_errors(details)
    assert result["schema_errors"] == 1


def test_categorize_mixed_errors():
    details = {
        "expect_column_values_to_not_be_null": 10,
        "expect_column_values_to_be_between": 5,
        "expect_column_values_to_be_in_set": 3,
        "expect_column_to_exist": 1,
    }
    result = categorize_errors(details)
    assert result["null_errors"] == 10
    assert result["range_errors"] == 5
    assert result["categorical_errors"] == 3
    assert result["schema_errors"] == 1
    assert result["type_errors"] == 0
