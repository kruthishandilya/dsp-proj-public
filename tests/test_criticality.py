"""Tests for criticality level calculation logic."""


def compute_criticality(failure_ratio, schema_error=False):
    """Mirrors the criticality logic from gx_validation.py."""
    if schema_error or failure_ratio > 0.5:
        return "HIGH"
    elif failure_ratio > 0.1:
        return "MEDIUM"
    elif failure_ratio > 0:
        return "LOW"
    else:
        return "NONE"


def test_high_criticality_over_50_percent():
    assert compute_criticality(0.6) == "HIGH"
    assert compute_criticality(0.51) == "HIGH"
    assert compute_criticality(1.0) == "HIGH"


def test_high_criticality_schema_error():
    assert compute_criticality(0.0, schema_error=True) == "HIGH"
    assert compute_criticality(0.05, schema_error=True) == "HIGH"


def test_medium_criticality():
    assert compute_criticality(0.15) == "MEDIUM"
    assert compute_criticality(0.3) == "MEDIUM"
    assert compute_criticality(0.5) == "MEDIUM"


def test_low_criticality():
    assert compute_criticality(0.01) == "LOW"
    assert compute_criticality(0.05) == "LOW"
    assert compute_criticality(0.1) == "LOW"


def test_no_criticality():
    assert compute_criticality(0.0) == "NONE"


def test_boundary_values():
    assert compute_criticality(0.0) == "NONE"
    # >0 and <=0.1 -> LOW
    assert compute_criticality(0.001) == "LOW"
    # >0.1 and <=0.5 -> MEDIUM
    assert compute_criticality(0.101) == "MEDIUM"
    # >0.5 -> HIGH
    assert compute_criticality(0.501) == "HIGH"
