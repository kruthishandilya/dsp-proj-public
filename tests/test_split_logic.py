"""Tests for file splitting logic (good/bad row separation)."""

import pandas as pd


def split_data(df, failed_indices):
    """Mirrors the split logic from ingestion_pipeline.py."""
    total_rows = len(df)
    failed_rows = len(failed_indices)

    if failed_rows == 0:
        return df, pd.DataFrame()

    if failed_rows >= total_rows:
        return pd.DataFrame(), df

    valid_indices = [i for i in failed_indices if i < len(df)]
    bad_df = df.loc[df.index.isin(valid_indices)]
    good_df = df.loc[~df.index.isin(valid_indices)]
    return good_df, bad_df


def test_all_good():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    good, bad = split_data(df, failed_indices=[])
    assert len(good) == 3
    assert len(bad) == 0


def test_all_bad():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    good, bad = split_data(df, failed_indices=[0, 1, 2])
    assert len(good) == 0
    assert len(bad) == 3


def test_mixed_split():
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
    good, bad = split_data(df, failed_indices=[1, 3])
    assert len(good) == 3
    assert len(bad) == 2
    assert list(bad["a"]) == [2, 4]
    assert list(good["a"]) == [1, 3, 5]


def test_single_bad_row():
    df = pd.DataFrame({"a": [10, 20, 30]})
    good, bad = split_data(df, failed_indices=[0])
    assert len(good) == 2
    assert len(bad) == 1
    assert bad.iloc[0]["a"] == 10


def test_invalid_index_ignored():
    df = pd.DataFrame({"a": [1, 2, 3]})
    good, bad = split_data(df, failed_indices=[0, 99])
    assert len(good) == 2
    assert len(bad) == 1
