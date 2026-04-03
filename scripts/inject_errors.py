import pandas as pd
import numpy as np
import argparse
import random
import os


DEFAULT_PROBS = {
    "null": 0.1,
    "range": 0.1,
    "categorical": 0.1,
    "type": 0.1,
    "duplicate": 0.1,
    "outlier": 0.1,
    "schema": 0.1,
}


def inject_errors(input_path, output_path, probs=None):
    """Inject errors with configurable probability per error type."""
    if probs is None:
        probs = DEFAULT_PROBS

    df = pd.read_csv(input_path)
    df_corrupted = df.copy()
    n_rows = len(df)

    # 1. NULL VALUES (completeness)
    for col in df.columns:
        mask = np.random.rand(n_rows) < probs["null"]
        df_corrupted.loc[mask, col] = None

    # 2. OUT OF RANGE (validity)
    for col in df.select_dtypes(include=np.number).columns:
        mask = np.random.rand(n_rows) < probs["range"]
        df_corrupted.loc[mask, col] = -9999

    # 3. INVALID CATEGORICAL VALUES (consistency)
    for col in df.select_dtypes(include="object").columns:
        mask = np.random.rand(n_rows) < probs["categorical"]
        df_corrupted.loc[mask, col] = "INVALID_CATEGORY"

    # 4. WRONG DATA TYPE (type)
    for col in df.select_dtypes(include=np.number).columns:
        mask = np.random.rand(n_rows) < probs["type"]
        df_corrupted[col] = df_corrupted[col].astype("object")
        df_corrupted.loc[mask, col] = "wrong_type"

    # 5. DUPLICATE ROWS (additional)
    if n_rows > 0:
        duplicate_rows = df.sample(frac=probs["duplicate"])
        df_corrupted = pd.concat(
            [df_corrupted, duplicate_rows], ignore_index=True
        )

    # 6. STATISTICAL OUTLIERS (additional)
    for col in df.select_dtypes(include=np.number).columns:
        mask = np.random.rand(len(df_corrupted)) < probs["outlier"]
        df_corrupted.loc[mask, col] = df[col].mean() * 1000

    # 7. MISSING COLUMN / schema error (file-level)
    if random.random() < probs["schema"]:
        col_to_drop = random.choice(list(df.columns))
        df_corrupted = df_corrupted.drop(columns=[col_to_drop])
        print(f"Dropped column: {col_to_drop}")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_corrupted.to_csv(output_path, index=False)
    print(f"Corrupted dataset saved at {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Inject data errors with per-type probability"
    )
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--error_prob", type=float, default=0.1,
                        help="Default probability for all error types")
    parser.add_argument("--null_prob", type=float, default=None)
    parser.add_argument("--range_prob", type=float, default=None)
    parser.add_argument("--categorical_prob", type=float, default=None)
    parser.add_argument("--type_prob", type=float, default=None)
    parser.add_argument("--duplicate_prob", type=float, default=None)
    parser.add_argument("--outlier_prob", type=float, default=None)
    parser.add_argument("--schema_prob", type=float, default=None)

    args = parser.parse_args()

    # Build per-type probability dict
    probs = {
        "null": args.null_prob if args.null_prob is not None else args.error_prob,
        "range": args.range_prob if args.range_prob is not None else args.error_prob,
        "categorical": (args.categorical_prob if args.categorical_prob is not None
                        else args.error_prob),
        "type": args.type_prob if args.type_prob is not None else args.error_prob,
        "duplicate": (args.duplicate_prob if args.duplicate_prob is not None
                      else args.error_prob),
        "outlier": (args.outlier_prob if args.outlier_prob is not None
                    else args.error_prob),
        "schema": (args.schema_prob if args.schema_prob is not None
                   else args.error_prob),
    }

    inject_errors(args.input_path, args.output_path, probs)
