import argparse
from pathlib import Path
import pandas as pd
import numpy as np


def split_dataset(input_path, output_folder, n_files):
    if n_files <= 0:
        raise ValueError("n_files must be > 0")

    df = pd.read_csv(input_path)

    if df.empty:
        raise ValueError("Dataset is empty")

    # Shuffle data
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)

    # Remove old files
    for file in output_path.glob("*.csv"):
        file.unlink()

    # Split dataset
    chunks = np.array_split(df, n_files)

    # Save files
    for i, chunk in enumerate(chunks):
        pd.DataFrame(chunk).to_csv(output_path / f"data_part_{i}.csv", index=False)

    print(f"YES, Created {n_files} files in {output_folder}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split dataset into N files")

    parser.add_argument("--input_path", required=True, help="Path to input dataset")
    parser.add_argument("--output_folder", required=True, help="Path to raw_data folder")
    parser.add_argument("--n_files", type=int, required=True, help="Number of files to create")

    args = parser.parse_args()

    split_dataset(args.input_path, args.output_folder, args.n_files)