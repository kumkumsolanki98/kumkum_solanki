from pathlib import Path
import pandas as pd

files = sorted(Path("orders_parquet").glob("*.parquet"))

if files:
    df = pd.concat(
        [pd.read_parquet(file) for file in files],
        ignore_index=True
    )

    print(df)
    print("Total rows:", len(df))
else:
    print("No Parquet files found.")
