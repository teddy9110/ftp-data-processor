from typing import Dict

import polars as pl
from polars import DataFrame
from prefect import task


@task
def split_frame_by_operator(df: pl.DataFrame) -> Dict[str, DataFrame]:
    """
    Split DataFrame by operator for parallel processing.
    Now splits by a combination of home_pmn_code and visitor_pmn_code
    to ensure unique operator pairs.
    """
    print("\n--- Splitting DataFrame by 'home_pmn_code' and 'visitor_pmn_code'...")

    if len(df) == 0:
        print("⚠️ No data to split")
        return {}

    # Create a combined key for grouping
    df_with_combined_key = df.with_columns(
        (
            pl.col("home_pmn_code").cast(pl.Utf8)
            + "_"
            + pl.col("visitor_pmn_code").cast(pl.Utf8)
        ).alias("operator_pair_key")
    )

    operator_dataframes = {
        str(key): data
        for key, data in df_with_combined_key.group_by("operator_pair_key")
    }

    # Remove the temporary combined key column from the sub-DataFrames
    for key in operator_dataframes:
        operator_dataframes[key] = operator_dataframes[key].drop("operator_pair_key")

    print(
        f"✅ DataFrame successfully split into {len(operator_dataframes)} sub-frames based on PMN pairs."
    )
    return operator_dataframes
