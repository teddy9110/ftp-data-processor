from typing import BinaryIO, Union

import polars as pl
from prefect import task

from app.utils.utils import clean_col_name


@task
def load_and_clean_data(
        file_source: Union[str, BinaryIO], filename: str = "stream", skip_rows: int = 0
) -> pl.DataFrame:
    print(f"Reading and cleaning data from: {filename}...")

    df = pl.read_csv(
        file_source,
        skip_rows=skip_rows,
        try_parse_dates=True,
        infer_schema_length=5000,
    )

    df.columns = [clean_col_name(col) for col in df.columns]
    print("✅ Data loaded and cleaned.")
    return df