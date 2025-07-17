from io import BytesIO
from typing import Any, Dict, List, Union

import polars as pl
from prefect import flow, task

from app.db.connection import get_session
from app.sqlalchemy_schemas import FileHashTable
from app.tasks.bolt_transformers.NGC_bolt_transform import transform_to_bolt_format
from app.tasks.load_and_clean_data import load_and_clean_data
from app.tasks.process_operator_subframe import process_operator_subframe
from app.tasks.split_frame_by_operator import split_frame_by_operator

"""
   Main Prefect flow that orchestrates the processing of a raw CSV file.

   This flow performs the following key steps:
   1. Loads and cleans the raw CSV data.
   2. Transforms the cleaned data into a standardized 'Bolt' format.
   3. Splits the transformed data by operator pairs (Home PMN - Visitor PMN).
   4. Retrieves the unique identifier (UUID) for the processed file using its hash.
   5. Iterates through each operator-specific sub-dataframe and processes it
      sequentially using the `process_operator_subframe` task.

   It differentiates PMN roles for API calls based on the 'file_type' (home/visiting),
   which is crucial for correct data handling and subsequent API interactions.
   """

@flow(log_prints=True)
def process_csv_flow(
    file_source: Union[str, bytes] = None,
    filename: str = "unknown",
    service_mappings: List[Dict[str, str]] = None,
    skip_rows: int = 0,
    vpmn: str = None,  # This is the PMN code from the FTP watcher (the file owner)
    file_type: str = "unknown",  # 'home' or 'visiting' as determined by FTP watcher
    file_hash: str = None,
):

    print(f"Processing file: {filename}")
    print(f"File Type received by flow: {file_type}")
    print(f"VPMN (file owner) received by flow: {vpmn}")

    if isinstance(file_source, bytes):
        file_source = BytesIO(file_source)

    cleaned_df = load_and_clean_data(
        file_source=file_source, filename=filename, skip_rows=skip_rows
    )
    long_df = transform_to_bolt_format(
        cleaned_df, service_mappings=service_mappings, pmn=vpmn, file_type=file_type
    )

    if long_df is None or long_df.is_empty():
        print("No transformed data to process. Exiting flow.")
        return {
            "transformed_df": pl.DataFrame(),
            "operator_results": [],
        }

    all_operator_dfs = split_frame_by_operator(df=long_df)

    if all_operator_dfs:
        # Get the first key, which is the combined home_pmn_code_visitor_pmn_code
        first_operator_pair_key = next(iter(all_operator_dfs))
        first_operator_df = all_operator_dfs[first_operator_pair_key]
        print(
            f"\n--- Debugging: First operator DataFrame (PMN Pair Key: {first_operator_pair_key}) ---"
        )
        with pl.Config(tbl_cols=-1):
            print(first_operator_df.head())  # Print the head of the first DataFrame
        print(f"Shape of first operator DataFrame: {first_operator_df.shape}")
        print(f"Columns of first operator DataFrame: {first_operator_df.columns}")
    else:
        print("\n--- Debugging: No operator DataFrames to display ---")

    processing_results = []

    if all_operator_dfs:
        print("\n--- Processing operator pairs sequentially ---")
        # get the file UUID
        file_uuid = get_file_uuid(file_hash)
        print(f"getting file uuid: {file_uuid} , from hash of file: {file_hash}")
        for key, operator_df in all_operator_dfs.items():
            home_pmn, visitor_pmn = key.split("_")
            # Submit the task and immediately wait for the result
            future = process_operator_subframe.submit(
                home_pmn_code_from_grouped_data=home_pmn,
                visitor_pmn_code_from_grouped_data=visitor_pmn,
                operator_df=operator_df,
                file_type_from_flow=file_type,
                file_hash=file_hash,
                file_uuid = file_uuid
            )
            future.wait()
            result = future.result()  # Waits for the task to finish before continuing
            processing_results.append(result)
    else:
        print("⚠️ No operator data to process after transformation and splitting.")

    results_dict = {
        "transformed_df": long_df,
        "operator_results": processing_results,
    }
    return results_dict

@task
def get_file_uuid(file_hash):
    with (get_session() as db):
        return db.query(FileHashTable.uuid).filter(FileHashTable.sha_256_hash == file_hash).scalar()