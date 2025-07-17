import time
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

import polars as pl
from polars import DataFrame
from prefect import task

from app.db.connection import get_session
from app.services.deal_data_service_mapper import DealDataServiceMapper
from app.sqlalchemy_schemas import MonthlyTable
from app.tasks.get_deal_data import get_deal_data
from app.tasks.upsert_operator_monthly_subframe import upsert_operator_monthly_subframe

"""
This module contains a Prefect task for processing operator-specific dataframes.

It retrieves deal data, maps it to a B.I.F (Bolt Interchange Format) format,
and then upserts the processed data into the monthly DB table.
"""
@task
def process_operator_subframe(
    home_pmn_code_from_grouped_data: str,  # This is the 'home_pmn_code' from the grouped subframe
    visitor_pmn_code_from_grouped_data: str,  # This is the 'visitor_pmn_code' from the grouped subframe
    operator_df: pl.DataFrame,
    file_type_from_flow: Optional[
        str
    ] = None,
    file_hash: Optional[str] = None,
    file_uuid: Optional[UUID] = None,
) -> DataFrame:
    """
    Process individual operator data and make an API call, using the home and visited
    PMN codes derived from the grouped data.
    """

    # Determine a representative date for the operator's data
    operator_dates = (
        operator_df.select(pl.col("date").unique()).get_column("date").to_list()
    )
    representative_date = str(operator_dates[-1]) if operator_dates else "unknown_date"

    # Make the API call with the home and visited PMN codes from the grouped data
    contract_uuid, deal_data = get_deal_data(
        home_pmn_api=home_pmn_code_from_grouped_data,
        visited_pmn_api=visitor_pmn_code_from_grouped_data,
        date=representative_date,
        file_type=file_type_from_flow,  # Pass file_type for contextual logging in get_deal_data
    )

    # Initialise the deal data service mapper
    if deal_data is None:
        print(
            f"Deal data could not be retrieved for PMN Pair: Home '{home_pmn_code_from_grouped_data}', Visited '{visitor_pmn_code_from_grouped_data}'."
        )
        return None

    mapper = DealDataServiceMapper(deal_data= deal_data)

    bif_format_frame = mapper.map_all_bifs(operator_df)
    with pl.Config(tbl_cols=-1):
        print(f"B.I.F for {home_pmn_code_from_grouped_data, visitor_pmn_code_from_grouped_data}: {bif_format_frame}")
    upsert_operator_monthly_subframe(bif_format_frame, file_uuid, contract_uuid)

    return bif_format_frame
