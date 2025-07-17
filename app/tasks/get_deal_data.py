import uuid
from datetime import datetime
from os import environ
from typing import Optional, Tuple
from uuid import UUID

import requests
from docusign_esign.client import api_response
from prefect import task

from app.pydantic_models.deal_data import AA12_DealData


@task
def get_deal_data(
    home_pmn_api: str,  # The PMN representing the "home" side for the API call
    visited_pmn_api: str,  # The PMN representing the "visited" side for the API call
    date: str,
    file_type: str,  # The original file type (home/visiting)
) -> tuple[None, None] | tuple[UUID, AA12_DealData] | None:
    """
    Makes an API call with the home and visited PMN codes, and a representative date.
    Returns the API response (or an error message) and the input parameters.
    """
    api_url = f"{environ['BASE_URL']}/internal/contracts/query"
    try:
        parsed_date = datetime.strptime(date + "1", "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError as e:
        print(f"Error parsing date '{date}': {e}")
        return None

    payload = {"hpmn": home_pmn_api, "vpmn": visited_pmn_api, "query_date": parsed_date}

    try:
        response = requests.get(api_url, params=payload)
        response.raise_for_status()
        api_result = response.json()
        if not api_result:
            print(f"API call Didnt Find contract, printing payload: {payload}")
        print(
            f"API call successful Response UUID : {api_result[0][0]}"
        )
        print(
            f"API call successful Response: {api_result[0][1]}"
        )

        return uuid.UUID(api_result[0][0]), AA12_DealData.model_validate(api_result[0][1])
    except requests.exceptions.RequestException as e:
        print(
            f"Error making API call for Home PMN: {home_pmn_api}, Visited PMN: {visited_pmn_api}: {e}"
        )
        return None, None