"""
market_etl__taskflow_api__helpers.py

Description:
    File that contains the helpers for the DAG instantiated with the TaskFlow API in the market_etl__traditional.py
    file.

Author: Jake Roach
Date: 2023-11-29
"""

# Import modules here
from airflow.decorators import task
from typing import List, Dict, Any
import pandas as pd

from include.market_etl__traditional__helpers import transform_market_data__callable, load_market_data__callable


@task()
def flatten_market_data(raw_dataset: List[Dict[str, Any]]) -> List[Any]:
    """
    Description:
        Function that flattens the raw dataset returned by the extract_market_data opeartor. Also acts as a data
        quality check to make sure that the status is acceptable.

        NOT calling the existing callable to show that TaskFlow API tasks can be defined in a difference director,
        before being imported into the function where the DAG was defined.

    Params:
        raw_data (List[Dict[str, Any]])

    Returns:
        flattened_dataset (List[Any])
    """
    # Create a list of headers and a list to store the normalized data in
    raw_headers: List[str] = [
        "status",
        "from",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]
    flattened_dataset: List[Any] = [raw_headers]

    # Normalize the data into a list of lists
    for raw_record in raw_dataset:
        if raw_record.get("status") == "OK":
            # Create a list to append the data to
            flattened_record: List[Any] = []

            for header in raw_headers:
                # Append the data
                flattened_record.append(raw_record.get(header))

            # Append the normalized record to the normalized dataset
            flattened_dataset.append(flattened_record)
        else:
            raise Exception(f"status was of state: {raw_record.get('status')}, DAG run failed")

    return flattened_dataset


@task()
def transform_market_data(flattened_dataset: List[Any]) -> pd.DataFrame:
    """
    Description:
        Function that transforms the flattened market dataset into a schema that matches a Postgres table that data
        will be loaded to in the following task.

        This functions calls the existing callable in the include/market_etl__traditional__helpers.py file.

    Params:
        flattened_dataset (List[Any])

    Returns:
        transformed_dataframe (pd.DataFrame)
    """
    # Call the existing callable
    return transform_market_data__callable(flattened_dataset)


@task()
def load_market_data(transformed_dataframe: pd.DataFrame, **context: Dict[str, Any]) -> None:
    """
    Description:
        Overwrite the existing table with the new results, using the .to_sql() pandas method.

        This functions calls the existing callable in the include/market_etl__traditional__helpers.py file.

    Params:
        transformed_dataframe (pd.DataFrame)
        **context (Dict[str, Any])

    Return:
        None
    """
    # Call the existing callable
    return load_market_data__callable(transformed_dataframe, **context)

# Last line of file
