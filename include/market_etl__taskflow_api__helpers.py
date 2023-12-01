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
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.engine.base import Engine


@task()
def flatten_market_data(raw_dataset: List[Dict[str, Any]]) -> List[Any]:
    """
    Description:
        Function that flattens the raw dataset returned by the extract_market_data opeartor. Also acts as a data
        quality check to make sure that the status is acceptable.

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

    Params:
        flattened_dataset (List[Any])

    Returns:
        transformed_dataframe (pd.DataFrame)
    """
    # Convert to a pandas DataFrame
    raw_headers: List[str] = flattened_dataset.pop(0)
    flattened_dataframe: pd.DataFrame = pd.DataFrame(flattened_dataset, columns=raw_headers)

    # Perform feature engineering, reduce number of fields
    flattened_dataframe["change"] = flattened_dataframe["close"] - flattened_dataframe["open"]
    transformed_dataframe: pd.DataFrame = flattened_dataframe.loc[:, [
             "from",
             "symbol",
             "open",
             "high",
             "low",
             "close",
             "change",
             "volume",
        ]
    ]
    transformed_dataframe.columns = [
        "market_date",
        "ticker",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "change",
        "volume",
    ]

    # Return the dataset
    return transformed_dataframe


@task()
def load_market_data(transformed_dataframe: pd.DataFrame, **context: Dict[str, Any]) -> None:
    """
    Description:
        Overwrite the existing table with the new results, using the .to_sql() pandas method

    Params:
        transformed_dataframe (pd.DataFrame)
        **context (Dict[str, Any])

    Return:
        None
    """
    # Pull the connection
    market_database_hook: PostgresHook = PostgresHook("postgres_market_conn")
    market_database_conn: Engine = market_database_hook.get_sqlalchemy_engine()

    # Delete existing rows
    query: str = f"DELETE FROM market.transformed_market_data WHERE market_date = '{context.get('ds')}'"
    market_database_hook.run(query)

    # Load the table to Postgres, replace if it exists
    transformed_dataframe.to_sql(
        name="transformed_market_data",
        con=market_database_conn,
        schema="market",
        if_exists="append",
        index=False
    )

# Last line of file
