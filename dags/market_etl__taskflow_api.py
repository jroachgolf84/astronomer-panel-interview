"""
market_etl__taskflow_api.py

Description:
    A DAG that extracts data from the Polygon API, flattens it, transforms it, and loads the cleaned data to a Postgres
    database. Rather than using Traditional operators, the TaskFlow API is used for this implementation.

TODO:
    - Decorate functions upon import?
    - Add typing
    - Add comments
    - Add one task in the code, import the others
    - Think about a branch operator... start -> check if market is open -> end or extract

Author: Jake Roach
Date: 2023-11-29
"""

# Import modules needed to build the DAG here
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from typing import List, Dict, Any
from airflow.models import Variable, Connection
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.engine.base import Engine


@dag(
    # Using the same parameters as the DAG using traditional operators
    start_date=datetime(2023, 8, 1),
    end_date=datetime(2023, 8, 31),
    schedule_interval="0 9 * * 1-5",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1)
    }

)
def market_etl__taskflow_api():
    @task()
    def extract_market_data(**context):
        # Instantiate a list of tickers that will be pulled and looped over
        stock_tickers: List[str] = [
            "AAPL",
            "AMZN"
        ]

        # Set variables
        POLYGON_API_KEY: str = Variable.get("POLYGON_API_KEY")
        ds: str = context.get("ds")

        # Create a list to append the response data to
        raw_dataset: List[Dict[str, Any]] = []

        # Loop through each ticker
        for stock_ticker in stock_tickers:
            # Build the URL, and make a request to the API
            url: str = f"https://api.polygon.io/v1/open-close/{stock_ticker}/{ds}?adjusted=true&apiKey={POLYGON_API_KEY}"
            response: requests.Response = requests.get(url)
            raw_dataset.append(response.json())

        # Return the raw data
        return raw_dataset

    @task()
    def flatten_market_data(raw_dataset):
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
    def transform_market_data(flattened_dataset):
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
    def load_market_data(transformed_dataframe, **context):
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

    # Set dependencies between tasks using function calls
    raw_data = extract_market_data()
    flattened_data = flatten_market_data(raw_data)
    transformed_data = transform_market_data(flattened_data)
    load_market_data(transformed_data)


market_etl__taskflow_api()

# Last line of file
