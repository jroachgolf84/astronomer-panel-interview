"""
market_etl__taskflow_api.py

Description:
    A DAG that extracts data from the Polygon API, flattens it, transforms it, and loads the cleaned data to a Postgres
    database. Rather than using Traditional operators, the TaskFlow API is used for this implementation.

Author: Jake Roach
Date: 2023-11-29
"""

# Import modules needed to build the DAG here
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from typing import List, Dict, Any
from airflow.models import Variable, Connection
import requests

from include.market_etl__taskflow_api__helpers import flatten_market_data
from include.market_etl__taskflow_api__helpers import transform_market_data
from include.market_etl__taskflow_api__helpers import load_market_data


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
    def extract_market_data(**context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Description:
            Pull market data from the Polygon API using the requests module

        Params:
            **context (Dict[str, Any])

        Returns:
            raw_dataset (List[Dict[str, Any]])
        """
        # Instantiate a list of tickers that will be pulled and looped over
        stock_tickers: List[str] = [
            "AAPL",
            "AMZN"
        ]

        # Set variables
        polygon_api_key: str = Variable.get("POLYGON_API_KEY")
        ds: str = context.get("ds")

        # Create a list to append the response data to
        raw_dataset: List[Dict[str, Any]] = []

        # Loop through each ticker
        for stock_ticker in stock_tickers:
            # Build the URL, and make a request to the API
            url: str = f"https://api.polygon.io/v1/open-close/{stock_ticker}/{ds}?adjusted=true&apiKey={polygon_api_key}"
            response: requests.Response = requests.get(url)
            raw_dataset.append(response.json())

        # Return the raw data
        return raw_dataset

    # Set dependencies between tasks using function calls
    raw_data = extract_market_data()
    flattened_data = flatten_market_data(raw_data)
    transformed_data = transform_market_data(flattened_data)
    load_market_data(transformed_data)


market_etl__taskflow_api()

# Last line of file
