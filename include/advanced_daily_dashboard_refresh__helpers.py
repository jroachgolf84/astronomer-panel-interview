"""
advanced_daily_dashboard_refresh__helpers.py

Description:
    File containing helper functions and operators for the DAG defined in the advanced_market_etl.py file.

Author: Jake Roach
Date: 2023-12-02
"""

# Import modules here
from airflow.decorators import task
from airflow.models import Variable
from typing import List, Dict, Any
import requests


# While duplicate code, define function here for clarity
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


# Market holidays
market_holidays: List[str] = [
    "2023-04-07",
    "2023-05-29",
    "2023-06-19"
]

# Create the configuration for dynamically-generated TaskGroups
replication_task_config: List[Dict[str, str]] = [
    {
        "task_name": "guest_attendance",
        "sql": """
            SELECT
                *
            FROM daily_operations.guest_attendance AS guest_attendance
            WHERE guest_attendance.admission_date = '{{ ds }}'
        """,
        "s3_key": "{{ ds }}/guest_attendance.csv",
        "snowflake_table": "guest_attendance"
    },
    {
        "task_name": "inventory",
        "sql": """
            SELECT
                *
            FROM daily_operations.inventory AS inventory
            WHERE inventory.inventory_date = '{{ ds }}'
        """,
        "s3_key": "{{ ds }}/inventory.csv",
        "snowflake_table": "inventory"
    },
    {
        "task_name": "item_sales",
        "sql": """
            SELECT
                *
            FROM daily_operations.item_sales AS item_sales
            WHERE item_sales.sales_date = '{{ ds }}'
        """,
        "s3_key": "{{ ds }}/item_sales.csv",
        "snowflake_table": "item_sales"
    },
    {
        "task_name": "labor",
        "sql": """
            SELECT
                *
            FROM daily_operations.labor AS labor
            WHERE labor.labor_date <= '{{ ds }}'
        """,
        "s3_key": "{{ ds }}/labor.csv",
        "snowflake_table": "labor"
    }
]

# Last line of file
