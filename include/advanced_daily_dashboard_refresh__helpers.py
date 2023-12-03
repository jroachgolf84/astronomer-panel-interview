"""
advanced_daily_dashboard_refresh__helpers.py

Description:
    File containing helper functions and operators for the DAG defined in the advanced_market_etl.py file.

Author: Jake Roach
Date: 2023-12-02
"""

# Import modules here
from airflow.decorators import task
from typing import List, Dict, Any

from include.market_etl__traditional__helpers import extract_market_data__callable


# While duplicate code, define function here for clarity
@task()
def extract_market_data(**context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Description:
        Pull market data from the Polygon API using the requests module.

        Since this task was defined within a DAG-definition file, it was added to this directory. An existing callable
        was invoked, rather than copying and pasting logic.

    Params:
        **context (Dict[str, Any])

    Returns:
        raw_dataset (List[Dict[str, Any]])
    """

    # Call the existing callable
    return extract_market_data__callable(**context)


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
