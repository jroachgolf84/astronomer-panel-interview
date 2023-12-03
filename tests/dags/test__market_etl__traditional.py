"""
test__market_etl__traditional.py

Description:
    File that contains unit tests for DAG defined in the market_etl__traditional.py file (in the dags/ directory).

Author: Jake Roach
Date: 2023-12-03
"""

# Import modules here
import pytest
from airflow.models import DagBag
from datetime import datetime, timezone


# Create any fixtures that are needed for this test
@pytest.fixture()
def dag__market_etl__traditional():
    """
    Description:
        Grab all DAGs in the dags/ directory (in the form of a DagBag).

    Returns:
        DagBag
    """
    return DagBag(include_examples=False).dags.get("market_etl__traditional")


# Define unit tests
def test_dag_start_end_dates(dag__market_etl__traditional):
    # Assign a timezone to this, since Airflow will interpret as UTC
    assert dag__market_etl__traditional.start_date == datetime(2023, 8, 1, tzinfo=timezone.utc)
    assert dag__market_etl__traditional.end_date == datetime(2023, 8, 31, tzinfo=timezone.utc)


def test_dag_schedule_parameters(dag__market_etl__traditional):
    # Cron job (note that this uses "schedule_interval", instead of schedule)
    assert dag__market_etl__traditional.schedule_interval == "0 9 * * 1-5"
    assert dag__market_etl__traditional.catchup

# Last line of file
