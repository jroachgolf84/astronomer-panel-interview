"""
test__market_etl__traditional.py

Description:
    File that contains unit tests for DAG defined in the market_etl__traditional.py file (in the dags/ directory). With
    these unit tests, the `assert` statements are quite simple. Mostly just checking parameters passed upon definition,
    as well as some basic task-level information.

Author: Jake Roach
Date: 2023-12-03
"""

# Import modules here
import pytest
from airflow.models import DagBag
from datetime import datetime, timezone, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Create any fixtures that are needed for this test
@pytest.fixture()
def market_etl__traditional():
    """
    Description:
        Grab all DAGs in the dags/ directory (in the form of a DagBag).

    Returns:
        DAG
    """
    return DagBag(include_examples=False).dags.get("market_etl__traditional")


# Define unit tests
def test_dag_start_end_dates(market_etl__traditional):
    # Assign a timezone to this, since Airflow will interpret as UTC
    assert market_etl__traditional.start_date == datetime(2023, 8, 1, tzinfo=timezone.utc)
    assert market_etl__traditional.end_date == datetime(2023, 8, 31, tzinfo=timezone.utc)


def test_dag_schedule_parameters(market_etl__traditional):
    # Cron job (note that this uses "schedule_interval", instead of schedule)
    assert market_etl__traditional.schedule_interval == "0 9 * * 1-5"
    assert market_etl__traditional.catchup


def test_dag_other_parameters(market_etl__traditional):
    # Test the max_active_runs, render, default arguments
    assert market_etl__traditional.max_active_runs == 1
    assert market_etl__traditional.render_template_as_native_obj == True
    assert market_etl__traditional.default_args == {
        "retries": 3,
        "retry_delay": timedelta(minutes=1)
    }


# Test the task composition of the DAG
def test_tasks(market_etl__traditional):
    # Check the total number of tasks
    assert len(market_etl__traditional.tasks) == 6

    for task in market_etl__traditional.tasks:
        assert isinstance(task, EmptyOperator) or isinstance(task, PythonOperator)
        assert task.task_id in (
            "start",
            "extract_market_data",
            "flatten_market_data",
            "transform_market_data",
            "load_market_data",
            "end"
        )

# Last line of file
