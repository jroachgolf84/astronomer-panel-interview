"""
market_etl__traditional.py

Description:
    A DAG that extracts data from the Polygon API, flattens it, transforms it, and loads the cleaned data to a Postgres
    database.

Author: Jake Roach
Date: 2023-11-29
"""

# Import modules needed to build the DAG here
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from include.market_etl__traditional__helpers import extract_market_data__callable
from include.market_etl__traditional__helpers import flatten_market_data__callable
from include.market_etl__traditional__helpers import transform_market_data__callable
from include.market_etl__traditional__helpers import load_market_data__callable


# Define the DAG to run for a month
with DAG(
    dag_id="market_etl__traditional",
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
) as dag:

    # Transient start operator
    start: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="start"
    )

    extract_market_data: PythonOperator = PythonOperator(
        dag=dag,
        task_id="extract_market_data",
        python_callable=extract_market_data__callable
    )

    flatten_market_data: PythonOperator = PythonOperator(
        dag=dag,
        task_id="flatten_market_data",
        python_callable=flatten_market_data__callable,
        op_kwargs={
            "raw_dataset": "{{ ti.xcom_pull(task_ids='extract_market_data') }}"
        }
    )

    transform_market_data: PythonOperator = PythonOperator(
        dag=dag,
        task_id="transform_market_data",
        python_callable=transform_market_data__callable,
        op_kwargs={
            "flattened_dataset": "{{ ti.xcom_pull(task_ids='flatten_market_data') }}"
        }
    )

    load_market_data: PythonOperator = PythonOperator(
        dag=dag,
        task_id="load_market_data",
        python_callable=load_market_data__callable,
        op_kwargs={
            "transformed_dataframe": "{{ ti.xcom_pull(task_ids='transform_market_data') }}"
        }
    )

    # Transient end operator
    end: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="end"
    )

    # Set dependencies
    start >> extract_market_data >> flatten_market_data >> transform_market_data >> load_market_data >> end

# Last line of file
