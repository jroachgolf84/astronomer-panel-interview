"""
daily_operations_view_update.py

Description:
    File that is used to instantiate the daily_operations_view_update DAG. This DAG runs four SQL store-procedures
    concurrently, similar to a task a Data Engineer, Analyst, or Scientist may have to run daily at the beginning of
    each work day if there weren't to have a tool like Airflow.

TODO:
    - Update the existing SQL query
    - Add a .sql file in a directory

Author: Jake Roach
Date: 2023-11-30
"""

# Import modules here
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Instantiate the DAG
with DAG(
    dag_id="daily_operations_view_update",
    start_date=datetime(2023, 4, 1),
    end_date=datetime(2023, 6, 30),
    schedule_interval="0 12 * * *",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1)
    }
) as dag:

    # Create a transient start
    start: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="start"
    )

    update_guest_attendance_view: EmptyOperator = PostgresOperator(
        dag=dag,
        task_id="update_guest_attendance_view",
        postgres_conn_id="postgres_daily_operational_conn",
        sql="""
        CREATE OR REPLACE VIEW summarized_guest_attendance AS (
            SELECT
                *
            FROM daily_operations.guest_attendance AS guest_attendance
            WHERE guest_attendance.admission_date = '{{ ds }}'
        );
        """
    )

    update_inventory_view: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="update_inventory_view"
    )

    update_item_sales_view: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="update_item_sales_view"
    )

    update_labor_view: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="update_labor_view"
    )

    # Create a transient end task
    end: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="end"
    )

    # Set task dependencies
    start >> [update_guest_attendance_view, update_inventory_view, update_item_sales_view, update_labor_view] >> end


# Last line of file
