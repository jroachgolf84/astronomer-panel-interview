"""
daily_operations_view_update.py

Description:
    File that is used to instantiate the daily_operations_view_update DAG. This DAG runs four SQL store-procedures
    concurrently, similar to a task a Data Engineer, Analyst, or Scientist may have to run daily at the beginning of
    each work day if there weren't to have a tool like Airflow.

Author: Jake Roach
Date: 2023-11-30
"""

# Import modules here
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# Instantiate the DAG
with DAG(
    dag_id="daily_operations_view_update",
    start_date=datetime(2023, 4, 1),
    end_date=datetime(2023, 6, 30),
    schedule="0 12 * * *",
    catchup=True,
    max_active_runs=1,
    template_searchpath="include/sql",  # This path can be searched for templated SQL queries (in .sql files)
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

    update_guest_attendance_view: SQLExecuteQueryOperator = SQLExecuteQueryOperator(
        dag=dag,
        task_id="update_guest_attendance_view",
        conn_id="postgres_daily_operational_conn",
        # Query for verbosity, there is also a file for this
        sql="""
        CREATE OR REPLACE VIEW daily_operations.admission_by_entrance AS (
            SELECT
                admission_date,
                entrance_name,
                SUM(quantity_sold) AS total_quantity_sold,
                SUM(ticket_price * quantity_sold) AS total_gross_revenue
            FROM daily_operations.guest_attendance AS guest_attendance
            WHERE guest_attendance.admission_date = '{{ ds }}'
            GROUP BY
                admission_date,
                entrance_name
        );
        """
    )

    update_inventory_view: SQLExecuteQueryOperator = SQLExecuteQueryOperator(
        dag=dag,
        task_id="update_inventory_view",
        conn_id="postgres_daily_operational_conn",
        sql="update_inventory_view.sql"
    )

    update_item_sales_view: SQLExecuteQueryOperator = SQLExecuteQueryOperator(
        dag=dag,
        task_id="update_item_sales_view",
        conn_id="postgres_daily_operational_conn",
        sql="update_item_sales_view.sql"
    )

    update_labor_view: SQLExecuteQueryOperator = SQLExecuteQueryOperator(
        dag=dag,
        task_id="update_labor_view",
        conn_id="postgres_daily_operational_conn",
        sql="update_labor_view.sql"
    )

    # Create a transient end task
    end: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="end"
    )

    # Set task dependencies
    start >> [update_guest_attendance_view, update_inventory_view, update_item_sales_view, update_labor_view] >> end


# Last line of file
