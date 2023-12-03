"""
advanced_daily_dashboard_refresh.py

Description:
    A DAG that extracts, transforms, and loads data from the Polygon API as part of a larger data pipeline, with a
    number of complex dependencies. It also pulls data from Postgres into S3 and Snowflake, later archiving that data.

Author: Jake Roach
Date: 2023-12-02
"""

# Import modules here
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from typing import Dict, Any, List

from include.advanced_daily_dashboard_refresh__helpers import (
    market_holidays,
    extract_market_data,
    replication_task_config
)
from include.market_etl__taskflow_api__helpers import flatten_market_data, transform_market_data, load_market_data
from plugins.custom_postgres_to_s3_operator import CustomPostgresToS3Operator
from plugins.custom_s3_to_snowflake_operator import CustomS3ToSnowflakeOperator


# Instantiate the DAG
with DAG(
    dag_id="advanced_daily_dashboard_refresh",
    # Use a different date range than the previously defined DAGs
    start_date=datetime(2023, 5, 1),
    end_date=datetime(2023, 5, 31),
    schedule="0 12 * * *",
    catchup=True,
    max_active_runs=1,
    template_searchpath="include/sql",  # This path can be searched for templated SQL queries (in .sql files)
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1)
    }
) as dag:

    # Like before, create a transient start task
    start: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="start"
    )

    # Create a branch operator
    @task.branch
    def check_market_open(**context: Dict[str, Any]) -> str:
        # Pull from the API
        ds: str = context.get("ds")

        # Check if is_weekend or is_market_holiday
        is_weekend: bool = True if datetime.strptime(ds, "%Y-%m-%d").weekday() > 4 else False
        is_market_holiday: bool = ds in market_holidays

        if is_weekend or is_market_holiday:
            return ["market_closed_notification"]

        # Kick off the market ETL
        return ["process_market_data.extract_market_data"]


    # Create a transient notification operator
    market_closed_notification: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="market_closed_notification"
    )

    @task_group(group_id="process_market_data")
    def process_market_data():
        # Use previously defined tasks for market work (using the TaskFlow API)
        raw_data = extract_market_data()
        flattened_data = flatten_market_data(raw_data)
        transformed_data = transform_market_data(flattened_data)
        load_market_data(transformed_data)


    # Create TaskGroups
    replication_task_groups: List[TaskGroup] = []

    for replication_task in replication_task_config:
        with TaskGroup(group_id=f"{replication_task.get('task_name')}_replication_task_group") as task_group:
            # Replicate tables to S3
            postgres_to_s3: CustomPostgresToS3Operator = CustomPostgresToS3Operator(
                dag=dag,
                task_id="postgres_to_s3",
                postgres_conn_id="postgres_utility_conn",
                postgres_sql=replication_task.get("sql"),
                aws_conn_id="aws_utility_conn",
                s3_bucket="s3://replication-bucket",
                s3_key=replication_task.get("s3_key")
            )

            # S3 to Snowflake
            s3_to_snowflake: CustomS3ToSnowflakeOperator = CustomS3ToSnowflakeOperator(
                dag=dag,
                task_id="s3_to_snowflake",
                aws_conn_id="aws_utility_conn",
                s3_bucket="s3://replication-bucket",
                s3_key=replication_task.get("s3_key"),
                snowflake_conn_id="snowflake_utility_conn",
                snowflake_stage="s3_stage",
                snowflake_schema="daily_operations",
                snowflake_table=replication_task.get("snowflake_table")
            )

            # Set dependencies, add TaskGroup to list of TaskGroups
            postgres_to_s3 >> s3_to_snowflake
            replication_task_groups.append(task_group)

    # Invoke a Lambda to clean up S3
    archive_s3_files: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="archive_s3_files"
    )

    # Similar to the other DAGs that have been defined, create a transient end task
    end: EmptyOperator = EmptyOperator(
        dag=dag,
        task_id="end"
    )

    # Set dependencies
    start >> check_market_open() >> [process_market_data(), market_closed_notification] >> end
    start >> replication_task_groups >> archive_s3_files >> end


# Last line of file
