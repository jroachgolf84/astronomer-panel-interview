"""
custom_postgres_to_s3_operator.py

Description:
    "Mocking" an operator that moves data from Postgres to S3. This operator only prints details to logs.

Author: Jake Roach
Date: 2023-12-02
"""

# Import modules here
from airflow.models.baseoperator import BaseOperator
from typing import Tuple, Dict, Any


# Define the operator
class CustomPostgresToS3Operator(BaseOperator):
    """
    Description:
        Operator that takes a query, and replicates the data to S3.
    """

    template_files: Tuple[str] = (
        "postgres_sql",
        "s3_bucket",
        "s3_key"
    )

    def __init__(
            self,
            postgres_conn_id: str,
            postgres_sql: str,
            aws_conn_id: str,
            s3_bucket: str,
            s3_key: str,
            **kwargs: Dict[Any, Any]
    ) -> None:
        # Instantiate variables
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.postgres_sql = postgres_sql
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket,
        self.s3_key = s3_key

    def execute(self, context):
        # Print the query
        print("Executing the following query:")
        print(self.postgres_sql)

        # Print the S3 bucket
        print("Query results written as a .csv file to the following location in S3:")
        print(f"{self.s3_bucket}/{self.s3_key}")

# Last line of file
