"""
custom_s3_to_snowflake_operator.py

Description:
    Similar to the CustomPostgresToS3Operator, mocking an operator to move data from S3 to Snowflake.

Author: Jake Roach
Date: 2023-12-02
"""

# Import modules here
from airflow.models.baseoperator import BaseOperator
from typing import Tuple, Dict, Any


# Define the operator
class CustomS3ToSnowflakeOperator(BaseOperator):
    """
    Description:
        Operator that moves data to S3 into Snowflake.
    """

    template_files: Tuple[str] = (
        "s3_key"
    )

    def __init__(
            self,
            aws_conn_id: str,
            s3_bucket: str,
            s3_key: str,
            snowflake_conn_id: str,
            snowflake_stage: str,
            snowflake_schema: str,
            snowflake_table: str,
            **kwargs: Dict[Any, Any]
    ) -> None:
        # Instantiate variables
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.snowflake_conn_id = snowflake_conn_id
        self.snowflake_stage = snowflake_stage
        self.snowflake_schema = snowflake_schema
        self.snowflake_table = snowflake_table

    def execute(self, context):
        # Print the S3 bucket
        print("Data from below S3 path will be written to Snowflake:")
        print(f"{self.s3_bucket}/{self.s3_key}")

        # Print the Snowflake information
        print("Location in Snowflake data from above will be written to:")
        print(f"STAGE:  {self.snowflake_stage}")
        print(f"SCHEMA: {self.snowflake_schema}")
        print(f"TABLE:  {self.snowflake_table}")

# Last line of file
