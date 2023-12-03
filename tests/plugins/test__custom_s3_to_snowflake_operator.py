"""
test__customer_s3_to_snowflake_operator.py

Description:
    Unit tests for this basic operator. This is an important part of the development process when building operators.
    Since this is a mostly transient operator, the unit tests aren't overly complex.

Author: Jake Roach
Date: 2023-12-03
"""

# Import modules here
import pytest
from plugins.custom_s3_to_snowflake_operator import CustomS3ToSnowflakeOperator
from airflow.models.baseoperator import BaseOperator
import inspect


# Create fixtures
@pytest.fixture()
def custom_s3_to_snowflake_operator__fixture():
    # Not defining DAG context, very basic fixture
    return CustomS3ToSnowflakeOperator(
        task_id="s3_to_snowflake__fixture",
        aws_conn_id="aws_conn_id__fixture",
        s3_bucket="s3://s3-bucket--fixture",
        s3_key="s3_key__fixture/2023-08-01.csv",  # Skipping using templating, for now
        snowflake_conn_id="snowflake_conn_id__fixture",
        snowflake_stage="snowflake_stage__fixture",
        snowflake_schema="snowflake_schema__fixture",
        snowflake_table="snowflake_table__fixture",
    )


# Test the operator
def test_custom_s3_to_snowflake_operator(custom_s3_to_snowflake_operator__fixture):
    # Check the subclass
    assert issubclass(CustomS3ToSnowflakeOperator, BaseOperator)
    assert CustomS3ToSnowflakeOperator.template_files == "s3_key"

    # Check parameters
    parameter_names = list(inspect.signature(CustomS3ToSnowflakeOperator.__init__).parameters.keys())
    assert "aws_conn_id" in parameter_names
    assert "s3_bucket" in parameter_names
    assert "s3_key" in parameter_names
    assert "snowflake_conn_id" in parameter_names
    assert "snowflake_stage" in parameter_names
    assert "snowflake_schema" in parameter_names
    assert "snowflake_table" in parameter_names

    # Check the instantiation
    assert custom_s3_to_snowflake_operator__fixture.task_id == "s3_to_snowflake__fixture"  # From inherited class
    assert custom_s3_to_snowflake_operator__fixture.aws_conn_id == "aws_conn_id__fixture"
    assert custom_s3_to_snowflake_operator__fixture.s3_bucket == "s3://s3-bucket--fixture"
    assert custom_s3_to_snowflake_operator__fixture.s3_key == "s3_key__fixture/2023-08-01.csv"
    assert custom_s3_to_snowflake_operator__fixture.snowflake_conn_id == "snowflake_conn_id__fixture"
    assert custom_s3_to_snowflake_operator__fixture.snowflake_stage == "snowflake_stage__fixture"
    assert custom_s3_to_snowflake_operator__fixture.snowflake_schema == "snowflake_schema__fixture"
    assert custom_s3_to_snowflake_operator__fixture.snowflake_table == "snowflake_table__fixture"

    # Check execution details, pass empty context
    assert custom_s3_to_snowflake_operator__fixture.execute(**{"context": None}) is None

# Last line of file
