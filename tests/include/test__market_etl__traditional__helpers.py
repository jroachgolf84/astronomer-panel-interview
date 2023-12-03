"""
test__market_etl__traditional__helpers.py

Description:
    Unit tests for functions (and other code) stored in the include/test__market_etl__traditional__helpers.py file. This
    file is just for testing purposes, and only basic tests will be written and executed.

Author: Jake Roach
Date: 2023-12-03
"""

# Import modules needed here
import pytest
import pandas as pd

# Import functions to be tested
from include.market_etl__traditional__helpers import (
    flatten_market_data__callable,
    transform_market_data__callable
)


# Create any needed fixtures here
@pytest.fixture()
def raw_dataset__fixture():
    # This is for a run on 2023-08-01
    return [
        {
            'status': 'OK',
            'from': '2023-08-01',
            'symbol': 'AAPL',
            'open': 196.235,
            'high': 196.73,
            'low': 195.28,
            'close': 195.605,
            'volume': 35281426.0,
            'afterHours': 195.3,
            'preMarket': 196.35
        }, {
            'status': 'OK',
            'from': '2023-08-01',
            'symbol': 'AMZN',
            'open': 133.55,
            'high': 133.69,
            'low': 131.6199,
            'close': 131.69,
            'volume': 42250989.0,
            'afterHours': 131.26,
            'preMarket': 133.57
        }
    ]


@pytest.fixture()
def flattened_dataset__fixture():
    return [
        ["status", "from", "symbol", "open", "high", "low", "close", "volume"],
        ["OK", "2023-08-01", "AAPL", 196.235, 196.73, 195.28, 195.605, 35281426.0],
        ["OK", "2023-08-01", "AMZN", 133.55, 133.69, 131.6199, 131.69, 42250989.0]
    ]


# Define unit tests here
def test_flatten_market_data__callable(raw_dataset__fixture, flattened_dataset__fixture):
    flattened_dataset = flatten_market_data__callable(raw_dataset__fixture)
    assert flattened_dataset == flattened_dataset__fixture
    assert isinstance(flattened_dataset, list)

    for record in flattened_dataset:
        assert isinstance(record, list)


def test_transform_market_data__callable(flattened_dataset__fixture):
    transformed_dataset = transform_market_data__callable(flattened_dataset__fixture)

    # Check the type, columns, number of rows, etc
    assert isinstance(transformed_dataset, pd.DataFrame)
    assert transformed_dataset.shape == (2, 8)
    assert list(transformed_dataset.columns) == [
        "market_date",
        "ticker",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "change",
        "volume",
    ]

# Last line of file
