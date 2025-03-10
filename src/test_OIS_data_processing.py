"""
OIS Data Processing Test Suite
------------------------------

This module provides comprehensive testing for the OIS (Overnight Indexed Swap) 
rate data processing pipeline, which is a critical component in the equity 
spot-futures arbitrage analysis.

The test suite validates:
1. Data integrity (non-empty, correct columns, no missing values)
2. Data format (decimal representation, DatetimeIndex, sorted)
3. Data quality (reasonable bounds, continuity, sufficient coverage)
4. Output format (correct CSV structure for downstream processes)

These tests ensure that the OIS data processing component meets all requirements
for the forward rate calculations and arbitrage spread analysis. Each test 
includes detailed rationale explaining its importance in the context of the
overall analytical pipeline.

The tests use the actual processing function and data sources to verify both
correct implementation and appropriate data quality.
"""

import pytest
import pandas as pd
from pathlib import Path
from settings import config
from OIS_data_processing import process_ois_data
import os

INPUT_DIR = config("INPUT_DIR")
DATA_MANUAL = config("MANUAL_DATA_DIR")
PROCESSED_DIR = config("PROCESSED_DIR")
INPUT_PARQUET_FILE = "bloomberg_historical_data.parquet"

if os.path.exists(INPUT_DIR / INPUT_PARQUET_FILE):
    print("Parquet file exists")
    parquet_path  = INPUT_DIR / INPUT_PARQUET_FILE
else:
    print("Parquet file does not exist, using manual data")
    parquet_path = DATA_MANUAL / INPUT_PARQUET_FILE

START_DATE = "2010-01-01"
END_DATE = "2024-12-31"


@pytest.fixture(scope="module")
def ois_df() -> pd.DataFrame:
    """
    Loads the actual parquet file from INPUT_DIR using `process_ois_data`.
    """
    return process_ois_data(parquet_path)


def test_not_empty(ois_df):
    """
    Test #1: Check for Non-Empty DataFrame

    Rationale:
      - An empty DataFrame would cause all subsequent calculations to fail.
      - This basic check ensures we have data to work with before proceeding.
    """
    assert not ois_df.empty, "OIS DataFrame should not be empty"


def test_column_exists(ois_df):
    """
    Test #2: Verify OIS_3M Column Exists

    Rationale:
      - The entire analysis depends on the 3-month OIS rate.
      - If this column is missing or incorrectly named, all downstream calculations
        would fail or produce meaningless results.
    """
    assert "OIS_3M" in ois_df.columns, "OIS_3M column should exist in the DataFrame"


def test_decimal_format(ois_df):
    """
    Test #3: Confirm Decimal Format Conversion

    Rationale:
      - The downstream forward rate calculations require OIS rates in decimal format
        (e.g., 0.0325 rather than 3.25%).
      - Using percentage values would amplify implied forward rates by 100x, creating
        dramatically incorrect arbitrage spreads.
    """
    assert (ois_df['OIS_3M'] < 1.0).all(), "OIS rates should be in decimal format (less than 1.0)"


def test_no_missing_values(ois_df):
    """
    Test #4: Ensure No Missing Values

    Rationale:
      - Missing OIS values would create gaps in our arbitrage spread calculations.
      - We enforce a clean dataset to prevent unexplained NaN values later in the analysis pipeline.
    """
    missing_count = ois_df['OIS_3M'].isna().sum()
    assert missing_count == 0, f"There should be no missing values, but found {missing_count}"


def test_reasonable_rate_bounds(ois_df):
    """
    Test #5: Validate Reasonable Rate Range

    Rationale:
      - OIS rates should be positive (>=0) and realistically below 50% (<0.5 in decimal).
      - Values outside this range likely indicate data errors that would distort our arbitrage analysis.
    """
    assert (ois_df['OIS_3M'] >= 0).all(), "OIS rates should not be negative"
    assert (ois_df['OIS_3M'] < 0.5).all(), "OIS rates should be less than 0.50 in decimal"


def test_datetime_index(ois_df):
    """
    Test #6: Verify DatetimeIndex

    Rationale:
      - The as-of merge in the forward rate calculation requires a DatetimeIndex.
      - This merge operation finds the latest OIS rate available on or before each futures
        observation date, making proper datetime indexing essential.
    """
    assert isinstance(ois_df.index, pd.DatetimeIndex), "Index should be a DatetimeIndex"


def test_data_continuity(ois_df):
    """
    Test #7: Check Data Continuity for Merging Consistency (10-day threshold)

    Rationale:
      - The as-of merge requires reasonable continuity in OIS data.
      - We use a 10-day threshold because:
        1. Trading days typically occur 5 days per week
        2. A 10-day gap represents approximately two weeks of calendar time
        3. Longer gaps would mean using significantly outdated OIS rates in the merge
        4. The downstream equity arbitrage calculation uses OIS as a benchmark risk-free rate,
           which becomes unreliable if too stale
        5. Large gaps in OIS data could mean many futures data points match to the same outdated rate.
    """
    date_diffs = ois_df.index.to_series().diff().dt.days.dropna()
    max_gap = date_diffs.max()
    gap_days_threshold = 10
    assert max_gap <= gap_days_threshold, \
        f"Max gap in OIS data is {max_gap} days, should be ≤ {gap_days_threshold} days"


def test_index_sorted(ois_df):
    """
    Test #8: Confirm Sorted Index

    Rationale:
      - The `pd.merge_asof()` function specifically requires sorted time series data.
      - If the index isn't monotonically increasing, the as-of merge will fail or produce incorrect matches.
    """
    assert ois_df.index.is_monotonic_increasing, "Index must be sorted in ascending order"


def test_date_coverage(ois_df):
    """
    Test #9: Verify Date Range Coverage

    Rationale:
      - We target at least 70% coverage of the specified date range because:
        1. Insufficient coverage could bias our arbitrage spread analysis toward certain time periods
        2. Most financial analyses require reasonable coverage across market cycles
        3. The coverage ratio accounts for weekends and holidays (when markets are closed)
        4. Significant gaps could lead to seasonal biases in our arbitrage spread analysis
    """
    start = pd.to_datetime(START_DATE)
    end = pd.to_datetime(END_DATE)

    data_start = ois_df.index.min()
    data_end = ois_df.index.max()

    total_days = (end - start).days
    coverage_ratio = len(ois_df) / total_days

    assert coverage_ratio >= 0.7, \
        f"Date coverage ratio is {coverage_ratio:.2f}, expected ≥ 0.7"


def test_csv_output_exists(ois_df):
    """
    Test #10: Validate CSV Output Format

    Rationale:
      - The forward rate calculation script explicitly loads "cleaned_ois_rates.csv" and expects:
        1. A date column (either as 'Date' or in 'Unnamed: 0')
        2. An 'OIS_3M' column with decimal-formatted rates
      - This test verifies that our output file meets these requirements,
        ensuring seamless integration with downstream processes.
    """
    output_path = Path(PROCESSED_DIR) / "cleaned_ois_rates.csv"
    assert output_path.exists(), "Output CSV file must exist"

    test_df = pd.read_csv(output_path)
    # Check that date column exists (often as 'Date' or 'Unnamed: 0')
    date_col_exists = any(col in ["Date", "Unnamed: 0"] for col in test_df.columns)
    assert date_col_exists, "CSV must have a date column for merging"

    # Check that OIS_3M column exists
    assert "OIS_3M" in test_df.columns, "CSV must have an 'OIS_3M' column"
    
if __name__ == "__main__":
    pytest.main(["-v", __file__])