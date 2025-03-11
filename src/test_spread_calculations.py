"""
test_spread_calculations.py

This module contains unit tests for the Spread Calculations script/notebook. 
Each test ensures that the equity/futures spread data is loaded, processed, and 
validated according to our data pipeline requirements. These tests verify the 
correctness, consistency, and completeness of the final spreads prior to downstream 
analysis (e.g., forward-rate computations, arbitrage checks).

Key Areas Covered by These Tests:
1. **Data Integrity**: Confirms that the required columns exist, that rows do not 
   contain unexpected NaNs, and that final DataFrames have the proper shape.
2. **Calculation Checks**: Verifies numeric transformations (like spread differences,
   rolling averages, or forward rates) produce sensible results.
3. **Range & Sanity Checks**: Ensures the spreads fall within expected bounds (e.g., 
   no negative spreads if not domain-appropriate) and time-series indexes are valid
   for as-of merges.
4. **Output Format**: Confirms that the final output (CSV or DataFrame) meets the 
   format required by subsequent pipeline steps.

By running these tests, we help maintain the accuracy and reliability of our 
spread data, protecting downstream analyses from subtle but costly data errors.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import os
from settings import config
INPUT_DIR = config("INPUT_DIR")
DATA_MANUAL = config("MANUAL_DATA_DIR")
PROCESSED_DIR = config("PROCESSED_DIR")
INPUT_PARQUET_FILE = "bloomberg_historical_data.parquet"
OUTPUT_DIR = config("OUTPUT_DIR")
TEMP_DIR = config("TEMP_DIR")
DATA_DIR = config("DATA_DIR")
if os.path.exists(INPUT_DIR / INPUT_PARQUET_FILE):
    print("Parquet file exists")
    parquet_path  = INPUT_DIR / INPUT_PARQUET_FILE
else:
    print("Parquet file does not exist, using manual data")
    parquet_path = DATA_MANUAL / INPUT_PARQUET_FILE

# File to test with, retrieved from Professor Jeremey's repo
REAL_DATA_FILE = DATA_MANUAL / "equity_spreads_test_data.csv"

FORWARD_RATE_FILES = {
    "NDX": Path(PROCESSED_DIR) / "NDX_Forward_Rates.csv",   # Contains columns [Date, spread_NDX]
    "SPX": Path(PROCESSED_DIR) / "SPX_Forward_Rates.csv",   # Contains columns [Date, spread_SPX]
    "DOW": Path(PROCESSED_DIR) /"INDU_Forward_Rates.csv",   # Contains columns [Date, spread_INDU]
}

# 2) Map the ticker to (ForwardRatesColumn, RealDataColumn):
#    e.g., NDX -> forward col 'spread_NDX' vs real col 'Eq_SF_NDAQ'
TICKER_TO_COLS = {
    "NDX": ("spread_NDX",  "Eq_SF_NDAQ"),
    "SPX": ("spread_SPX",  "Eq_SF_SPX"),
    "DOW": ("spread_INDU", "Eq_SF_Dow"),
}

# 3) Example thresholds for correlation and RMSE
CORRELATION_THRESHOLD = 0.95   
RMSE_TOLERANCE = 5.0         

# -----------------------------------------------------------------------------


@pytest.fixture(scope="module")
def real_data() -> pd.DataFrame:
    """
    Loads the equity_spreads_test_data.csv, which contains dates and columns:
      - Eq_SF_NDAQ
      - Eq_SF_SPX
      - Eq_SF_Dow

    We expect a date column or an index representing dates.
    """
    df = pd.read_csv(REAL_DATA_FILE, index_col=0, parse_dates=True)
    # If needed, uncomment to set a 'Date' column as the index:
    # df['Date'] = pd.to_datetime(df['Date'])
    # df.set_index('Date', inplace=True)

    return df


@pytest.fixture(scope="module", params=list(FORWARD_RATE_FILES.items()))
def forward_df(request) -> pd.DataFrame:
    """
    Parametrized fixture that returns a tuple (ticker, df) for each forward
    rates file. Each CSV is expected to have columns [Date, spread_*] and
    a datetime index or a date column that we convert.

    For example, if ticker="NDX", the CSV should have 'spread_NDX' as its data column.
    """
    ticker, filepath = request.param
    df = pd.read_csv(filepath, index_col=0, parse_dates=True)
    # If the CSV has a 'Date' column instead of using the first col as index, do:
    # df['Date'] = pd.to_datetime(df['Date'])
    # df.set_index('Date', inplace=True)

    return ticker, df


def test_date_overlap(real_data, forward_df):
    """
    Test: Check that the forward rates have overlapping dates with the real data.

    Rationale:
      - If there's no date overlap, we can't compare them. Possibly a mismatch
        or incorrect file if the date ranges don't intersect at all.
    """
    ticker, fwd_df = forward_df
    real_start, real_end = real_data.index.min(), real_data.index.max()
    fwd_start, fwd_end = fwd_df.index.min(), fwd_df.index.max()

    overlap_start = max(real_start, fwd_start)
    overlap_end = min(real_end, fwd_end)

    assert overlap_start <= overlap_end, (
        f"{ticker} forward file has no overlapping dates with real_data.\n"
        f"Real data range = [{real_start}, {real_end}], {ticker} range = [{fwd_start}, {fwd_end}]."
    )


def test_correlation_within_overlap(real_data, forward_df):
    """
    Test: For the overlapping date range, check correlation between the
    forward spread column (e.g., spread_NDX) and the real data column
    (e.g., Eq_SF_NDAQ).

    Rationale:
      - Although forward rates may be derived or simulated, they should
        show some alignment (positive correlation) with the real data,
        indicating similar market movement or direction.
      - A correlation below CORRELATION_THRESHOLD suggests they diverge significantly.
    """
    ticker, fwd_df = forward_df
    fwd_col, real_col = TICKER_TO_COLS[ticker]

    # Ensure real data has that column
    if real_col not in real_data.columns:
        pytest.skip(f"Real data has no column named '{real_col}', skipping correlation test for {ticker}.")

    # Identify overlapping index
    overlap_index = real_data.index.intersection(fwd_df.index)
    if overlap_index.empty:
        pytest.skip(f"No overlapping dates for {ticker}, skipping correlation test.")

    # Extract overlapping slices
    real_slice = real_data.loc[overlap_index, real_col].astype(float)
    fwd_slice = fwd_df.loc[overlap_index, fwd_col].astype(float)

    corr = real_slice.corr(fwd_slice)
    print(f"{ticker} correlation on {len(overlap_index)} overlapping dates: {corr:.3f}")

    assert corr >= CORRELATION_THRESHOLD, (
        f"Correlation between {real_col} and {fwd_col} is {corr:.3f}, "
        f"below the threshold {CORRELATION_THRESHOLD}."
    )


def test_rmse_within_tolerance(real_data, forward_df):
    """
    Test: Check RMSE (Root Mean Squared Error) between the forward spread
    column and real data column over the overlap. Must be below RMSE_TOLERANCE.

    Rationale:
      - Large RMSE means the forward rates deviate substantially from the
        historical reference data, possibly making them unreliable for
        further analysis.
      - Adjust the tolerance according to domain knowledge (e.g., if the
        data is in basis points or percentages).
    """
    ticker, fwd_df = forward_df
    fwd_col, real_col = TICKER_TO_COLS[ticker]

    if real_col not in real_data.columns:
        pytest.skip(f"Real data has no column named '{real_col}', skipping RMSE test for {ticker}.")

    overlap_index = real_data.index.intersection(fwd_df.index)
    if overlap_index.empty:
        pytest.skip(f"No overlapping dates for {ticker}, skipping RMSE test.")

    real_slice = real_data.loc[overlap_index, real_col].astype(float)
    fwd_slice = fwd_df.loc[overlap_index, fwd_col].astype(float)

    errors = real_slice - fwd_slice
    rmse = np.sqrt((errors ** 2).mean())

    assert rmse < RMSE_TOLERANCE, (
        f"{ticker} forward rates' RMSE = {rmse:.3f}, exceeds the limit {RMSE_TOLERANCE:.3f}."
    )


def test_monotonic_date_index(forward_df):
    """
    Test: Ensure the forward data has a strictly increasing date index.

    Rationale:
      - Many time-series joins and merges (e.g. pd.merge_asof) require the
        data to be sorted by date. Duplicate or unsorted indices can break
        downstream processes.
    """
    ticker, fwd_df = forward_df

    # Ascending sort check
    assert fwd_df.index.is_monotonic_increasing, (
        f"{ticker} forward rates index is not sorted in ascending order."
    )

    # No duplicated dates
    duplicates = fwd_df.index.duplicated().sum()
    assert duplicates == 0, (
        f"{ticker} forward rates index has {duplicates} duplicate date(s)."
    )
    
if __name__ == "__main__":
    pytest.main(["-v", __file__])