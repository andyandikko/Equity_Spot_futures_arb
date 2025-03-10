"""
Futures Data Quality Testing Suite
----------------------------------

This module provides comprehensive data quality tests for futures calendar spread data,
which is critical for accurate equity spot-futures arbitrage calculations.

The test suite verifies:
1. Data completeness across all three major indices (SPX, NDX, INDU)
2. Proper term structure (deferred contracts have longer TTM than nearby contracts)
3. Reasonable TTM value ranges and positive futures prices 
4. Absence of missing values in critical data fields
5. Proper handling of special cases (e.g., observations on settlement dates)

These tests ensure that our processed futures data meets all requirements for
accurate arbitrage spread calculations. Clean, properly structured futures data
is essential for:
- Computing accurate forward rates
- Identifying genuine arbitrage opportunities
- Comparing spreads across different market segments

Each test includes detailed rationale explaining its importance in the context
of equity spot-futures arbitrage analysis.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import os

@pytest.fixture
def combined_spreads():
    """
    Fixture that loads the combined calendar spreads data for testing.
    
    Returns:
        pd.DataFrame: Combined calendar spreads data
    """
    from settings import config
    PROCESSED_DIR = config("PROCESSED_DIR")
    
    file_path = Path(PROCESSED_DIR) / "all_indices_calendar_spreads.csv"
    if not os.path.exists(file_path):
        pytest.skip(f"Required file {file_path} not found")
    
    return pd.read_csv(file_path)

def test_all_indices_present(combined_spreads):
    """
    Test 1: Check that we have data for all three indices
    
    Rationale:
      - All three major equity indices (SPX, NDX, INDU) are required for comprehensive analysis
      - Missing indices would create significant gaps in our comparisons and arbitrage analysis
      - This test ensures the completeness of our market coverage
    """
    expected_indices = {'SPX', 'NDX', 'INDU'}
    actual_indices = set(combined_spreads['Index'].unique())
    
    assert expected_indices == actual_indices, f"Missing indices: {expected_indices - actual_indices}"

def test_term_ordering(combined_spreads):
    """
    Test 2: Check that Term2_TTM > Term1_TTM for all rows
    
    Rationale:
      - By definition, Term2 (deferred contract) must have a longer time to maturity
        than Term1 (nearby contract)
      - This property is fundamental to calendar spread calculations
      - Violations would lead to negative time spreads and invalid forward rate calculations
    """
    assert all(combined_spreads['Term2_TTM'] > combined_spreads['Term1_TTM']), \
        "Term2 TTM should always be greater than Term1 TTM"

def test_ttm_reasonable_ranges(combined_spreads):
    """
    Test 3: Check that all TTM values are reasonable
    
    Rationale:
      - TTM values should be positive (contracts can't expire in the past)
      - Term1 TTM is typically less than 365 days for nearby contracts
      - Term2 TTM is typically less than 730 days for deferred contracts
      - Values outside these ranges could indicate data errors or calendar miscalculations
    """
    assert all(combined_spreads['Term1_TTM'] >= 0), "Term1 TTM should be non-negative"
    assert all(combined_spreads['Term2_TTM'] > 0), "Term2 TTM should be positive"
    
    assert combined_spreads['Term1_TTM'].max() < 365, \
        f"Term1 TTM max is {combined_spreads['Term1_TTM'].max()}, should be < 365 days"
    assert combined_spreads['Term2_TTM'].max() < 730, \
        f"Term2 TTM max is {combined_spreads['Term2_TTM'].max()}, should be < 730 days"

def test_no_missing_critical_values(combined_spreads):
    """
    Test 4: Check for missing values in critical columns
    
    Rationale:
      - Missing futures prices or TTM values would break the arbitrage calculations
      - These values are critical inputs to the forward rate formula
      - Complete data is essential for consistent and reliable spread calculations
    """
    critical_columns = ['Term1_Futures_Price', 'Term2_Futures_Price', 'Term1_TTM', 'Term2_TTM']
    
    for col in critical_columns:
        assert combined_spreads[col].isna().sum() == 0, f"Missing values in {col}"

def test_positive_futures_prices(combined_spreads):
    """
    Test 5: Check that futures prices are positive
    
    Rationale:
      - Futures prices must be positive for meaningful calculations
      - Zero or negative prices would cause mathematical errors in the forward rate formula
      - This test verifies the basic validity of the pricing data
    """
    assert all(combined_spreads['Term1_Futures_Price'] > 0), "Term1 futures prices should be positive"
    assert all(combined_spreads['Term2_Futures_Price'] > 0), "Term2 futures prices should be positive"

def test_ttm_zero_handling(combined_spreads):
    """
    Test 6: Specifically check for and handle Term1_TTM=0 cases
    
    Rationale:
      - We previously identified that SPX contracts sometimes have observations
        on settlement dates (TTM=0)
      - While these aren't strictly errors, they require special handling in
        the forward rate calculations
      - This test ensures that we have proper checks for these edge cases
    """
    # Identify any Term1_TTM=0 cases
    zero_ttm_rows = combined_spreads[combined_spreads['Term1_TTM'] == 0]
    
    if len(zero_ttm_rows) > 0:
        # Print informational message but don't fail the test
        print(f"Found {len(zero_ttm_rows)} rows with Term1_TTM=0")
        
        # Check that these rows don't cause problems in forward rate calculations
        # by ensuring that Term2_TTM - Term1_TTM is still positive
        assert all(zero_ttm_rows['Term2_TTM'] > zero_ttm_rows['Term1_TTM']), \
            "Even for TTM=0 cases, Term2_TTM must be greater than Term1_TTM"