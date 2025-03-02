import pytest
import pandas as pd
from futures_data_processing import (
    get_third_friday, parse_contract_month_year,
    process_index_futures, merge_calendar_spreads
)

# Sample data for testing
sample_contracts = [
    ("H24", (2024, 3)),  # March 2024 contract
    ("M24", (2024, 6)),  # June 2024 contract
    ("U24", (2024, 9)),  # September 2024 contract
    ("Z24", (2024, 12))  # December 2024 contract
]

@pytest.mark.parametrize("contract_str, expected", sample_contracts)
def test_parse_contract_month_year(contract_str, expected):
    assert parse_contract_month_year(contract_str) == expected

@pytest.mark.parametrize("year, month, expected_day", [
    (2024, 3, 15),  # March 15, 2024 (third Friday)
    (2024, 6, 21),  # June 21, 2024 (third Friday)
    (2024, 9, 20),  # September 20, 2024 (third Friday)
    (2024, 12, 20)  # December 20, 2024 (third Friday)
])
def test_get_third_friday(year, month, expected_day):
    assert get_third_friday(year, month).day == expected_day

# Load sample CSV data
def load_csv_data(file_path):
    return pd.read_csv(file_path)

def test_process_index_futures():
    data = load_csv_data("all_indices_calendar_spreads.csv")
    futures_codes = ["ES1 Index", "NQ1 Index", "DM1 Index"]
    processed_data = process_index_futures(data, futures_codes)
    assert isinstance(processed_data, pd.DataFrame)
    assert not processed_data.empty

def test_merge_calendar_spreads():
    spx_data = load_csv_data("SPX_calendar_spread.csv")
    ndx_data = load_csv_data("NDX_calendar_spread.csv")
    indu_data = load_csv_data("INDU_calendar_spread.csv")

    merged_data = merge_calendar_spreads([spx_data, ndx_data, indu_data])
    assert isinstance(merged_data, pd.DataFrame)
    assert not merged_data.empty
