"""
Futures Data Processing Script
------------------------------
This script processes multi-index Bloomberg futures data by:
  1. Extracting the contract maturity from the 'CURRENT_CONTRACT_MONTH_YR' column
     and mapping it to the actual settlement date (third Friday of the expiry month).
  2. Computing the time-to-maturity (TTM) in days.
  3. Reshaping the data (for the two nearest contracts) for later calendar spread calculations.

Downstream, the settlement date and TTM are used to:
  - Accumulate dividends from spot data up to maturity.
  - Determine the exact time period for forward rate calculations.
  - Interpolate appropriate risk-free (OIS) rates.
  
This script is designed for three indices: SPX, NDX, and INDU,
with futures tickers like:
  - SPX: ['ES1 Index', 'ES2 Index', 'ES3 Index', 'ES4 Index']
  - Nasdaq: ['NQ1 Index', 'NQ2 Index', 'NQ3 Index', 'NQ4 Index']
  - DowJones: ['DM1 Index', 'DM2 Index', 'DM3 Index', 'DM4 Index']
  
The raw data is expected to be in a parquet file at:
    DATA_DIR/input/bloomberg_historical_data.parquet
and has a multi-index column structure.
------------------------------
We have a timeseries dataframe containing futures multi-index nature with columns signature looking like:

            (         'ES1 Index',                   'PX_LAST'),
            (         'ES1 Index',                 'PX_VOLUME'),
            (         'ES1 Index',                  'OPEN_INT'),
            (         'ES1 Index', 'CURRENT_CONTRACT_MONTH_YR'),
            (         'ES2 Index',                   'PX_LAST')
            
1) For each timeseries contract, we first need to extract the contract maturity from the 'CURRENT_CONTRACT_MONTH_YR' column.
    - Slice of Data (data.loc[:,('DM4 Index', 'CURRENT_CONTRACT_MONTH_YR')]): 
        - 2010-01-04    DEC 10
    - DEC 10 is an example of a contract maturity for DM4 Index with 12 month expiry, therefore on  Jan 2010, the contract mature in DEC 2010.
    - There is a rolling process, where timeseries of a contract has the same maturity date, then at maturity, it is rolled and the maturity date changes

2) We need to define a function that maps each Current Contract Month Year to the actual settlement date which is the 3rd Friday of the month.
    - First check the 'CURRENT_CONTRACT_MONTH_YR' column splitn away the months and ensure they exist in [MAR, JUN, SEP, DEC]
        - This should be true for all contracts else we have an anomaly, raise error, check dataset
    - Write a function that takes in 'CURRENT_CONTRACT_MONTH_YR' which is string type data, and returns the actual settlement date, and TTM calculations
    - Clean and tidy data

"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import calendar
import logging
import sys
from pathlib import Path
sys.path.insert(1, "./src")
from settings import config
from datetime import datetime
# Configuration from settings
DATA_DIR = config("DATA_DIR")
TEMP_DIR = config("TEMP_DIR")
INPUT_DIR = config("INPUT_DIR")
PROCESSED_DIR = config("PROCESSED_DIR")
DATA_MANUAL = config("MANUAL_DATA_DIR")


log_file_path = TEMP_DIR/f'futures_processing.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def get_third_friday(year, month):
    """
    Calculate the third Friday of a given month and year.
    
    Args:
        year (int): Year
        month (int): Month (1-12)
        
    Returns:
        datetime: Date object for the third Friday
    """
    # Use calendar.monthcalendar: each week is a list of ints (0 if day not in month)
    month_cal = calendar.monthcalendar(year, month)
    # The first week that has a Friday (weekday index 4)
    fridays = [week[calendar.FRIDAY] for week in month_cal if week[calendar.FRIDAY] != 0]
    if len(fridays) < 3:
        raise ValueError(f"Not enough Fridays in {year}-{month}")
    return datetime(year, month, fridays[2])  # third Friday

def parse_contract_month_year(contract_str):
    """
    Parse Bloomberg's contract month/year string (e.g., 'DEC 10') into
    a month number and a full year.
    
    Args:
        contract_str (str): Contract month/year string
        
    Returns:
        tuple: (month_num, year_full) or (None, None) if invalid.
    """
    if pd.isna(contract_str) or contract_str.strip() == '':
        return None, None
    parts = contract_str.split()
    if len(parts) != 2:
        logger.warning(f"Unexpected contract format: {contract_str}")
        return None, None
    month_abbr, year_abbr = parts
    allowed = {"MAR": 3, "JUN": 6, "SEP": 9, "DEC": 12}
    if month_abbr.upper() not in allowed:
        raise ValueError(f"Contract month {month_abbr} not in allowed set {list(allowed.keys())}")
    month_num = allowed[month_abbr.upper()]
    try:
        yr = int(year_abbr)
        year_full = 2000 + yr if yr < 50 else 1900 + yr
    except ValueError:
        logger.warning(f"Could not parse year: {year_abbr}")
        return None, None
    return month_num, year_full

def process_index_futures(data, futures_codes):
    """
    Process futures data for one index.
    
    Args:
        data (pd.DataFrame): Multi-index DataFrame with Bloomberg data (indexed by Date)
        futures_codes (list): List of futures codes (e.g., ['ES1', 'ES2', ...])
        
    Returns:
        dict: Dictionary of processed DataFrames, one for each futures code.
              Each DataFrame contains:
                  - Date
                  - Futures_Price (from PX_LAST)
                  - Volume, OpenInterest (if available)
                  - ContractSpec (raw CURRENT_CONTRACT_MONTH_YR)
                  - SettlementDate (actual settlement, 3rd Friday)
                  - TTM (time-to-maturity in days)
    """
    result_dfs = {}
    for code in futures_codes:
        logger.info(f"Processing futures data for {code} Index")
        try:
            # Extract columns for this contract:
            # Expecting columns like: (f'{code} Index', 'PX_LAST'), (f'{code} Index', 'CURRENT_CONTRACT_MONTH_YR'), etc.
            price_series = data.loc[:, (f'{code} Index', 'PX_LAST')]
            volume_series = data.loc[:, (f'{code} Index', 'PX_VOLUME')]
            oi_series = data.loc[:, (f'{code} Index', 'OPEN_INT')]
            contract_series = data.loc[:, (f'{code} Index', 'CURRENT_CONTRACT_MONTH_YR')]
            
            # Create a DataFrame for this contract; index is Date (from raw data)
            df_contract = pd.DataFrame({
                'Date': data.index,
                'Futures_Price': price_series,
                'Volume': volume_series,
                'OpenInterest': oi_series,
                'ContractSpec': contract_series
            })
            df_contract = df_contract.reset_index(drop=True)
            
            # Parse contract specification and compute settlement date
            settlement_dates = []
            for cs in df_contract['ContractSpec']:
                month_num, year_full = parse_contract_month_year(cs)
                if month_num is None or year_full is None:
                    settlement_dates.append(None)
                else:
                    settlement_dates.append(get_third_friday(year_full, month_num))
            df_contract['SettlementDate'] = pd.to_datetime(settlement_dates)
            
            # Compute TTM in days: SettlementDate - Date
            df_contract['Date'] = pd.to_datetime(df_contract['Date'])
            df_contract['TTM'] = (df_contract['SettlementDate'] - df_contract['Date']).dt.days
            
            # Drop rows with missing TTM (if settlement date couldn't be computed)
            df_contract = df_contract.dropna(subset=['TTM'])
            result_dfs[code] = df_contract
            logger.info(f"Processed {code}: {len(df_contract)} rows")
        except Exception as e:
            logger.error(f"Error processing {code}: {e}")
            continue
    return result_dfs

def merge_calendar_spreads(all_futures):
    """
    For each index, merge the processed data for the two nearest futures contracts (Term 1 and Term 2)
    on the Date field, and then combine the calendar spreads for all indices.
    
    Args:
        all_futures (dict): Dictionary keyed by index code (e.g., 'SPX', 'NDX', 'INDU') where the value is
                            another dictionary mapping futures code to its processed DataFrame.
                            
    Returns:
        pd.DataFrame: Combined calendar spread data for all indices.
    """
    combined = []
    # For each index, assume the first two codes in the list are the two nearest contracts.
    # For SPX, these would be ['ES1', 'ES2'].
    for index_code, fut_dict in all_futures.items():
        # Identify term1 and term2 codes:
        codes = list(fut_dict.keys())
        if len(codes) < 2:
            logger.warning(f"Not enough futures data for {index_code}")
            continue
        term1 = fut_dict[codes[0]].copy()
        term2 = fut_dict[codes[1]].copy()
        # Add a prefix so that we can merge and distinguish columns:
        term1 = term1.add_prefix('Term1_')
        term2 = term2.add_prefix('Term2_')
        # Rename the Date columns back to 'Date' for merging
        term1.rename(columns={'Term1_Date': 'Date'}, inplace=True)
        term2.rename(columns={'Term2_Date': 'Date'}, inplace=True)
        merged = pd.merge(term1, term2, on='Date', how='inner')
        merged['Index'] = index_code
        combined.append(merged)
        # Save each index’s calendar spread separately
        output_file = PROCESSED_DIR / f"{index_code}_calendar_spread.csv"
        merged.to_csv(output_file , index=False)
        logger.info(f"Saved calendar spread for {index_code}: {len(merged)} rows")
        logger.info(f"DataFrame merged:\n{merged.head()}")
    if combined:
        combined_df = pd.concat(combined, ignore_index=True)
        output_file = PROCESSED_DIR / "all_indices_calendar_spreads.csv"
        combined_df.to_csv(output_file, index=False)
        logger.info(f"Saved combined calendar spread data: {len(combined_df)} rows")
        logger.info(f"DataFrame combined:\n{combined_df.head()}")
        return combined_df
    else:
        logger.warning("No valid calendar spread data to combine")
        return None

def main():
    """
    Main function to process raw futures data from a parquet file.
    
    It:
      - Loads the multi-index raw data from DATA_DIR/input/bloomberg_historical_data.parquet
      - Processes each index (SPX, NDX, INDU) futures for the two nearest contracts
      - Extracts settlement dates and computes TTM
      - Merges the Term 1 and Term 2 data to form calendar spread datasets
      - Saves both individual and combined outputs for downstream spread calculations.
    """
    try:
        try:
            INPUT_FILE = INPUT_DIR / "bloomberg_historical_data.parquet"
            raw_data = pd.read_parquet(INPUT_FILE)
        except Exception as e:
            INPUT_FILE = DATA_MANUAL / "bloomberg_historical_data.parquet"
            raw_data = pd.read_parquet(INPUT_FILE)
        logger.info(f"Loading raw data from {INPUT_FILE}")
        if not isinstance(raw_data.index, pd.DatetimeIndex):
            raw_data.index = pd.to_datetime(raw_data.index)
        indices = {
            'SPX': ['ES1', 'ES2', 'ES3', 'ES4'],
            'NDX': ['NQ1', 'NQ2', 'NQ3', 'NQ4'],
            'INDU': ['DM1', 'DM2', 'DM3', 'DM4']
        }
        
        all_futures = {}
        for index_code, futures_codes in indices.items():
            logger.info(f"Processing futures for index {index_code}")
            processed = process_index_futures(raw_data, futures_codes)
            all_futures[index_code] = processed
        
        # Merge calendar spreads (using only Term 1 and Term 2)
        combined_spreads = merge_calendar_spreads(all_futures)
        logger.info("Futures data processing completed successfully")
    
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    main()



