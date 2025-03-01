"""
Bloomberg Data Extraction Script
--------------------------------
This script extracts equity index data from Bloomberg (spot, dividends, futures)
and USD OIS rates data, then merges them into a single multi-index timeseries DataFrame.
The final merged data is saved to a CSV file in the "input" folder under DATA_DIR.

Dependencies:
- pandas
- numpy
- xbbg
- logging
- pathlib
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import logging
import sys
import os

# Add the src directory to path to load configuration settings
sys.path.insert(1, "./src")
from settings import config

# Load configuration values
USING_XBBG = config("USING_XBBG")
DATA_DIR = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

# Setup Bloomberg access (requires xbbg and Bloomberg Terminal)
if USING_XBBG:
    from xbbg import blp
else:
    print("Warning: xbbg not available. This script needs to be run on a machine with Bloomberg access.")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bloomberg_data_extraction.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

INDEX_CONFIG = {
    "SP": {
        "spot_ticker": "SPX Index",
        "futures_tickers": ["ES1 Index", "ES2 Index", "ES3 Index", "ES4 Index"]
    },
    "Nasdaq": {
        "spot_ticker": "NDX Index",
        "futures_tickers": ["NQ1 Index", "NQ2 Index", "NQ3 Index", "NQ4 Index"]
    },
    "DowJones": {
        "spot_ticker": "INDU Index",
        "futures_tickers": ["DM1 Index", "DM2 Index", "DM3 Index", "DM4 Index"]
    }
}

OIS_TICKERS = {
    # Short tenors
    "OIS_1W": "USSO1Z CMPN Curncy",  # 1 Week
    "OIS_2W": "USSOA CMPN Curncy",    # 2 Weeks
    "OIS_3W": "USSOB CMPN Curncy",    # 3 Weeks
    "OIS_1M": "USSO1 CMPN Curncy",     # 1 Month
    "OIS_2M": "USSO2 CMPN Curncy",     # 2 Months
    "OIS_3M": "USSO3 CMPN Curncy",     # 3 Months
    "OIS_4M": "USSO4 CMPN Curncy",     # 4 Months
    "OIS_5M": "USSO5 CMPN Curncy",     # 5 Months
    "OIS_6M": "USSOF CMPN Curncy",     # 6 Months (F for Forward)
    
    # Medium tenors
    "OIS_7M": "USSO7 CMPN Curncy",     # 7 Months
    "OIS_1Y": "USSO10 CMPN Curncy",    # 1 Year
    
    # Long tenors
    "OIS_15M": "USSO15 CMPN Curncy",   # 15 Months
    "OIS_20M": "USSO20 CMPN Curncy",   # 20 Months
    "OIS_30M": "USSO30 CMPN Curncy"    # 30 Months
}


def pull_spot_div_data(tickers, start_date, end_date):
    """
    Pulls spot price and dividend data for an index from Bloomberg.
    
    Args:
        tickers (list): List of Bloomberg tickers for the index (e.g., ["SPX Index"]).
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        
    Returns:
        pandas.DataFrame: DataFrame with Date as a column and MultiIndex columns
                          for each field (e.g., PX_LAST, IDX_EST_DVD_YLD, INDX_GROSS_DAILY_DIV).
    """
    try:
        logger.info(f"Extracting spot/dividend data for {tickers}")
        fields = ["PX_LAST", "IDX_EST_DVD_YLD", "INDX_GROSS_DAILY_DIV"]
        df = blp.bdh(tickers, fields, start_date=start_date, end_date=end_date)
        df = df.reset_index()  # Ensure Date is a column
        logger.info(f"Successfully extracted spot data with {len(df)} rows for {tickers}")
        return df
    except Exception as e:
        logger.error(f"Error pulling spot data for {tickers}: {e}")
        empty_df = pd.DataFrame(columns=pd.MultiIndex.from_product([tickers, fields]))
        empty_df['Date'] = pd.DatetimeIndex([])
        empty_df.set_index('Date', inplace=True)
        return empty_df


def pull_futures_data(tickers, start_date, end_date):
    """
    Pulls futures data for an index from Bloomberg.
    
    Args:
        tickers (list): List of Bloomberg tickers for futures contracts.
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        
    Returns:
        pandas.DataFrame: DataFrame with Date as a column and MultiIndex columns 
                          (ticker, field) for each futures contract.
    """
    try:
        logger.info(f"Extracting futures data for tickers: {tickers}")
        fields = ["PX_LAST", "PX_VOLUME", "OPEN_INT", "CURRENT_CONTRACT_MONTH_YR"]
        df = blp.bdh(tickers, fields, start_date=start_date, end_date=end_date)
        df = df.reset_index()
        df.rename(columns={'index': 'Date'}, inplace=True)
        logger.info(f"Successfully extracted futures data with {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error pulling futures data for tickers {tickers}: {e}")
        empty_df = pd.DataFrame(columns=pd.MultiIndex.from_product([tickers, fields]))
        empty_df['Date'] = pd.DatetimeIndex([])
        empty_df.set_index('Date', inplace=True)
        return empty_df


def pull_ois_rates(tickers, start_date, end_date):
    """
    Pulls USD OIS rates from Bloomberg and formats the result in a DataFrame with 
    a Date index and MultiIndex columns (ticker, 'PX_LAST').

    Args:
        tickers (list): List of Bloomberg tickers for OIS rates.
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        
    Returns:
        pandas.DataFrame: DataFrame with Date as the index and MultiIndex columns,
                          where each ticker has a single field: 'PX_LAST'.
    """
    try:
        logger.info(f"Extracting OIS data for tickers: {tickers}")
        fields = ["PX_LAST"]
        ois_df = blp.bdh(tickers, fields, start_date=start_date, end_date=end_date)
        ois_df = ois_df.reset_index()
        ois_df.rename(columns={'index': 'Date'}, inplace=True)
        ois_df.set_index('Date', inplace=True)
        
        # Reformat columns into a MultiIndex (ticker, 'PX_LAST')
        new_cols = [(col, 'PX_LAST') for col in ois_df.columns]
        ois_df.columns = pd.MultiIndex.from_tuples(new_cols)
        
        logger.info(f"Successfully extracted OIS data with {len(ois_df)} rows")
        return ois_df
    except Exception as e:
        logger.error(f"Error pulling OIS rates for tickers {tickers}: {e}")
        raise


def main():
    """
    Main function to extract spot/dividend, futures, and OIS rate data from Bloomberg,
    merge them into a single multi-index timeseries DataFrame using merge operations, 
    and save the final DataFrame.
    """
    if USING_XBBG:
        try:
            logger.info(f"Pulling data from {START_DATE} to {END_DATE}")

            spot_dfs = []
            futures_dfs = []
            
            for index_name, cfg in INDEX_CONFIG.items():
                logger.info(f"Processing {index_name} spot/dividend data")
                spot_tickers = cfg["spot_ticker"]
                if isinstance(spot_tickers, str):
                    spot_tickers = [spot_tickers]
                spot_df = pull_spot_div_data(spot_tickers, START_DATE, END_DATE)
                spot_df.set_index("Date", inplace=True)
                spot_dfs.append(spot_df)
                
                logger.info(f"Processing {index_name} futures data")
                fut_df = pull_futures_data(cfg["futures_tickers"], START_DATE, END_DATE)
                fut_df.set_index("Date", inplace=True)
                futures_dfs.append(fut_df)
            all_spot = spot_dfs[0]
            for df in spot_dfs[1:]:
                all_spot = all_spot.merge(df, left_index=True, right_index=True, how='outer')
            all_futures = futures_dfs[0]
            for df in futures_dfs[1:]:
                all_futures = all_futures.merge(df, left_index=True, right_index=True, how='outer')
            ois_ticker_list = list(OIS_TICKERS.values())
            ois_df = pull_ois_rates(ois_ticker_list, START_DATE, END_DATE)
            final_df = all_spot.merge(all_futures, left_index=True, right_index=True, how='outer')
            final_df = final_df.merge(ois_df, left_index=True, right_index=True, how='outer')
            final_df.sort_index(inplace=True)
            
            # Save the final merged DataFrame to CSV in DATA_DIR / "input"
            output_path = Path(DATA_DIR) / "input" / "bloomberg_merged_data.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            final_df.to_csv(output_path)
            logger.info(f"Final merged data saved to {output_path}")
            
        except Exception as e:
            logger.error(f"Error extracting Bloomberg data: {e}")
            sys.exit(1)
    else:
        logger.warning("Defaulting to cached data. Set USING_XBBG=True in settings.py to pull fresh data.")

if __name__ == "__main__":
    main()


