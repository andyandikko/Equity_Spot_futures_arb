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
USING_XBBG = True  # config("USING_XBBG")
DATA_DIR = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")
TEMP_DIR = config("TEMP_DIR")
INPUT_DIR = config("INPUT_DIR")

# Setup Bloomberg access (requires xbbg and Bloomberg Terminal)
if USING_XBBG:
    from xbbg import blp
else:
    print("Warning: xbbg not available. This script needs to be run on a machine with Bloomberg access.")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(TEMP_DIR / 'bloomberg_data_extraction.log'),
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

def pull_data(tickers, fields, start_date, end_date):
    """
    Generic function to pull data from Bloomberg using xbbg.
    """
    try:
        logger.info(f"Extracting data for {tickers}")
        df = blp.bdh(tickers, fields, start_date=start_date, end_date=end_date)
        df.index = pd.to_datetime(df.index)  # Ensure index is datetime
        logger.info(f"Successfully extracted data with {len(df)} rows for {tickers}")
        return df
    except Exception as e:
        logger.error(f"Error pulling data for {tickers}: {e}")
        return pd.DataFrame()

def main():
    """
    Main function to extract and merge Bloomberg data.
    """
    if USING_XBBG:
        try:
            logger.info(f"Pulling data from {START_DATE} to {END_DATE}")

            spot_dfs, futures_dfs = [], []
            
            for index_name, cfg in INDEX_CONFIG.items():
                spot_tickers = [cfg["spot_ticker"]]
                spot_df = pull_data(spot_tickers, ["PX_LAST", "IDX_EST_DVD_YLD", "INDX_GROSS_DAILY_DIV"], START_DATE, END_DATE)
                spot_dfs.append(spot_df)
                
                futures_df = pull_data(cfg["futures_tickers"], ["PX_LAST", "PX_VOLUME", "OPEN_INT", "CURRENT_CONTRACT_MONTH_YR"], START_DATE, END_DATE)
                futures_dfs.append(futures_df)

            # Merge all spot data
            all_spot = pd.concat(spot_dfs, axis=1) if spot_dfs else pd.DataFrame()
            all_futures = pd.concat(futures_dfs, axis=1) if futures_dfs else pd.DataFrame()

            # Final merge
            final_df = all_spot.join(all_futures, how='outer') if not all_spot.empty else all_futures
            final_df.sort_index(inplace=True)
            
            # Save output
            INPUT_DIR.mkdir(parents=True, exist_ok=True)
            output_path = INPUT_DIR / "bloomberg_historical_data.parquet"
            final_df.to_parquet(output_path)
            logger.info(f"Final merged data saved to {output_path}")
            logger.info(f"Merged data: {final_df.head(20).to_string()}")
        
        except Exception as e:
            logger.error(f"Error extracting Bloomberg data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            sys.exit(1)
    else:
        logger.warning("Defaulting to cached data. Set USING_XBBG=True in settings.py to pull fresh data.")

if __name__ == "__main__":
    main()


