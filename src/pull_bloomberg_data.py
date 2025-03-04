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

OIS_TICKERS = {
    "OIS_1W": "USSO1Z CMPN Curncy",
    "OIS_1M": "USSO1 CMPN Curncy",
    "OIS_3M": "USSOC CMPN Curncy",
    "OIS_6M": "USSOF CMPN Curncy",
    "OIS_1Y": "USSO10 CMPN Curncy"
}

def pull_spot_div_data(tickers, start_date, end_date):
    try:
        logger.info(f"Extracting spot/dividend data for {tickers}")
        fields = ["PX_LAST", "IDX_EST_DVD_YLD", "INDX_GROSS_DAILY_DIV"]
        df = blp.bdh(tickers, fields, start_date=start_date, end_date=end_date)
        df.index = pd.to_datetime(df.index)
        return df
    except Exception as e:
        logger.error(f"Error pulling spot data for {tickers}: {e}")
        return pd.DataFrame()

def pull_futures_data(tickers, start_date, end_date):
    try:
        logger.info(f"Extracting futures data for {tickers}")
        fields = ["PX_LAST", "PX_VOLUME", "OPEN_INT", "CURRENT_CONTRACT_MONTH_YR"]
        df = blp.bdh(tickers, fields, start_date=start_date, end_date=end_date)
        df.index = pd.to_datetime(df.index)
        return df
    except Exception as e:
        logger.error(f"Error pulling futures data for {tickers}: {e}")
        return pd.DataFrame()

def pull_ois_rates(tickers, start_date, end_date):
    try:
        logger.info(f"Extracting OIS rates for {tickers}")
        fields = ["PX_LAST"]
        df = blp.bdh(tickers, fields, start_date=start_date, end_date=end_date)
        df.index = pd.to_datetime(df.index)
        return df
    except Exception as e:
        logger.error(f"Error pulling OIS rates: {e}")
        return pd.DataFrame()

def main():
    if USING_XBBG:
        try:
            logger.info(f"Pulling data from {START_DATE} to {END_DATE}")

            spot_dfs, futures_dfs = [], []
            for index_name, cfg in INDEX_CONFIG.items():
                spot_df = pull_spot_div_data([cfg["spot_ticker"]], START_DATE, END_DATE)
                futures_df = pull_futures_data(cfg["futures_tickers"], START_DATE, END_DATE)
                spot_dfs.append(spot_df)
                futures_dfs.append(futures_df)

            all_spot = pd.concat(spot_dfs, axis=1) if spot_dfs else pd.DataFrame()
            all_futures = pd.concat(futures_dfs, axis=1) if futures_dfs else pd.DataFrame()

            ois_df = pull_ois_rates(list(OIS_TICKERS.values()), START_DATE, END_DATE)

            final_df = all_spot.join(all_futures, how='outer') if not all_spot.empty else all_futures
            final_df = final_df.join(ois_df, how='outer') if not final_df.empty else ois_df
            final_df.sort_index(inplace=True)
            
            INPUT_DIR.mkdir(parents=True, exist_ok=True)
            output_path = INPUT_DIR / "bloomberg_historical_data.parquet"
            final_df.to_parquet(output_path)
            logger.info(f"Final merged data saved to {output_path}")
        except Exception as e:
            logger.error(f"Error extracting Bloomberg data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            sys.exit(1)
    else:
        logger.warning("Defaulting to cached data. Set USING_XBBG=True in settings.py to pull fresh data.")

if __name__ == "__main__":
    main()
