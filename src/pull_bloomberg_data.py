"""
Bloomberg Data Extraction Script
--------------------------------
This script extracts equity index data from Bloomberg (spot, dividends, futures)
and OIS rates data, saving them to CSV files for further processing.

Dependencies:
- pandas
- xbbg 
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import logging
import sys
try:
    from xbbg import blp
    USING_XBBG = True
except ImportError:
    USING_XBBG = False
    print("Warning: xbbg not available. This script needs to be run on a machine with Bloomberg access.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bloomberg_data_extraction.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Create output directories if they don't exist
Path("input").mkdir(exist_ok=True)
Path("temp").mkdir(exist_ok=True)
Path("output").mkdir(exist_ok=True)

# Index configuration
INDEX_CONFIG = {
    "SP": {
        "spot_ticker": "SPX Index",
        "futures_tickers": ["ESA Comdty", "ESB Comdty", "ESC Comdty", "ESD Comdty"]
    },
    "Nasdaq": {
        "spot_ticker": "NDX Index",
        "futures_tickers": ["NQA Comdty", "NQB Comdty", "NQC Comdty", "NQD Comdty"]
    },
    "DowJones": {
        "spot_ticker": "INDU Index",
        "futures_tickers": ["DJA Comdty", "DJB Comdty", "DJC Comdty", "DJD Comdty"]
    }
}

# OIS rates tickers
OIS_TICKERS = {
    "OIS_1W": "USSW1 Curncy",
    "OIS_1M": "USSW1M Curncy",
    "OIS_3M": "USSW3M Curncy",
    "OIS_6M": "USSW6M Curncy",
    "OIS_1Y": "USSW1Y Curncy"
}

def pull_spot_div_data(ticker, start_date, end_date):
    """
    Pull spot prices and dividend data for an index from Bloomberg.
    
    Args:
        ticker (str): Bloomberg ticker for the index
        start_date (str): Start date in 'YYYY-MM-DD' format
        end_date (str): End date in 'YYYY-MM-DD' format
        
    Returns:
        pandas.DataFrame: DataFrame with spot prices and dividend data
    """
    if not USING_XBBG:
        # Create a mock dataframe for testing when Bloomberg is not available
        logger.warning("Using mock data as xbbg is not available")
        dates = pd.date_range(start=start_date, end=end_date, freq='B')
        n = len(dates)
        
        mock_data = {
            'Date': dates,
            'Spot_Price': np.random.normal(2000, 100, n),
            'Div_Yield': np.random.normal(2, 0.2, n),
            'Daily_Div': np.random.normal(5, 0.5, n) / 365
        }
        return pd.DataFrame(mock_data)
    
    # Pull historical daily data
    fields = ["PX_LAST", "IDX_EST_DVD_YLD", "INDX_GROSS_DAILY_DIV"]
    
    try:
        spot = blp.bdh(ticker, "PX_LAST", start_date=start_date, end_date=end_date)
        dvd_yield = blp.bdh(ticker, "IDX_EST_DVD_YLD", start_date=start_date, end_date=end_date)
        daily_div = blp.bdh(ticker, "INDX_GROSS_DAILY_DIV", start_date=start_date, end_date=end_date)
        
        # Combine the DataFrames
        df = pd.concat([spot, dvd_yield, daily_div], axis=1)
        df.reset_index(inplace=True)
        df.rename(columns={
            "index": "Date",
            f"{ticker} PX_LAST": "Spot_Price", 
            f"{ticker} IDX_EST_DVD_YLD": "Div_Yield",
            f"{ticker} INDX_GROSS_DAILY_DIV": "Daily_Div"
        }, inplace=True)
        
        return df
    except Exception as e:
        logger.error(f"Error pulling data for {ticker}: {e}")
        raise

def pull_futures_data(tickers, start_date, end_date):
    """
    Pull futures data for an index from Bloomberg.
    
    Args:
        tickers (list): List of Bloomberg tickers for futures contracts
        start_date (str): Start date in 'YYYY-MM-DD' format
        end_date (str): End date in 'YYYY-MM-DD' format
        
    Returns:
        pandas.DataFrame: DataFrame with futures prices, volumes, and open interest
    """
    if not USING_XBBG:
        # Create mock futures data
        logger.warning("Using mock futures data as xbbg is not available")
        dates = pd.date_range(start=start_date, end=end_date, freq='B')
        n = len(dates)
        
        mock_data = {'Date': dates}
        for i, ticker in enumerate(tickers, 1):
            base = 2000 - (i-1) * 20  # Each further-out contract trades at a slight discount
            mock_data[f'F_P{i}'] = np.random.normal(base, 50, n)
            mock_data[f'Vol{i}'] = np.random.randint(1000, 10000, n)
            mock_data[f'OI{i}'] = np.random.randint(10000, 50000, n)
            mock_data[f'Contract{i}'] = [
                ["MAR23", "JUN23", "SEP23", "DEC23"][j % 4] for j in range(n)
            ]
        
        return pd.DataFrame(mock_data)
    
    # Actual Bloomberg data pull
    fut_data = {}
    contract_info = {}
    
    try:
        for i, ticker in enumerate(tickers, 1):
            # Get price data
            fut_df = blp.bdh(ticker, "PX_LAST", start_date=start_date, end_date=end_date)
            fut_df.rename(columns={f"{ticker} PX_LAST": f"F_P{i}"}, inplace=True)
            
            # Get volume data
            vol_df = blp.bdh(ticker, "PX_VOLUME", start_date=start_date, end_date=end_date)
            vol_df.rename(columns={f"{ticker} PX_VOLUME": f"Vol{i}"}, inplace=True)
            
            # Get open interest data
            oi_df = blp.bdh(ticker, "OPEN_INT", start_date=start_date, end_date=end_date)
            oi_df.rename(columns={f"{ticker} OPEN_INT": f"OI{i}"}, inplace=True)
            
            # Get contract month/year information
            contract_df = blp.bdh(ticker, "CURRENT_CONTRACT_MONTH_YR", start_date=start_date, end_date=end_date)
            contract_df.rename(columns={f"{ticker} CURRENT_CONTRACT_MONTH_YR": f"Contract{i}"}, inplace=True)
            
            fut_data[f"F_P{i}"] = fut_df
            fut_data[f"Vol{i}"] = vol_df
            fut_data[f"OI{i}"] = oi_df
            contract_info[f"Contract{i}"] = contract_df
    
        # Merge futures data on date
        futures_df = pd.concat([
            pd.concat([fut_data[f"F_P{i}"], fut_data[f"Vol{i}"], fut_data[f"OI{i}"], contract_info[f"Contract{i}"]], axis=1)
            for i in range(1, len(tickers) + 1)
        ], axis=1)
        
        futures_df.reset_index(inplace=True)
        futures_df.rename(columns={"index": "Date"}, inplace=True)
        
        return futures_df
    
    except Exception as e:
        logger.error(f"Error pulling futures data: {e}")
        raise

def pull_ois_rates(start_date, end_date):
    """
    Pull USD OIS rates from Bloomberg.
    
    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format
        end_date (str): End date in 'YYYY-MM-DD' format
        
    Returns:
        pandas.DataFrame: DataFrame with USD OIS rates
    """
    if not USING_XBBG:
        # Create mock OIS rates data
        logger.warning("Using mock OIS data as xbbg is not available")
        dates = pd.date_range(start=start_date, end=end_date, freq='B')
        n = len(dates)
        
        mock_data = {'Date': dates}
        base_rate = 3.0  # Base rate in percentage
        for rate_name in OIS_TICKERS.keys():
            # Each tenor has slightly higher rate
            tenor_modifier = {"OIS_1W": 0, "OIS_1M": 0.1, "OIS_3M": 0.2, 
                             "OIS_6M": 0.3, "OIS_1Y": 0.5}
            rate = base_rate + tenor_modifier[rate_name]
            mock_data[rate_name] = np.random.normal(rate, 0.1, n)
        
        return pd.DataFrame(mock_data)
    
    # Actual Bloomberg data pull
    ois_data = {}
    
    try:
        for label, ois_ticker in OIS_TICKERS.items():
            ois_df = blp.bdh(ois_ticker, "PX_LAST", start_date=start_date, end_date=end_date)
            ois_df.rename(columns={f"{ois_ticker} PX_LAST": label}, inplace=True)
            ois_data[label] = ois_df
        
        # Merge OIS data on date
        ois_df = pd.concat(list(ois_data.values()), axis=1)
        ois_df.reset_index(inplace=True)
        ois_df.rename(columns={"index": "Date"}, inplace=True)
        
        return ois_df
    
    except Exception as e:
        logger.error(f"Error pulling OIS rates: {e}")
        raise

def main():
    """Main function to download and save all required data."""
    try:
        # Define date range
        start_date = "2003-01-01"  # Starting from 2003 as per original code
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        logger.info(f"Pulling data from {start_date} to {end_date}")
        
        # Pull and save spot and dividend data for each index
        for index_name, config in INDEX_CONFIG.items():
            logger.info(f"Processing {index_name} spot/dividend data")
            df = pull_spot_div_data(config["spot_ticker"], start_date, end_date)
            df.to_csv(f"input/{index_name}_spot_div.csv", index=False)
            logger.info(f"Saved {index_name} spot/dividend data")
        
        # Pull and save futures data for each index
        for index_name, config in INDEX_CONFIG.items():
            logger.info(f"Processing {index_name} futures data")
            fut_df = pull_futures_data(config["futures_tickers"], start_date, end_date)
            fut_df.to_csv(f"input/{index_name}_Futures.csv", index=False)
            logger.info(f"Saved {index_name} futures data")
        
        # Pull and save OIS rates
        logger.info("Processing USD OIS rates")
        ois_df = pull_ois_rates(start_date, end_date)
        ois_df.to_csv("input/USD_OIS_Rates.csv", index=False)
        logger.info("Saved USD OIS rates data")
        
        logger.info("All data extraction completed successfully!")
    
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()