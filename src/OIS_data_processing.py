"""
OIS Data Extraction and Preprocessing Script
--------------------------------------------
This script processes Bloomberg OIS rate data from a multi-index DataFrame,
preparing it for merging with futures data in the next step.

What this script does:
1. **Extracts and formats OIS rates** from a Bloomberg multi-index dataset.
2. **Ensures required OIS columns are present** and renames them for consistency.
3. **Converts OIS rates from percentage to decimal format** (e.g., 4.5% → 0.045).
4. **Saves the cleaned dataset**, ensuring all necessary tenors are present.

What this script **does NOT** do:
    - It does **not** interpolate missing values (this will be handled in the merging step with futures data).
    - It does **not** compute time-to-maturity (TTM) (this depends on futures contract maturities).

The final cleaned OIS dataset will be used to:
- Interpolate missing rates based on TTM in the next script.
- Merge with futures data for spread calculations.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging
import sys
import os
from pathlib import Path
sys.path.insert(1, "./src")
from settings import config

# Load configuration paths
DATA_DIR = config("DATA_DIR")
TEMP_DIR = config("TEMP_DIR")
INPUT_DIR = config("INPUT_DIR")
PROCESSED_DIR = config("PROCESSED_DIR")
DATA_MANUAL = config("MANUAL_DATA_DIR")


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = Path(TEMP_DIR) / f'ois_processing_{timestamp}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


OIS_TENORS = {
    "OIS_1W": "USSO1Z CMPN Curncy",  # 1 Week OIS Rate
    "OIS_1M": "USSO1 CMPN Curncy",   # 1 Month OIS Rate
    "OIS_3M": "USSO3 CMPN Curncy",   # 3 Month OIS Rate
    "OIS_6M": "USSOF CMPN Curncy",   # 6 Month OIS Rate
    "OIS_1Y": "USSO10 CMPN Curncy"   # 1 Year OIS Rate
}

def process_ois_data(filepath: Path) -> pd.DataFrame:
    """
    Extracts, cleans, and formats OIS rate data from Bloomberg historical dataset.

    Args:
        filepath (Path): Path to the parquet file containing multi-index Bloomberg data.

    Returns:
        pd.DataFrame: Cleaned OIS dataset with required tenors formatted correctly.

    Raises:
        ValueError: If required OIS columns are missing.
    """
    logger.info(f"Loading OIS data from {filepath}")

    try:
        ois_df = pd.read_parquet(filepath)
    except Exception as e:
        logger.error(f"Error reading parquet file: {e}")
        raise
    

    logger.info(f"Column levels: {ois_df.columns.names}")
    logger.info(f"Level 0 sample: {ois_df.columns.get_level_values(0)[:5].tolist()}")

    # Ensure all required OIS columns are present
    missing_cols = [col for col in OIS_TENORS.values() if col not in ois_df.columns.get_level_values(0)]
    if missing_cols:
        raise ValueError(f"Missing required OIS columns: {missing_cols}")

    # Sort MultiIndex for structured slicing
    ois_df = ois_df.sort_index(axis=1)

    # Select only relevant OIS columns (matching the PX_LAST field)
    try:
        ois_df = ois_df.loc[:, (slice("USSO1Z CMPN Curncy", "USSO30 CMPN Curncy"), 'PX_LAST')]
    except KeyError as e:
        logger.error(f"Error selecting OIS columns: {e}")
        raise

    # Flatten MultiIndex column names
    ois_df.columns = ois_df.columns.get_level_values(0)
    rename_dict = {v: k for k, v in OIS_TENORS.items()}
    ois_df = ois_df.rename(columns=rename_dict)

    # Convert OIS rates to decimal format (if they are in percentage format)
    for col in OIS_TENORS.keys():
        if col in ois_df.columns:
            max_value = ois_df[col].max()
            if max_value > 10:  # Check if the values are in percentage (e.g., 4.5 instead of 0.045)
                logger.info(f"Converting {col} from percentage to decimal format")
                ois_df[col] = ois_df[col] / 100


    output_path = Path(PROCESSED_DIR) / "cleaned_ois_rates.csv"
    ois_df.to_csv(output_path, index=True)
    logger.info(f"Saved cleaned OIS rates to {output_path}")

    return ois_df


def main():
    """
    Main function to process OIS rates.
    
    It loads the Bloomberg historical data, extracts OIS rates,
    cleans and formats them, and saves them for use in further analysis.
    """
    INPUT_FILE = Path(INPUT_DIR) / "bloomberg_historical_data.parquet"
    
    if not os.path.exists(INPUT_FILE):
        logger.warning("Primary input file not found, switching to cached data")
        INPUT_FILE = Path(DATA_MANUAL) / "bloomberg_historical_data.parquet"

    try:
        processed_data = process_ois_data(INPUT_FILE)
        logger.info("OIS data processing completed successfully!")
    except Exception as e:
        logger.error(f"Error processing OIS data: {e}")
        raise

if __name__ == "__main__":
    main()
