#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

log_file = Path(TEMP_DIR) / f'ois_processing.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Required column mapping
OIS_TENORS = {
    "OIS_3M": "USSOC CMPN Curncy",   # 3 Month OIS Rate
}

def process_ois_data(filepath: Path) -> pd.DataFrame:
    """
    Extracts, cleans, and formats only the 3-month OIS rate from Bloomberg historical dataset.

    Args:
        filepath (Path): Path to the parquet file containing multi-index Bloomberg data.

    Returns:
        pd.DataFrame: Cleaned OIS dataset containing only the 3-month OIS rate.

    Raises:
        ValueError: If the required OIS column is missing.
    """
    logger.info(f"Loading OIS data from {filepath}")

    try:
        ois_df = pd.read_parquet(filepath)
    except Exception as e:
        logger.error(f"Error reading parquet file: {e}")
        raise

    logger.info(f"Column levels: {ois_df.columns.names}")

    # Ensure required OIS column is present
    required_col = OIS_TENORS["OIS_3M"]
    if (required_col, "PX_LAST") not in ois_df.columns:
        raise ValueError(f"Missing required OIS column: {required_col}")

    # Select only the required 3-month OIS rate column
    ois_df = ois_df.loc[:, [(required_col, "PX_LAST")]]
    ois_df.columns = ["OIS_3M"]  # Rename to a clean column name

    # Convert OIS rates from percentage to decimal format (if applicable)
    logger.info("Converting OIS_3M from percentage to decimal format")
    ois_df["OIS_3M"] = ois_df["OIS_3M"] / 100

    # Drop rows with missing values
    ois_df = ois_df.dropna(subset=["OIS_3M"])

    # Save the cleaned dataset
    output_path = Path(PROCESSED_DIR) / "cleaned_ois_rates.csv"
    ois_df.to_csv(output_path, index=True)
    logger.info(f"Saved cleaned OIS rates to {output_path}")

    # Log dataset summary
    logger.info("\n========== OIS Data Summary ==========")
    logger.info(f"Shape of dataset: {ois_df.shape} (rows, columns)")
    logger.info(f"Missing values per column:\n{ois_df.isna().sum().to_string()}")
    logger.info("Descriptive statistics:\n%s", ois_df.describe().to_string())
    logger.info("First 5 rows of cleaned OIS data:\n%s", ois_df.head().to_string())

    return ois_df

def main():
    """
    Main function to process OIS rates.
    Loads Bloomberg historical data, extracts only the 3-month OIS rate,
    cleans and formats it, and saves it for further use.
    """
    INPUT_FILE = Path(INPUT_DIR) / "bloomberg_historical_data.parquet"

    if not os.path.exists(INPUT_FILE):
        logger.warning("Primary input file not found, switching to cached data")
        INPUT_FILE = Path(DATA_MANUAL) / "bloomberg_historical_data.parquet"

    try:
        process_ois_data(INPUT_FILE)
        logger.info("OIS data processing completed successfully!")
    except Exception as e:
        logger.error(f"Error processing OIS data: {e}")
        raise

if __name__ == "__main__":
    main()
