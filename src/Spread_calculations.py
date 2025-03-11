#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Implied Forward Rate Computation (Dividends + Single OIS + Compounding)
-----------------------------------------------------------------------
Based on the logic from your Stata scripts:
  - We compute 'Div_Sum1_Comp' and 'Div_Sum2_Comp' using half-interval compounding
    with a single 3-month OIS rate (no interpolation).
  - Then 'implied_forward_raw' => 'cal_{index_code}_rf'.
  - Also compute 'ois_fwd_raw' => 'ois_fwd_{index_code}'.
  - Finally 'spread_{index_code} = cal_{index_code}_rf - ois_fwd_{index_code}'.
  - Apply Barndorff-Nielsen outlier filter on the spread and save results.
  - Plot all indices together in one figure, with an option to keep date axis unbroken by missing data.

This script presumes:
  - Each index has a "{index_code}_Calendar_spread.csv" with 2-term data 
    (Term1, Term2) in PROCESSED_DIR (including TTM, SettlementDate, etc.).
  - "cleaned_ois_rates.csv" in PROCESSED_DIR has columns [Date, OIS_3M]
    (with OIS_3M in DECIMAL form, e.g. 0.013 => 1.3%).
  - "bloomberg_historical_data.parquet" with daily dividends for each index.
"""

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from datetime import datetime
from pathlib import Path
import logging
import sys


sys.path.insert(1, "./src")
from settings import config

DATA_DIR = config("DATA_DIR")
TEMP_DIR = config("TEMP_DIR")
INPUT_DIR = config("INPUT_DIR")
PROCESSED_DIR = config("PROCESSED_DIR")
DATA_MANUAL = config("MANUAL_DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")

Path(OUTPUT_DIR).mkdir(exist_ok=True, parents=True)

log_file = Path(TEMP_DIR) / f"forward_rate_calculation.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

INDEX_CODES = ["SPX", "NDX", "INDU"]


def build_daily_dividends(index_code: str) -> pd.DataFrame:
    """
    Load daily dividends for the given index code from bloomberg_historical_data.parquet.
    Return columns: [Date, Daily_Div].
    """
    logger.info(f"[{index_code}] Building daily dividend table")

    input_file = Path(INPUT_DIR) / "bloomberg_historical_data.parquet"
    if not os.path.exists(input_file):
        logger.warning("Primary input file not found, switching to cached data")
        input_file = Path(DATA_MANUAL) / "bloomberg_historical_data.parquet"

    raw_df = pd.read_parquet(input_file)
    div_col = (f"{index_code} Index", "INDX_GROSS_DAILY_DIV")
    if div_col not in raw_df.columns:
        raise ValueError(f"Missing daily dividend column {div_col} for index={index_code}")

    div_df = raw_df.loc[:, div_col].to_frame("Daily_Div").reset_index()
    div_df.rename(columns={"index": "Date"}, inplace=True)
    div_df["Date"] = pd.to_datetime(div_df["Date"], errors="coerce")
    div_df["Daily_Div"] = div_df["Daily_Div"].fillna(0)

    # Optionally drop any row that has no valid date
    before_drop = len(div_df)
    div_df.dropna(subset=["Date"], inplace=True)
    after_drop = len(div_df)
    if after_drop < before_drop:
        logger.info(
            f"[{index_code}] Dropped {before_drop - after_drop} rows with invalid or missing date in daily_div."
        )

    div_df.sort_values("Date", inplace=True)
    div_df.reset_index(drop=True, inplace=True)

    logger.info(f"[{index_code}] daily dividends final shape: {div_df.shape}")
    logger.info(f"[{index_code}] Sample daily dividends:\n{div_df.head(10)}")
    return div_df


def barndorff_nielsen_filter(df: pd.DataFrame,
                             colname: str,
                             date_col: str = "Date",
                             window: int = 45,
                             threshold: float = 10.0) -> pd.DataFrame:
    """
    Barndorff-Nielsen outlier filter on 'colname' over ±window days.
    1) rolling median => ...
    2) abs_dev from that median
    3) rolling mean(abs_dev) => mad
    4) outlier if abs_dev/mad >= threshold => set colname_filtered=NaN
    """
    df = df.sort_values(date_col).copy()

    rolling_median = df[colname].rolling(window=window*2+1, center=True, min_periods=1).median()
    rolling_median_shifted = rolling_median.shift(1)

    df["abs_dev"] = (df[colname] - rolling_median_shifted).abs()
    rolling_mad = df["abs_dev"].rolling(window=window*2+1, center=True, min_periods=1).mean()
    rolling_mad_shifted = rolling_mad.shift(1)

    df["bad_price"] = (df["abs_dev"] / rolling_mad_shifted) >= threshold
    df.loc[df[colname].isna(), "bad_price"] = False

    # Count how many outliers
    outlier_count = df["bad_price"].sum()
    if outlier_count > 0:
        logger.info(f"Barndorff-Nielsen filter: flagged {int(outlier_count)} outliers in {colname}")

    df[f"{colname}_filtered"] = df[colname].where(~df["bad_price"], np.nan)

    df.drop(["abs_dev", "bad_price"], axis=1, inplace=True, errors="ignore")
    return df


def process_index_forward_rates(index_code: str) -> pd.DataFrame:
    """
    1) Load near/next futures for index_code from _Calendar_spread.csv
    2) Merge with single OIS_3M (as-of)
    3) Merge daily dividends, compute Div_Sum1_Comp & Div_Sum2_Comp
    4) Implied forward => cal_{index_code}_rf, OIS forward => ois_fwd_{index_code}, spread
    5) Barndorff outlier filter, then multiply spread by 100 => bps
    6) Save & return
    """
    logger.info(f"[{index_code}] Starting forward rate computation")

    fut_file = Path(PROCESSED_DIR) / f"{index_code}_Calendar_spread.csv"
    if not fut_file.exists():
        logger.error(f"[{index_code}] Missing futures file: {fut_file}")
        return pd.DataFrame()

    fut_df = pd.read_csv(fut_file)
    logger.info(f"[{index_code}] Loaded futures shape: {fut_df.shape}")


    if "Date" not in fut_df.columns:
        logger.error(f"[{index_code}] No 'Date' column in {fut_file}, aborting.")
        return pd.DataFrame()
    fut_df["Date"] = pd.to_datetime(fut_df["Date"], errors="coerce")

    before_drop = len(fut_df)
    fut_df.dropna(subset=["Date"], inplace=True)
    logger.info(f"[{index_code}] Dropped {before_drop - len(fut_df)} rows lacking a valid Date in futures.")
    fut_df["Term1_SettlementDate"] = pd.to_datetime(fut_df["Term1_SettlementDate"], errors="coerce")
    fut_df["Term2_SettlementDate"] = pd.to_datetime(fut_df["Term2_SettlementDate"], errors="coerce")

    fut_df.sort_values("Date", inplace=True)
    fut_df.reset_index(drop=True, inplace=True)

    # === Merge single OIS_3M
    ois_file = Path(PROCESSED_DIR) / "cleaned_ois_rates.csv"
    if not ois_file.exists():
        logger.error(f"[{index_code}] Missing OIS file: {ois_file}")
        return pd.DataFrame()

    ois_df = pd.read_csv(ois_file)
    if "Date" not in ois_df.columns:
        ois_df.rename(columns={"Unnamed: 0": "Date"}, inplace=True)
    ois_df["Date"] = pd.to_datetime(ois_df["Date"], errors="coerce")
    ois_df.sort_values("Date", inplace=True)

    # as-of merge
    prev_len = len(fut_df)
    merged_df = pd.merge_asof(
        fut_df, ois_df, on="Date", direction="backward"
    )
    after_len = len(merged_df)
    logger.info(f"[{index_code}] as-of merged OIS: from {prev_len} -> {after_len} rows (should be same).")

    # rename OIS_3M => 'OIS'
    if "OIS_3M" in merged_df.columns:
        merged_df.rename(columns={"OIS_3M": "OIS"}, inplace=True)
    else:
        logger.warning(f"[{index_code}] 'OIS_3M' column not found in OIS data, using default 'OIS_3M'?")

    # === Load daily dividends
    div_df = build_daily_dividends(index_code)
    # add cumsum in div_df
    div_df["CumDiv"] = div_df["Daily_Div"].cumsum()

    # merge cumsum at current date
    prev_len = len(merged_df)
    merged_df = pd.merge_asof(
        merged_df.sort_values("Date"),
        div_df[["Date", "CumDiv"]].sort_values("Date"),
        on="Date",
        direction="backward"
    )
    after_len = len(merged_df)
    logger.info(
        f"[{index_code}] as-of merged CumDiv at current date: from {prev_len} -> {after_len} rows."
    )
    merged_df.rename(columns={"CumDiv": "CumDiv_current"}, inplace=True)
    logger.info(f"[{index_code}] Sample merged rows with cumulative div:\n{merged_df.head(10)}")
    # same approach for Term1 & Term2
    t1_df = div_df.rename(columns={"Date": "Term1_SettlementDate", "CumDiv": "CumDiv_Term1"})
    prev_len = len(merged_df)
    merged_df = pd.merge_asof(
        merged_df.sort_values("Term1_SettlementDate"),
        t1_df.sort_values("Term1_SettlementDate"),
        on="Term1_SettlementDate",
        direction="backward"
    )
    after_len = len(merged_df)
    logger.info(
        f"[{index_code}] as-of merged CumDiv for Term1: from {prev_len} -> {after_len} rows."
    )

    t2_df = div_df.rename(columns={"Date": "Term2_SettlementDate", "CumDiv": "CumDiv_Term2"})
    prev_len = len(merged_df)
    merged_df = pd.merge_asof(
        merged_df.sort_values("Term2_SettlementDate"),
        t2_df.sort_values("Term2_SettlementDate"),
        on="Term2_SettlementDate",
        direction="backward"
    )
    after_len = len(merged_df)
    logger.info(
        f"[{index_code}] as-of merged CumDiv for Term2: from {prev_len} -> {after_len} rows."
    )

    # compute Div_Sum1 & Div_Sum2
    merged_df["Div_Sum1"] = merged_df["CumDiv_Term1"] - merged_df["CumDiv_current"]
    merged_df["Div_Sum2"] = merged_df["CumDiv_Term2"] - merged_df["CumDiv_current"]

    # Handle missing TTM or price
    # If TTM is missing, can't compute rates => drop
    before_drop = len(merged_df)
    merged_df.dropna(subset=["Term1_TTM", "Term2_TTM", "Term1_Futures_Price", "Term2_Futures_Price"], inplace=True)
    logger.info(
        f"[{index_code}] Dropped {before_drop - len(merged_df)} rows missing TTM or Futures_Price."
    )

    # 4) Compounding
    ttm1 = "Term1_TTM"
    ttm2 = "Term2_TTM"
    merged_df["Div_Sum1_Comp"] = merged_df["Div_Sum1"] * (
        ((merged_df[ttm1] / 2.0) / 360.0) * merged_df["OIS"] + 1.0
    )
    merged_df["Div_Sum2_Comp"] = merged_df["Div_Sum2"] * (
        ((merged_df[ttm2] / 2.0) / 360.0) * merged_df["OIS"] + 1.0
    )

    # Implied Forward
    fp1 = "Term1_Futures_Price"
    fp2 = "Term2_Futures_Price"
    merged_df["implied_forward_raw"] = (
        (merged_df[fp2] + merged_df["Div_Sum2_Comp"]) /
        (merged_df[fp1] + merged_df["Div_Sum1_Comp"])
        - 1.0
    )

    dt = merged_df[ttm2] - merged_df[ttm1]
    merged_df[f"cal_{index_code}_rf"] = np.where(
        dt > 0,
        100.0 * merged_df["implied_forward_raw"] * (360.0 / dt),
        np.nan
    )

    # OIS-implied forward
    merged_df["ois_fwd_raw"] = (
        (1.0 + merged_df["OIS"] * merged_df[ttm2] / 360.0) /
        (1.0 + merged_df["OIS"] * merged_df[ttm1] / 360.0)
        - 1.0
    )
    merged_df[f"ois_fwd_{index_code}"] = np.where(
        dt > 0,
        merged_df["ois_fwd_raw"] * (360.0 / dt) * 100.0,
        np.nan
    )

    # Spread
    spread_col = f"spread_{index_code}"
    merged_df[spread_col] = merged_df[f"cal_{index_code}_rf"] - merged_df[f"ois_fwd_{index_code}"]
    # 8) BN outlier filter
    merged_df = barndorff_nielsen_filter(merged_df, spread_col, date_col="Date", window=45, threshold=10)
    # If outlier => set cal_rf & spread to NaN
    out_mask = merged_df[f"{spread_col}_filtered"].isna()
    outliers_count = out_mask.sum()
    if outliers_count > 0:
        logger.info(f"[{index_code}] Setting {outliers_count} outliers to NaN for cal_{index_code}_rf & {spread_col}")
    merged_df.loc[out_mask, f"cal_{index_code}_rf"] = np.nan
    merged_df.loc[out_mask, spread_col] = np.nan

    # Multiply spread by 100 => bps

    merged_df[spread_col] = merged_df[spread_col] * 100.0
    merged_df.set_index("Date", inplace=True)
    out_file = Path(PROCESSED_DIR) / f"{index_code}_Forward_Rates.csv"
    merged_df.to_csv(out_file)
    logger.info(f"[{index_code}] Final forward rates shape: {merged_df.shape}, saved to {out_file}")
    logger.info(
        f"[{index_code}] Sample final rows:\n"
        + merged_df[[f"cal_{index_code}_rf", f"ois_fwd_{index_code}", spread_col]].tail(5).to_string()
    )

    return merged_df


def plot_all_indices(results: dict, keep_dates: bool = True):
    """
    Generate two plots:
    1) From START_DATE to 01/01/2020
    2) From START_DATE to END_DATE

    If keep_dates=True, reindex all series to the union of dates in results.
    """
    START_DATE = pd.to_datetime(config("START_DATE"))
    END_DATE = pd.to_datetime(config("END_DATE"))
    MID_DATE = pd.to_datetime("2020-01-01")

    def _plot(date_range, filename_suffix):
        plt.figure(figsize=(12, 7))

        date_index = None
        if keep_dates:
            all_dates = set()
            for df in results.values():
                if df is not None and not df.empty:
                    all_dates.update(df.index)
            date_index = pd.to_datetime(sorted(all_dates))

        colors = {"SPX": "blue", "NDX": "green", "INDU": "red"}

        for idx, df in results.items():
            if df is not None and not df.empty:
                spread_col = f"spread_{idx}"
                df_plot = df.reindex(date_index).ffill() if keep_dates and date_index is not None else df
                df_plot = df_plot.loc[(df_plot.index >= START_DATE) & (df_plot.index <= date_range)]
                
                plt.plot(df_plot.index, df_plot[spread_col], color=colors.get(idx, "black"), alpha=0.8, label=f"{idx} Spread (bps)")

        plt.axhline(0, color="k", linestyle="--", alpha=0.7)
        plt.title(f"Implied Forward Spread Across Indices (bps)\n[{START_DATE.date()} to {date_range.date()}]")
        plt.xlabel("Date")
        plt.ylabel("Spread (bps)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        ax = plt.gca()
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

        out_png = Path(OUTPUT_DIR) / f"all_indices_spread_{filename_suffix}.png"
        plt.savefig(out_png, dpi=300)
        logger.info(f"Saved plot to {out_png}")
        plt.close()

    # Generate two plots with different date ranges
    _plot(MID_DATE, "to_2020")
    _plot(END_DATE, "to_present")




def main():
    logger.info("== Starting forward rate calculations with compounding dividends, single OIS, BN outlier filter ==")
    results = {}
    for idx in INDEX_CODES:
        df_res = process_index_forward_rates(idx)
        results[idx] = df_res
    plot_all_indices(results, keep_dates=True)

    logger.info("All computations completed successfully.")


if __name__ == "__main__":
    main()
