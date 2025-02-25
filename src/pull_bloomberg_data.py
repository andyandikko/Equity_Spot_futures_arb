import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from xbbg import blp

# -------------------------------
# Step 1: Pull Spot/Dividend Data using xbbg
# -------------------------------
# Define tickers and fields for SPX (repeat for Nasdaq and DowJones as needed)
tickers = ["SPX Index"]
fields = ["PX_LAST", "INDX_EST_DVD_YLD", "INDX_GROSS_DAILY_DIV"]

spot_div = blp.bdp(tickers, fields)
# Convert result to DataFrame and add a date (for historical data you might use BDH)
spot_div = pd.DataFrame(spot_div).T.reset_index().rename(columns={'index': 'Ticker'})
# For a daily historical series, use BDH; e.g.,
spot_div_hist = blp.bdh("SPX Index", ["PX_LAST", "INDX_EST_DVD_YLD", "INDX_GROSS_DAILY_DIV"],
                         start_date="2023-01-01", end_date="2023-12-31")
spot_div_hist.reset_index(inplace=True)
spot_div_hist.rename(columns={'index': 'Date', 'PX_LAST': 'Spot_Price', 
                              'INDX_EST_DVD_YLD': 'Div_Yield', 
                              'INDX_GROSS_DAILY_DIV': 'Daily_Div'}, inplace=True)

# -------------------------------
# Step 2: Pull Futures Prices using xbbg
# -------------------------------
# For the nearest four SP futures contracts (e.g., ES1, ES2, ES3, ES4)
futures_tickers = ["ES1 Index", "ES2 Index", "ES3 Index", "ES4 Index"]
futures_hist = blp.bdh(futures_tickers, "PX_LAST", start_date="2023-01-01", end_date="2023-12-31")
futures_hist = futures_hist.stack().reset_index()
futures_hist.rename(columns={'level_1': 'Contract', 'PX_LAST': 'Futures_Price'}, inplace=True)

# -------------------------------
# Step 3: Process Contract Maturity Dates
# (For simplicity, assume that each contract's name encodes its expiry; here we mimic the Stata logic.)
month_map = {"ES1": "MAR", "ES2": "JUN", "ES3": "SEP", "ES4": "DEC"}
# For example, extract month from contract ticker:
futures_hist["MonthAbbr"] = futures_hist["Contract"].apply(lambda x: month_map.get(x, np.nan))
# Assume the contract has a two-digit year; for illustration, we set it as 23.
futures_hist["Year"] = 2023
# Compute a tentative maturity date as the first day of the month, then adjust to third Friday:
futures_hist["MatTentative"] = pd.to_datetime(dict(year=futures_hist["Year"],
                                                   month=pd.to_numeric(futures_hist["MonthAbbr"].map(
                                                       {"MAR": 3, "JUN": 6, "SEP": 9, "DEC": 12})),
                                                   day=1))
def third_friday(dt):
    # Find the third Friday of dt's month
    d = dt
    fridays = [day for day in range(15, 22) if datetime(dt.year, dt.month, day).weekday() == 4]
    return datetime(dt.year, dt.month, fridays[0]) if fridays else d
futures_hist["Mat"] = futures_hist["MatTentative"].apply(third_friday)

# -------------------------------
# Step 4: Merge Spot and Futures Data
# -------------------------------
# Merge on Date: for simplicity, merge using nearest date matching.
spot_div_hist['Date'] = pd.to_datetime(spot_div_hist['Date'])
futures_hist['Date'] = pd.to_datetime(futures_hist['Date'])
data_merged = pd.merge_asof(futures_hist.sort_values("Date"),
                            spot_div_hist.sort_values("Date"),
                            on="Date", direction="backward")

# -------------------------------
# Step 5: Pull USD OIS Rates using xbbg
# -------------------------------
ois_tickers = ["USSW1 Curncy", "USSW1M Curncy", "USSW3M Curncy", "USSW6M Curncy", "USSW1Y Curncy"]
ois_hist = blp.bdh(ois_tickers, "PX_LAST", start_date="2023-01-01", end_date="2023-12-31")
ois_hist = ois_hist.stack().reset_index()
ois_hist.rename(columns={'level_1': 'OIS_Tenor', 'PX_LAST': 'OIS_Rate'}, inplace=True)
# Pivot to have one row per date with columns for each tenor:
ois_wide = ois_hist.pivot(index="index", columns="OIS_Tenor", values="OIS_Rate").reset_index()
ois_wide.rename(columns={'index': 'Date'}, inplace=True)
ois_wide['Date'] = pd.to_datetime(ois_wide['Date'])
# Merge OIS data into our merged dataset on Date
data_merged = pd.merge_asof(data_merged.sort_values("Date"),
                            ois_wide.sort_values("Date"),
                            on="Date", direction="backward")

# -------------------------------
# Step 6: Compute Time-to-Maturity (TTM) and Interpolate OIS Rate
# -------------------------------
data_merged["TTM"] = (data_merged["Mat"] - data_merged["Date"]).dt.days

def interp_OIS(ttm, ois_1w, ois_1m, ois_3m, ois_6m, ois_1y):
    if ttm <= 7:
        return ois_1w
    elif 7 < ttm <= 30:
        return ((30 - ttm) / 23) * ois_1w + ((ttm - 7) / 23) * ois_1m
    elif 30 < ttm <= 90:
        return ((90 - ttm) / 60) * ois_1m + ((ttm - 30) / 60) * ois_3m
    elif 90 < ttm <= 180:
        return ((180 - ttm) / 90) * ois_3m + ((ttm - 90) / 90) * ois_6m
    else:
        return ((360 - ttm) / 180) * ois_6m + ((ttm - 180) / 180) * ois_1y

data_merged["OIS_interp"] = data_merged.apply(lambda row: interp_OIS(row["TTM"],
                                                      row["USSW1 Curncy"],
                                                      row["USSW1M Curncy"],
                                                      row["USSW3M Curncy"],
                                                      row["USSW6M Curncy"],
                                                      row["USSW1Y Curncy"]), axis=1)
# Convert to decimal:
data_merged["OIS_interp"] = data_merged["OIS_interp"] / 100

# -------------------------------
# Step 7: Compute Implied Forward Rates
# (This step follows the second Stata script.)
# We assume that the two most liquid futures contracts are available as Term 1 and Term 2.
# For illustration, we simulate that by filtering on Contract being "ES1 Index" (Term1) and "ES2 Index" (Term2).
# In your merged data, ensure you have variables F_P, Div_Sum, TTM, OIS_interp, Spot_Price, etc.
# Here we compute:
#   - Compound dividends at OIS rate (assuming uniform payment)
#   - Implied forward raw, then annualize it.
# -------------------------------
# For simplicity, we need to reshape the data so that Term 1 and Term 2 are side by side.
# Assume our merged data has a column "Contract" that indicates which term.
term1 = data_merged[data_merged["Contract"] == "ES1 Index"].copy()
term2 = data_merged[data_merged["Contract"] == "ES2 Index"].copy()
# Merge term1 and term2 on date:
merged_terms = pd.merge(term1, term2, on="Date", suffixes=("1", "2"))
# Calculate compounded dividends for each term:
merged_terms["Div_Sum1_Comp"] = merged_terms["Div_Sum1"] * (((merged_terms["TTM1"] / 2) / 360) * merged_terms["OIS_interp1"] + 1)
merged_terms["Div_Sum2_Comp"] = merged_terms["Div_Sum2"] * (((merged_terms["TTM2"] / 2) / 360) * merged_terms["OIS_interp2"] + 1)
# Compute raw implied forward:
merged_terms["implied_forward_raw"] = (merged_terms["F_P2"] + merged_terms["Div_Sum2_Comp"]) / (merged_terms["F_P1"] + merged_terms["Div_Sum1_Comp"]) - 1
merged_terms["cal_spx_rf"] = 100 * merged_terms["implied_forward_raw"] * (360 / (merged_terms["TTM2"] - merged_terms["TTM1"]))

# Compute OIS-implied forward rates:
merged_terms["ois_fwd_raw"] = (1 + merged_terms["OIS_interp2"] * merged_terms["TTM2"] / 360) / (1 + merged_terms["OIS_interp1"] * merged_terms["TTM1"] / 360) - 1
merged_terms["ois_fwd_spx"] = 100 * merged_terms["ois_fwd_raw"] * 360 / (merged_terms["TTM2"] - merged_terms["TTM1"])

# -------------------------------
# Step 8: Outlier Cleanup
# Calculate the arbitrage spread and remove outliers using a 45-day rolling window.
merged_terms = merged_terms.sort_values("Date")
merged_terms["arb_spx"] = merged_terms["cal_spx_rf"] - merged_terms["ois_fwd_spx"]
merged_terms["arb_spx_med"] = merged_terms["arb_spx"].rolling(window=45, center=True).median()
merged_terms["abs_dev"] = (merged_terms["arb_spx"] - merged_terms["arb_spx_med"]).abs()
merged_terms["mad"] = merged_terms["abs_dev"].rolling(window=45, center=True).mean()
merged_terms["bad_price"] = np.where((merged_terms["mad"] > 0) & (merged_terms["abs_dev"]/merged_terms["mad"] >= 10), 1, 0)
merged_terms.loc[merged_terms["bad_price"]==1, "cal_spx_rf"] = np.nan

# -------------------------------
# Step 9: Rescale and Finalize
# Multiply the implied rates by 100 (if needed; here they are already scaled)
# -------------------------------
# (Already computed above.)
# Save the final combined results (for SP, DowJones, Nasdaq, later merge them)
final_results = merged_terms[["Date", "cal_spx_rf", "ois_fwd_spx"]].copy()
final_results.to_csv("output/equity_sf_implied_rf.csv", index=False)
print("Final dataset saved as 'output/equity_sf_implied_rf.csv'.")

# -------------------------------
# Step 10: Plot All Implied Risk-Free Rates Together
# (If you have similar columns for dow and ndaq from their processing.)
# For illustration, we plot only the SP series.
plt.figure(figsize=(12,6))
plt.plot(final_results["Date"], final_results["cal_spx_rf"], label="cal_spx_rf")
plt.ylabel("Implied RF (%)")
plt.xlabel("Date")
plt.title("Implied Risk-Free Rates (Excludes outliers and pre-2003 data)")
plt.legend()
plt.tight_layout()
plt.savefig("output/all_implied_rf.pdf")
plt.close()
print("Combined plot saved as 'output/all_implied_rf.pdf'.")
