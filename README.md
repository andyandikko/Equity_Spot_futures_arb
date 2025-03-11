# Equity Spot-Futures Arbitrage Analysis

This repository implements the methodology described in the paper about Equity Spot-Futures Arbitrage to measure potential arbitrage opportunities in equity index futures markets. The analysis computes the spread between the implied funding rates derived from consecutive futures contracts and the risk-free OIS rate, providing insights into market frictions and funding constraints. Here is the link to our notebooks webpage [Webpage](https://andyandikko.github.io/Equity_Spot_futures_arb/index.html)

## Project Overview

The project calculates the "ESF" (Equity Spot-Futures) arbitrage spread by:

1. Extracting Bloomberg data for equity indices (S&P 500, Nasdaq 100, Dow Jones)
2. Processing futures data to determine settlement dates and time-to-maturity
3. Computing implied forward rates and comparing to OIS rates
4. Analyzing and visualizing the results

## Repository Structure

- `src/`: Source code files
  - `pull_bloomberg_data.py`: Extracts data from Bloomberg via xbbg
  - `futures_data_processing.py`: Processes futures data for maturity and TTM
  - `OIS_data_processing.py`: Cleans and processes OIS rate data
  - `Spread_calculations.py`: Calculates the implied forward rates and spreads
  - Notebooks (01, 02, 03): Detailed analysis and visualization of each step

- `_data/`: Main data directory
  - `input/`: Raw Bloomberg data
  - `processed/`: Intermediate processed files

- `_data_manual/`: Manually cached data
  - Contains Bloomberg data backups
  - Test data made publicly available by the authors

- `reports/`: LaTeX report files
  - `report.tex`: Main paper replication report template
  - `report.pdf`: Automatically generated report

- `_output/`: Final visualizations and notebook outputs

- `_temp/`: Logging files for debugging and tracking process execution

## Getting Started

1. Clone this repository
2. Install required packages:
   ```
   conda create --name test python=3.12
   conda activate test
   pip install -r requirements.txt
   ```
3. Configure your environment in `.env` file (see `.env.example` for required variables)
4. Run the entire workflow:
   ```
   doit
   ```


## Configuration

The project configuration is managed through:
- `settings.py`: Central configuration file with paths and parameters
- `.env`: Environment variables for user defined settings (see `.env.example`)
- Key settings include data directories, date ranges, and toggle for Bloomberg connectivity

## Requirements Met

This project successfully:
- Replicates the original paper's results within a 5bps RMSE tolerance
- Extends analysis through March 2025 with updated market data
- Provides informative visualizations with comprehensive captions
- Implements separate data cleaning and analysis processes
- Automates the entire workflow with PyDoit
- Uses robust unit testing to verify calculations
- Excludes raw data from version control for copyright compliance
- Follows proper documentation standards

## Further Details

For more detailed explanations of the methodology and analysis results, please refer to:
- The generated report in `reports/report.pdf`
- The Jupyter notebooks in `src/` and their HTML versions in `_output/`

## Acknowledgments

We thank Professor Jeremy Bejarano for his guidance and mentorship throughout this project.