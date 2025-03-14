# Equity Spot-Futures Arbitrage Analysis

This repository implements the methodology described in the paper [Segmented Arbitrage](https://www.hbs.edu/ris/Publication%20Files/24-030_1506d32b-3190-4144-8c75-a2326b87f81e.pdf) by Emil Siriwardane, Adi Sunderam, and Jonathan Wallen about Equity Spot-Futures Arbitrage to measure potential arbitrage opportunities in equity index futures markets. The analysis computes the spread between the implied funding rates derived from consecutive futures contracts and the risk-free OIS rate, providing insights into market frictions and funding constraints. Here is the link to our notebooks webpage [Webpage](https://andyandikko.github.io/Equity_Spot_futures_arb/index.html)

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

| **Task (Points)**                                                                                   | **Completion (Yes/No)** | **Evidence**                                                                                                                                                                                              |
|------------------------------------------------------------------------------------------------------|--------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1. (4/4) Generate a single LaTeX document with all tables and figures and a high-level overview of the replication | Yes                      | `./reports/report.tex`                                                                                                                                                                                   |
| 2. (4/4) Provide at least one Jupyter notebook that briefly tours the cleaned data and some analysis | Yes                      | `./src/01_OIS_Data_Processing.ipynb, ./src/02_Futures_Data_Processing.ipynb, ./src/03_Spread_Calculations.ipynb`                                                                                         |
| 3. (20/20) Replicate the series, tables, and/or figures from the paper within a reasonable tolerance | Yes                      | figures in `./reports/report.pdf`, tests in `./src/test_spread_calculations.py`                                                                                                                           |
| 4. (20/20) Reproduce the same series, tables, and/or figures with updated data (up to present)       | Yes                      | `./reports/report.tex`                                                                                                                                                                                   |
| 5. (20/20) Include your own summary statistics and charts with thorough captions in the LaTeX document | Yes                      | `/reports/report.tex`                                                                                                                                                                                    |
| 6. (4/4) Ensure the data is cleaned and placed into a “tidy” format in a separate file/set of files  | Yes                      | After running `doit`, `./src/OIS_data_processing.py` and `./src/futures_data_processing.py` generate cleaned CSV data in `./_data/processed`                                                                 |
| 7. (4/4) Make sure all table statistics in the LaTeX document are automatically generated by code    | Yes                      | After running `doit`, `ois_summary_statistics.tex` in `./output` which has a summary statistics table that `./reports/report.tex` refers to                                                                 |
| 8. (4/4) Automate the project end-to-end using PyDoit                                               | Yes                      | Everything is generated by running `doit`                                                                                                                                                                 |
| 9. (4/4) Use unit tests to ensure the code is working properly and is well motivated                 | Yes                      | Tests are in `./src`, all pass                                                                                                                                                                           |
| 10. (4/4) Use the template from `jmbejara/blank_project`                                             | Yes                      | Repo is generated from `jmbejara/blank_project`                                                                                                                                                          |
| 11. (4/4) Each unit test must have a clear purpose (no unnecessary or repetitive tests)             | Yes                      | All tests are documented with explicit purpose in `./src` test files                                                                                                                                      |
| 12. (4/4) Ensure the GitHub repository and history are free of copyrighted material                 | Yes                      | No copyright material                                                                                                                                                                                    |
| 13. (4/4) Ensure the GitHub repository and history are free of secrets (e.g., API keys)             | Yes                      | No keys/secrets                                                                                                                                                                                          |
| 14. (4/4) Use a `.env` file with defaults in `settings.py` to manage configuration (provide `.env.example`) | Yes                      | Run `git log --all --grep=".env"`                                                                                                                                                                        |
| 15. (4/4) Ensure there is no trace of the actual `.env` file in the repository or commit history     | Yes                      | No `.env` file in repo                                                                                                                                                                                  |
| 16. (4/4) Provide a `requirements.txt` that describes required packages                              | Yes                      | `requirements.txt` has all required packages                                                                                                                                                              |
| 17. (4/4) (Repeat check) Repository must be free of any secrets (especially API keys)                | Yes                      | No API keys                                                                                                                                                                                              |
| 18. (4/4) Each group member must have made commits to the Git repo                                   | Yes                      | Commits made by Andy and Harrison                                                                                                                                                                        |
| 19. (4/4) Each member must have created and merged a Pull Request                                    | Yes                      | Pull requests made by Andy and Harrison                                                                                                                                                                  |
| 20. (4/4) Each Python file must have a top-level docstring explaining its purpose                    | Yes                      | Docstrings on all files                                                                                                                                                                                 |
| 21. (4/4) Each Python function must have a descriptive name and (when appropriate) a docstring       | Yes                      | Function naming follows good and descriptive conventions                                                                                                                                                  |
| 22. (50/50) Did you individually accomplish the tasks assigned and contribute substantially (per commit history)? | Yes                      | Issues were made and solved by Andy and Harrison                                                                                                                                                         |

## Further Details

For more detailed explanations of the methodology and analysis results, please refer to:
- The generated report in `reports/report.pdf`
- The Jupyter notebooks in `src/` and their HTML versions in `_output/`

## Acknowledgments

We thank Professor Jeremy Bejarano for his guidance and mentorship throughout this project.
