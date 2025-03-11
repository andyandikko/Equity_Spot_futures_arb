"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based

"""

#######################################
## Configuration and Helpers for PyDoit
#######################################
## Make sure the src folder is in the path
import sys

sys.path.insert(1, "./src/")

import shutil
from os import environ, getcwd, path
from pathlib import Path

from colorama import Fore, Style, init

## Custom reporter: Print PyDoit Text in Green
# This is helpful because some tasks write to sterr and pollute the output in
# the console. I don't want to mute this output, because this can sometimes
# cause issues when, for example, LaTeX hangs on an error and requires
# presses on the keyboard before continuing. However, I want to be able
# to easily see the task lines printed by PyDoit. I want them to stand out
# from among all the other lines printed to the console.
from doit.reporter import ConsoleReporter

from settings import config

try:
    in_slurm = environ["SLURM_JOB_ID"] is not None
except:
    in_slurm = False


class GreenReporter(ConsoleReporter):
    def write(self, stuff, **kwargs):
        doit_mark = stuff.split(" ")[0].ljust(2)
        task = " ".join(stuff.split(" ")[1:]).strip() + "\n"
        output = (
            Fore.GREEN
            + doit_mark
            + f" {path.basename(getcwd())}: "
            + task
            + Style.RESET_ALL
        )
        self.outstream.write(output)


if not in_slurm:
    DOIT_CONFIG = {
        "reporter": GreenReporter,
        # other config here...
        # "cleanforget": True, # Doit will forget about tasks that have been cleaned.
        "backend": "sqlite3",
        "dep_file": "./.doit-db.sqlite",
    }
else:
    DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}
init(autoreset=True)


BASE_DIR = config("BASE_DIR")
DATA_DIR = config("DATA_DIR")
MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")
OS_TYPE = config("OS_TYPE")
# PUBLISH_DIR = config("PUBLISH_DIR")
TEMP_DIR = config("TEMP_DIR")
INPUT_DIR = config("INPUT_DIR")
PROCESSED_DIR = config("PROCESSED_DIR")

## Helpers for handling Jupyter Notebook tasks
# fmt: off
## Helper functions for automatic execution of Jupyter notebooks
environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"
def jupyter_execute_notebook(notebook):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --log-level WARN --inplace ./src/{notebook}.ipynb"
def jupyter_to_html(notebook, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --log-level WARN --output-dir={output_dir} ./src/{notebook}.ipynb"
def jupyter_to_md(notebook, output_dir=OUTPUT_DIR):
    """Requires jupytext"""
    return f"jupytext --to markdown --log-level WARN --output-dir={output_dir} ./src/{notebook}.ipynb"
def jupyter_to_python(notebook, build_dir):
    """Convert a notebook to a python script"""
    return f"jupyter nbconvert --log-level WARN --to python ./src/{notebook}.ipynb --output _{notebook}.py --output-dir {build_dir}"
def jupyter_clear_output(notebook):
    return f"jupyter nbconvert --log-level WARN --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace ./src/{notebook}.ipynb"
# fmt: on


def copy_file(origin_path, destination_path, mkdir=True):
    """Create a Python action for copying a file."""

    def _copy_file():
        origin = Path(origin_path)
        dest = Path(destination_path)
        if mkdir:
            dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, dest)

    return _copy_file


##################################
## Begin rest of PyDoit tasks here
##################################


from pathlib import Path
from settings import config

# Define the paths based on configuration
BASE_DIR = config("BASE_DIR")
DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
TEMP_DIR = Path(config("TEMP_DIR"))
INPUT_DIR = Path(config("INPUT_DIR"))
# PUBLISH_DIR = Path(config("PUBLISH_DIR"))
PROCESSED_DIR = Path(config("PROCESSED_DIR"))

# Define log file paths
LOG_FILES = [
    TEMP_DIR / "futures_processing.log",
    TEMP_DIR / "ois_processing.log",
    TEMP_DIR / "bloomberg_data_extraction.log"
]

def task_config():
    """Create empty directories for data and output if they don't exist, and ensure log files are created"""
    return {
        "actions": ["ipython ./src/settings.py"],  # This action should ensure directories and files are prepared
        "targets": [
            DATA_DIR, OUTPUT_DIR, TEMP_DIR, INPUT_DIR,  PROCESSED_DIR
        ] + LOG_FILES,  # Include log files in the targets to manage their existence
        "file_dep": ["./src/settings.py"],
        "clean": True,  # This will clean up all directories and log files when 'doit clean' is executed
    }

def task_pull_bloomberg():
    """ """
    file_dep = [
        "./src/settings.py"
    ]
    targets = [
        INPUT_DIR / "bloomberg_historical_data.parquet"
    ]

    return {
        "actions": [
            "ipython ./src/pull_bloomberg_data.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": [],  # Don't clean these files by default. The ideas
        # is that a data pull might be expensive, so we don't want to
        # redo it unless we really mean it. So, when you run
        # doit clean, all other tasks will have their targets
        # cleaned and will thus be rerun the next time you call doit.
        # But this one wont.
        # Use doit forget --all to redo all tasks. Use doit clean
        # to clean and forget the cheaper tasks.
    }

def task_process_futures_data():
    """
    Process futures data for indices after pulling the latest data.
    """
    file_dep = [
        "./src/settings.py",
        "./src/pull_bloomberg_data.py",
        "./src/futures_data_processing.py"
    ]
    targets = [
        PROCESSED_DIR / "all_indices_calendar_spreads.csv",
        PROCESSED_DIR / "INDU_calendar_spread.csv",
        PROCESSED_DIR / "SPX_calendar_spread.csv",
        PROCESSED_DIR / "NDX_calendar_spread.csv",
    ]

    return {
        "actions": [
            "python ./src/futures_data_processing.py",
        ],
        "file_dep": file_dep,
        "targets": targets,
        "clean": True,  
    }

def task_process_ois_data():
    """
    Process OIS data for 3-month rates after pulling the latest Bloomberg data.
    """
    file_dep = [
        "./src/settings.py",
        "./src/pull_bloomberg_data.py",
        "./src/OIS_data_processing.py"
    ]
    targets = [
        PROCESSED_DIR / "cleaned_ois_rates.csv"
    ]

    return {
        "actions": [
            "python ./src/OIS_data_processing.py",
        ],
        "file_dep": file_dep,
        "targets": targets,
        "clean": True,  # Add appropriate clean actions if necessary
    }

def task_spread_calculations():
    """
    Spread calculations from processed data
    """
    file_dep = [
        "./src/settings.py",
        "./src/pull_bloomberg_data.py",
        "./src/OIS_data_processing.py",  
        "./src/futures_data_processing.py"
    ]
    targets = [
        PROCESSED_DIR / "SPX_Forward_Rates.csv",
        PROCESSED_DIR / "NDX_Forward_Rates.csv",
        PROCESSED_DIR / "INDU_Forward_Rates.csv",
        OUTPUT_DIR / "all_indices_spread_to_2020.png",
        OUTPUT_DIR / "all_indices_spread_to_present.png"
    ]

    return {
        "actions": [
            "python ./src/Spread_calculations.py",
        ],
        "file_dep": file_dep,
        "targets": targets,
        "clean": True,  
    }


notebook_tasks = {
    "01_OIS_Data_Processing.ipynb": {
        "file_dep": ["./src/pull_bloomberg_data.py", "./src/OIS_data_processing.py"],
        "targets": [OUTPUT_DIR / 'ois_3m_rolling_statistics.png',
                    OUTPUT_DIR / 'ois_3m_rate_time_series.png',
                    OUTPUT_DIR / "ois_summary_statistics.tex"],
    },
    "02_Futures_Data_Processing.ipynb": {
        "file_dep": ["./src/pull_bloomberg_data.py", "./src/futures_data_processing.py"],
        "targets": [OUTPUT_DIR / "es1_contract_roll_pattern.png",
                    OUTPUT_DIR / "es1_ttm_distribution.png",
                    OUTPUT_DIR / "futures_prices_by_index.png",],
    },
    "03_Spread_Calculations.ipynb": {
        "file_dep": [
            "./src/pull_bloomberg_data.py", 
            "./src/futures_data_processing.py",
            "./src/OIS_data_processing.py",
            "./src/Spread_calculations.py"
        ],
        "targets": [],
    },
}


def task_convert_notebooks_to_scripts():
    """Convert notebooks to script form to detect changes to source code rather
    than to the notebook's metadata.
    """
    build_dir = Path(OUTPUT_DIR)

    for notebook in notebook_tasks.keys():
        notebook_name = notebook.split(".")[0]
        yield {
            "name": notebook,
            "actions": [
                jupyter_clear_output(notebook_name),
                jupyter_to_python(notebook_name, build_dir),
            ],
            "file_dep": [Path("./src") / notebook],
            "targets": [OUTPUT_DIR / f"_{notebook_name}.py"],
            "clean": True,
            "verbosity": 0,
        }


# fmt: off
def task_run_notebooks():
    """Preps the notebooks for presentation format.
    Execute notebooks if the script version of it has been changed.
    """
    for notebook in notebook_tasks.keys():
        notebook_name = notebook.split(".")[0]
        yield {
            "name": notebook,
            "actions": [
                """python -c "import sys; from datetime import datetime; print(f'Start """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
                jupyter_execute_notebook(notebook_name),
                jupyter_to_html(notebook_name),
                copy_file(
                    Path("./src") / f"{notebook_name}.ipynb",
                    OUTPUT_DIR / f"{notebook_name}.ipynb",
                    mkdir=True,
                ),
                jupyter_clear_output(notebook_name),
                # jupyter_to_python(notebook_name, build_dir),
                """python -c "import sys; from datetime import datetime; print(f'End """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
            ],
            "file_dep": [
                OUTPUT_DIR / f"_{notebook_name}.py",
                *notebook_tasks[notebook]["file_dep"],
            ],
            "targets": [
                OUTPUT_DIR / f"{notebook_name}.html",
                OUTPUT_DIR / f"{notebook_name}.ipynb",
                *notebook_tasks[notebook]["targets"],
            ],
            "clean": True,
        }



# ###############################################################
# ## Sphinx documentation
# ###############################################################

notebook_sphinx_pages = [
    "./_docs/_build/html/notebooks/" + notebook.split(".")[0] + ".html"
    for notebook in notebook_tasks.keys()
]
sphinx_targets = [
    "./_docs/_build/html/index.html",
    *notebook_sphinx_pages
]


def copy_docs_src_to_docs():
    """
    Copy all files and subdirectories from the docs_src directory to the _docs directory.
    This function loops through all files in docs_src and copies them individually to _docs,
    preserving the directory structure. It does not delete the contents of _docs beforehand.
    """
    src = Path("docs_src")
    dst = Path("_docs")

    # Ensure the destination directory exists
    dst.mkdir(parents=True, exist_ok=True)

    # Loop through all files and directories in docs_src
    for item in src.rglob("*"):
        relative_path = item.relative_to(src)
        target = dst / relative_path
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            shutil.copy2(item, target)


def copy_docs_build_to_docs():
    """
    Copy all files and subdirectories from _docs/_build/html to docs.
    This function copies each file individually while preserving the directory structure.
    It does not delete any existing contents in docs.
    After copying, it creates an empty .nojekyll file in the docs directory.
    """
    src = Path("_docs/_build/html")
    dst = Path("docs")
    dst.mkdir(parents=True, exist_ok=True)

    # Loop through all files and directories in src
    for item in src.rglob("*"):
        relative_path = item.relative_to(src)
        target = dst / relative_path
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)

    # Touch an empty .nojekyll file in the docs directory.
    (dst / ".nojekyll").touch()


def task_compile_sphinx_docs():
    """Compile Sphinx Docs"""
    notebook_scripts = [
        OUTPUT_DIR / ("_" + notebook.split(".")[0] + ".py")
        for notebook in notebook_tasks.keys()
    ]
    file_dep = [
        "./docs_src/conf.py",
        "./docs_src/index.md",
        "./docs_src/myst_markdown_demos.md",
        *notebook_scripts,
    ]

    return {
        "actions": [
            copy_docs_src_to_docs,
            "sphinx-build -M html ./_docs/ ./_docs/_build",
            copy_docs_build_to_docs,
        ],
        "targets": sphinx_targets,
        "file_dep": file_dep,
        "task_dep": ["run_notebooks"],
        "clean": True,
    }



# # ###############################################################
# # ## Task below is for LaTeX compilation
# # ###############################################################


def task_compile_latex_docs():
    """Compile the LaTeX documents to PDFs"""
    file_dep = [
        "./reports/report.tex"
    ]
    targets = [
        "./reports/report.pdf"
    ]

    return {
        "actions": [
            "latexmk -xelatex -halt-on-error -cd ./reports/report.tex",  # Compile
            "latexmk -xelatex -halt-on-error -c -cd ./reports/report.tex"  # Clean
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
    }
