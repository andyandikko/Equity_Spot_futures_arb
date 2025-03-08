"""
settings.py
-----------
Provides easy access to paths and credentials used in the project.
Creates required directories and (optionally) touches log files to ensure they exist.
"""

from pathlib import Path
from platform import system

from decouple import config as _config
from pandas import to_datetime

def get_os():
    os_name = system()
    if os_name == "Windows":
        return "windows"
    elif os_name in ("Darwin", "Linux"):
        return "nix"
    else:
        return "unknown"

# This internal dictionary `d` holds all settings
d = {}

d["OS_TYPE"] = get_os()

# Get absolute path to the root directory of the project
d["BASE_DIR"] = Path(__file__).absolute().parent.parent

def if_relative_make_abs(path):
    """If a relative path is given, make it absolute, assuming
    that it is relative to the project root directory (BASE_DIR).
    """
    path = Path(path)
    if path.is_absolute():
        return path.resolve()
    return (d["BASE_DIR"] / path).resolve()

# Load standard config values
d["START_DATE"] = _config("START_DATE", default="2010-01-01", cast=to_datetime)
d["END_DATE"]   = _config("END_DATE", default="2024-12-31", cast=to_datetime)

d["PIPELINE_DEV_MODE"] = _config("PIPELINE_DEV_MODE", default=True, cast=bool)
d["PIPELINE_THEME"]    = _config("PIPELINE_THEME", default="pipeline")
d["USING_XBBG"]        = _config("USING_XBBG", default=False, cast=bool)

# Define your key paths here
d["DATA_DIR"]      = if_relative_make_abs(_config('DATA_DIR', default=Path('_data'), cast=Path))
d["MANUAL_DATA_DIR"] = if_relative_make_abs(_config('MANUAL_DATA_DIR', default=Path('data_manual'), cast=Path))
d["INPUT_DIR"]     = if_relative_make_abs(_config('INPUT_DIR', default=Path('_data/input'), cast=Path))
d["PROCESSED_DIR"] = if_relative_make_abs(_config('PROCESSED_DIR', default=Path('_data/processed'), cast=Path))
d["OUTPUT_DIR"]    = if_relative_make_abs(_config('OUTPUT_DIR', default=Path('_output'), cast=Path))
d["PUBLISH_DIR"]   = if_relative_make_abs(_config('PUBLISH_DIR', default=Path('_output/publish'), cast=Path))
d["TEMP_DIR"]      = if_relative_make_abs(_config('TEMP_DIR', default=Path('_output/temp'), cast=Path))

# Name of Stata Executable
if d["OS_TYPE"] == "windows":
    d["STATA_EXE"] = _config("STATA_EXE", default="StataMP-64.exe")
elif d["OS_TYPE"] == "nix":
    d["STATA_EXE"] = _config("STATA_EXE", default="stata-mp")
else:
    raise ValueError("Unknown OS type")


def create_dirs():
    """
    Ensure all directories needed for data/output/logging exist.
    Optionally, create empty log files if they don't exist.
    """
    d["DATA_DIR"].mkdir(parents=True, exist_ok=True)
    d["OUTPUT_DIR"].mkdir(parents=True, exist_ok=True)
    d["TEMP_DIR"].mkdir(parents=True, exist_ok=True)
    d["INPUT_DIR"].mkdir(parents=True, exist_ok=True)
    d["PUBLISH_DIR"].mkdir(parents=True, exist_ok=True)
    d["PROCESSED_DIR"].mkdir(parents=True, exist_ok=True)

    # If you'd like to ensure these log files exist (touch them):
    for log_filename in (
        "futures_processing.log",
        "ois_processing.log",
        "bloomberg_data_extraction.log",
    ):
        log_file_path = d["TEMP_DIR"] / log_filename
        log_file_path.touch(exist_ok=True)


def config(*args, **kwargs):
    """
    Retrieve configuration variables. 
    Checks `d` first. If not found, falls back to .env via decouple.config.
    """
    key = args[0]
    default = kwargs.get("default", None)
    cast = kwargs.get("cast", None)
    if key in d:
        var = d[key]
        # If a default was passed but we already have a value in d, raise an error
        if default is not None:
            raise ValueError(f"Default for {key} already exists. Check your settings.py file.")
        if cast is not None:
            # If cast is requested, check that it wouldn't change the type
            if not isinstance(var, cast):
                # or if we want to actually recast:
                try:
                    new_var = cast(var)
                except Exception as e:
                    raise ValueError(f"Could not cast {key} to {cast}: {e}") from e
                if type(new_var) is not type(var):
                    raise ValueError(f"Type for {key} differs. Check your settings.py file.")
        return var
    else:
        return _config(*args, **kwargs)


if __name__ == "__main__":
    create_dirs()
