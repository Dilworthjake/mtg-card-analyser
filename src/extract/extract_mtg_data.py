import logging
import pandas as pd
import timeit
from pathlib import Path

from utils.file_utils import setup_logger, log_extract_success

######################
#     CONFIGURATION
######################

# Use Pathlib to stop location errors
project_root = Path(__file__).resolve().parents[2]
file_path = project_root / "data" / "raw" / "mtg_complete_data.csv"

EXPECTED_PERFORMANCE = 0.01
TYPE = "MTG cards from CSV"

# set up the logger
logger = setup_logger(__name__, "extract_data.log", level=logging.DEBUG)


def extract_mtg() -> pd.DataFrame:
    """
    Extracts MTG card data from the raw CSV file with performance logging.

    Returns:
        DataFrame containing card records from the CSV file.

    Raises:
        Exception: If CSV file cannot be loaded.
    """
    logger.info(f"Attempting to load data from: {file_path}")
    start_time = timeit.default_timer()

    try:
        # Pass the Path object directly to pandas
        raw_mtg_data = pd.read_csv(file_path)

        extract_mtg_execution_time = timeit.default_timer() - start_time

        log_extract_success(
            logger,
            TYPE,
            raw_mtg_data.shape,
            extract_mtg_execution_time,
            EXPECTED_PERFORMANCE,
        )
        return raw_mtg_data

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"Failed to load CSV file: {file_path}")
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        raise Exception(f"Failed to load CSV file due to an unknown error: {e}")
