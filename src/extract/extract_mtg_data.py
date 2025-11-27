import logging
import pandas as pd
import timeit
from pathlib import Path  # Use Path for robust file handling


# Import the modular utilities
from utils.file_utils import setup_logger, log_extract_success

# --------------------
#     CONFIGURATION
# --------------------
# Define the absolute file path using the PROJECT ROOT (CWD)
PROJECT_ROOT = Path.cwd()
FILE_PATH = PROJECT_ROOT / "data" / "raw" / "mtg_complete_data.csv"

EXPECTED_PERFORMANCE = 0.01  # Increased time for stability
TYPE = "MTG cards from CSV"

# Configure the logger
logger = setup_logger(__name__, "extract_data.log", level=logging.DEBUG)


def extract_mtg() -> pd.DataFrame:
    """
    Extracts MTG card data from the raw CSV file with performance logging.

    Returns:
        DataFrame containing card records from the CSV file.

    Raises:
        Exception: If CSV file cannot be loaded.
    """
    logger.info(f"Attempting to load data from: {FILE_PATH}")
    start_time = timeit.default_timer()

    try:
        # Pass the Path object directly to pandas
        raw_mtg_data = pd.read_csv(FILE_PATH)

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
        logger.error(f"File not found: {FILE_PATH}")
        raise FileNotFoundError(f"Failed to load CSV file: {FILE_PATH}")
    except Exception as e:
        logger.error(f"Error loading {FILE_PATH}: {e}")
        raise Exception(f"Failed to load CSV file due to an unknown error: {e}")
