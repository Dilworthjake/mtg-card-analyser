from pathlib import Path
import logging
from typing import Optional, Tuple  # Removed Final

# --------------------
#     CONSTANTS
# --------------------
# Define the logging level constants using standard Python variables
INFO = logging.INFO
DEBUG = logging.DEBUG
ERROR = logging.ERROR


def _ensure_log_directory() -> Path:
    """
    Create the 'logs' directory at the project root (Current Working Directory)
    if it doesn't exist.

    Returns:
        Path to the absolute logs directory.
    """
    project_root = Path.cwd()
    log_directory = project_root / "logs"

    log_directory.mkdir(parents=True, exist_ok=True)
    print(f"Directory checked/created: {log_directory}")
    return log_directory


def _create_formatter() -> logging.Formatter:
    """Create standard log formatter with timestamp and level."""
    return logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def _create_handlers(
    log_directory: Path, log_file: str, level: int
) -> Tuple[logging.FileHandler, logging.StreamHandler]:
    """Create configured file and console logging handlers."""

    # Use log_directory / log_file (Path joining)
    file_handler = logging.FileHandler(log_directory / log_file)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    formatter = _create_formatter()
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    return file_handler, console_handler


def setup_logger(
    name: str,
    log_file: str,
    level: int = INFO,  # Using the defined INFO constant
) -> logging.Logger:
    """
    Configure logger with file and console output.

    Args:
        name: Logger name, typically __name__.
        log_file: Name of the log file (e.g., "scraper.log").
        level: Logging level. Defaults to INFO.

    Returns:
        Configured logger instance.
    """
    log_directory = _ensure_log_directory()

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevents adding handlers multiple times if logger is called more than once
    if not logger.handlers:
        file_handler, console_handler = _create_handlers(log_directory, log_file, level)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


def log_extract_success(
    logger: logging.Logger,
    data_type: str,  # Renamed from 'type' to avoid shadowing built-in function
    shape: Tuple[int, int],
    execution_time: float,
    expected_rate: float,
) -> None:
    """Log successful data extraction with performance analysis.

    Args:
        logger: Logger instance to use for output.
        data_type: Description of the data type extracted.
        shape: Tuple of (rows, columns) extracted.
        execution_time: Time taken for extraction in seconds.
        expected_rate: Expected time per row threshold.
    """
    logger.info(f"Data extraction successful for {data_type}!")
    logger.info(f"Extracted {shape[0]} rows and {shape[1]} columns")
    logger.info(f"Execution time: {execution_time:.4f} seconds")

    # Conditional check to prevent ZeroDivisionError
    if shape[0] == 0:
        logger.warning("Extracted 0 rows. Performance check skipped.")
        return

    time_per_row = execution_time / shape[0]

    if time_per_row <= expected_rate:
        logger.info(f"Execution time per row: {time_per_row:.6f} seconds (OK)")
    else:
        logger.warning(
            f"Execution time per row exceeds {expected_rate:.6f}s: "
            f"{time_per_row:.6f} seconds (🚨 SLOW!)"
        )
