import pandas as pd
import logging
from pathlib import Path
from typing import Tuple

from utils.file_utils import setup_logger, INFO

load_logger = setup_logger(__name__, "load_data.log", level=INFO)

# --- CONFIGURATION ---
project_root = Path(__file__).resolve().parents[2]
data_clean_path = project_root / "data" / "clean"


# expect four data frames from cleaner
def load_normalized_data(
    card_df: pd.DataFrame,
    edition_lookup_df: pd.DataFrame,
    subtype_lookup_df: pd.DataFrame,
    card_subtype_link_df: pd.DataFrame,
):
    """
    Saves all four normalised DataFrames to the data/clean directory.
    Strictly performs file saving (Load phase).
    """

    # 1. Ensure clean directory exists
    data_clean_path.mkdir(parents=True, exist_ok=True)
    load_logger.info(f"Load phase saving normalised tables to: {data_clean_path}")

    # --- 2. Saving Tables ---

    # Edition Table
    load_logger.info("Saving edition_lookup.csv ...")
    edition_lookup_df.to_csv(data_clean_path / "edition_lookup.csv", index=False)

    # Subtype and Link Tables
    load_logger.info("Saving subtype_lookup.csv ...")
    subtype_lookup_df.to_csv(data_clean_path / "subtype_lookup.csv", index=False)

    load_logger.info("Saving card_subtype_link.csv ...")
    card_subtype_link_df.to_csv(data_clean_path / "card_subtype_link.csv", index=False)

    # Card Details Table
    load_logger.info("Saving card_details.csv ...")
    card_df.to_csv(data_clean_path / "card_details.csv", index=False)

    load_logger.info("Load phase complete. All four normalised tables saved.")
