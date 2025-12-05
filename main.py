import pandas as pd
import logging
from pathlib import Path
from utils.file_utils import setup_logger
from src.extract.extract_mtg_data import extract_mtg

# from src.extract.scraper import run_scraper
from src.transform.cleaner import run_cleaner
from src.load.load_mtg_data import load_normalized_data

logger = setup_logger(__name__, "main.log", level=logging.INFO)


def main():
    logger.info("Starting MTG Data Pipeline...")

    # --- PHASE 0: Scraping the web data ---

    # Scrape the site for raw data, comment this out if raw data is already available
    logger.info("--- PHASE 0: Scraping---")
    # run_scraper()

    # --- PHASE 1: EXTRACTION ---
    logger.info("--- PHASE 1: Extraction (from CSV to DataFrame) ---")

    try:
        raw_df = extract_mtg()
    except FileNotFoundError as e:
        logger.error(f"FATAL ERROR: {e}")
        print(
            "HINT: Ensure the scraper (run_scraper()) has been run to create the raw data file, or check the file path."
        )

    # --- PHASE 2: TRANSFORMATION  ---
    logger.info("--- PHASE 2: Transformation ---")

    # Capture the four DataFrames returned by the Cleaner (main card data, subtype lookup, subtype link, editions)
    try:
        (clean_card_df, subtype_lookup_df, card_subtype_link_df, edition_lookup_df) = (
            run_cleaner(raw_df)
        )
    except Exception as e:
        logger.error(f"FATAL ERROR during Transformation: {e}")

    # --- PHASE 3: LOAD (Save as clean/normalised csvs) ---
    logger.info("--- PHASE 3: Load (Saving Normalised Data) ---")

    # Pass ALL four DataFrames to the Load function
    try:
        load_normalized_data(
            clean_card_df, subtype_lookup_df, card_subtype_link_df, edition_lookup_df
        )
    except Exception as e:
        logger.error(f"FATAL ERROR during Load: {e}")

    logger.info("Pipeline complete. All normalised data tables saved to data/clean/.")


if __name__ == "__main__":
    main()
