from src.extract.scraper import run_scraper
from src.extract.extract_mtg_data import extract_mtg


def main():
    print("🎬 Starting MTG Data Pipeline...")

    # --- PHASE 1: EXTRACTION (Scraping) ---
    print("\n--- PHASE 1: Extraction ---")
    run_scraper()

    # --- PHASE 2: TRANSFORMATION ---
    raw_df = extract_mtg()


if __name__ == "__main__":
    main()
