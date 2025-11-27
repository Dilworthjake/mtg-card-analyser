import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
from pathlib import Path

from utils.file_utils import setup_logger

# ----------------------
#        config
# ----------------------
BASE_URL = "https://deckbox.org/games/mtg/cards"
DECKBOX_ROOT = "https://deckbox.org"

# --- ABSOLUTE PATH DEFINITION (THE FIX) ---
# 1. Define the project root using the Current Working Directory.
PROJECT_ROOT = Path.cwd()
# 2. Define the absolute paths for the data directory and the CSV file.
ABS_DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
ABS_CSV_FILE = ABS_DATA_RAW_DIR / "mtg_complete_data.csv"

# Log file name (the directory "logs" is handled by setup_logger)
LOG_FILE = "scraping_errors.log"
# --------------------------------
# Headers to mimic a web browser
# --------------------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
# -----------------------------------
# List of output fields for the CSV
# -----------------------------------
FIELDNAMES = ["Name", "Edition", "Price", "Type", "Mana Cost"]
# ----------------------------

# Initialize the Logger
LOGGER = setup_logger(
    name="mtg_scraper",
    log_file=LOG_FILE,
    level=logging.ERROR,  # Only log ERRORs to the file
)


def get_total_pages():
    # Fetches the base page and extracts the total page count from pagination controls.
    total_pages = 0
    try:
        response = requests.get(BASE_URL, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"Failed to fetch base page for pagination check: {e}")
        return 0

    soup = BeautifulSoup(response.content, "html.parser")
    pagination_div = soup.find("div", {"class": "pagination_controls"})

    if pagination_div:
        page_span = pagination_div.find("span", class_=None)

        if page_span:
            try:
                text = page_span.text.strip()
                total_pages = int(text.split("of")[-1].strip())
                return total_pages
            except ValueError:
                LOGGER.error(f"Could not parse total pages from text: {text}")
                return 0
    return total_pages


# ----------------------------
#       Scrape Function
# ----------------------------
def scrape_listing_page(url, writer):
    print(f"\n--- Scraping Listing Page: {url} ---")
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"Error fetching listing page {url}: {e}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", {"class": "set_cards"})

    if not table:
        LOGGER.error("Listing table not found on page.")
        return None

    for row in table.find_all("tr", {"data-id": True}):
        columns = row.find_all("td")
        if len(columns) < 5:
            continue

        name_link = columns[0].find("a", {"class": "simple"})
        if not name_link:
            continue
        card_name = name_link.text.strip()
        edition_tag = columns[1].find("svg")
        edition_name = edition_tag.get("data-title", "N/A") if edition_tag else "N/A"
        price = columns[2].text.strip().replace("$", "")
        card_type = columns[3].text.strip()

        mana_cost_td = columns[4]
        mana_symbols = []
        for svg in mana_cost_td.find_all("svg", class_="mtg_mana"):
            symbol = next(
                (c for c in svg.get("class", []) if c.startswith("sym_")), None
            )
            if symbol:
                mana_symbols.append(symbol)
        mana_cost = " ".join(mana_symbols)

        final_data = {
            "Name": card_name,
            "Edition": edition_name,
            "Price": price,
            "Type": card_type,
            "Mana Cost": mana_cost,
        }

        writer.writerow(final_data)
        time.sleep(0.5)
    return True


# ----------------------------
#       Main Function (Run from orchestrator)
# ----------------------------
def run_scraper():

    # 💥 STEP 1: Ensure the raw data directory exists 💥
    # This now uses the safe, absolute path: mtg_etl_project/data/raw
    ABS_DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Data directory checked/created: {ABS_DATA_RAW_DIR}")

    total_pages = get_total_pages()
    page_limit = 2  # Use a test limit

    if total_pages == 0:
        print("Scraping aborted: Could not determine total pages.")
        return

    loop_limit = min(total_pages, page_limit)
    print(f"Starting scrape: {loop_limit} total pages detected.")

    # ----------------------------
    #       CSV Function
    # ----------------------------
    # Use the absolute Path object, converted to string for the open() function
    with open(str(ABS_CSV_FILE), "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()

        for page_num in range(1, loop_limit + 1):

            current_url = f"{BASE_URL}?p={page_num}"

            scrape_listing_page(current_url, writer)

            if page_num < loop_limit:
                print(f"Pausing for 10 seconds before next page...")
                time.sleep(10)

    print(f"\n--- Scraping complete. Total pages processed: {loop_limit} ---")
    print(f"Data saved to {ABS_CSV_FILE}. Check logs/{LOG_FILE} for errors.")
