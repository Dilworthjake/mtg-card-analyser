import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
from pathlib import Path

from utils.file_utils import setup_logger

########################
#        config
########################
BASE_URL = "https://deckbox.org/games/mtg/cards"
DECKBOX_ROOT = "https://deckbox.org"


# Use Pathlib to stop location errors(etl project logging_utils for ref)
# Define the project root
project_root = Path(__file__).resolve().parents[2]
# Define the absolute paths for the data directory and the CSV file.
data_raw_path = project_root / "data" / "raw"
csv_file_path = data_raw_path / "mtg_complete_data.csv"

# Log file name
logging_file = "scraping_progress.log"

##################################
# Headers to mimic a web browser
##################################
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

##############################
# columns names for the CSV
##############################
column_names = ["Name", "Edition", "Price", "Type", "Mana Cost"]

# setup the logger
logger = setup_logger(
    name="mtg_scraper",
    log_file=logging_file,
    level=logging.INFO,  # log information and errors to the file
)

##################################
# find max page limit to scrape
##################################


def get_total_pages():
    # Fetches the base page and extracts the total page count from pagination controls.

    # set total_pages to 0 so will return 0 if there are missed errors
    total_pages = 0

    # check response request
    try:
        response = requests.get(BASE_URL, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch base page for pagination check: {e}")
        return 0

    # use beautiful soup to parse html
    soup = BeautifulSoup(response.content, "html.parser")
    pagination_div = soup.find(
        "div", {"class": "pagination_controls"}
    )  # html attribute with total pages

    # total pages on site is in a span with no class
    if pagination_div:
        page_span = pagination_div.find("span", class_=None)

        # strip the html text and retrieve the last instance and return as int - log error if not found return 0 for handling
        if page_span:
            try:
                text = page_span.text.strip()
                total_pages = int(text.split("of")[-1].strip())
                return total_pages
            except ValueError:
                logger.error(f"Could not parse total pages from text: {text}")
                return 0
    return total_pages


############################
#       Scrape Function
############################


# create the scraping function the will retrieve the required information
def scrape_listing_page(url, writer):
    # progress tracking log for the page to scrape
    logger.info(f"\n--- Scraping Listing Page: {url} ---")
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        # error if issue getting page
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching listing page {url}: {e}")
        return None
    # html parser set to the table holding the information to scrape
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", {"class": "set_cards"})
    # error if issues locating table
    if not table:
        logger.error("Listing table not found on page.")
        return None
    #  tr tag with each row having a data-id
    for row in table.find_all("tr", {"data-id": True}):
        columns = row.find_all("td")
        if (
            len(columns) < 5
        ):  # table is dynamic sizing as there may be additional columns depending on page
            continue

        # columns always in same order,
        name_link = columns[0].find("a", {"class": "simple"})
        if not name_link:  # added so will not crash scraper if row data is missing
            continue
        card_name = name_link.text.strip()  # turns casrd name link into text
        edition_tag = columns[1].find("svg")  # finds edition icon
        edition_name = (
            edition_tag.get("data-title", "N/A") if edition_tag else "N/A"
        )  # retrieves icon name, also handles if icon has no title or if no icon was found
        price = (
            columns[2].text.strip().replace("$", "")
        )  # removes currency from price text
        card_type = columns[3].text.strip()  # gets text from card type

        mana_cost_td = columns[4]
        mana_symbols = []  # list as cards can have multiple manas
        for svg in mana_cost_td.find_all(
            "svg", class_="mtg_mana"
        ):  # iterate through each icon in the mana column
            symbol = next(  # use next as each line contains html like this sym_3 mtg_mana this allows it to read the sym_3 then carry on to the next
                (c for c in svg.get("class", []) if c.startswith("sym_")),
                None,  # for each icon get the text that starts with sym, if no class store as empty list meaning no mana
            )
            if symbol:  # if mana exists appending it to the list
                mana_symbols.append(symbol)
        mana_cost = " ".join(mana_symbols)  # change it to one string of text

        # save the information in a dictionary under its correct column name
        final_data = {
            "Name": card_name,
            "Edition": edition_name,
            "Price": price,
            "Type": card_type,
            "Mana Cost": mana_cost,
        }

        writer.writerow(final_data)  # convert dictionary to a line in a csv
        time.sleep(0.4)  # rate limit to avoid stress site and getting banned
    return True  # success


#########################
#       Main Function
#########################


def run_scraper():

    #  make the raw data directory exists

    data_raw_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Data directory checked/created: {data_raw_path}")

    total_pages = get_total_pages()
    page_limit = 10  # Use a test limit - uncomment if running test

    if total_pages == 0:
        logger.info("Scraping aborted: Could not determine total pages.")
        return

    loop_limit = (
        page_limit  # change this to page_limit for testing or total_pages for full run
    )
    logger.info(f"Starting scrape: {loop_limit} total pages detected.")

    # Use the path object, converted to string for the open() function
    with open(str(csv_file_path), "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()

        for page_num in range(1, loop_limit + 1):

            current_url = f"{BASE_URL}?p={page_num}"

            scrape_listing_page(current_url, writer)

            if page_num < loop_limit:
                logger.info("Pausing for 2 seconds before next page...")
                time.sleep(2)

    logger.info(f"\n--- Scraping complete. Total pages processed: {loop_limit} ---")
    logger.info(
        f"Data saved to {csv_file_path}. Check logs/{logging_file} to check for errors."
    )
