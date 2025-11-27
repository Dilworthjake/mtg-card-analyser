import requests
from bs4 import BeautifulSoup
import csv
import time
import logging

#########################
#        config

BASE_URL = "https://deckbox.org/games/mtg/cards"
DECKBOX_ROOT = "https://deckbox.org"

# Save the CSV file inside the 'data/raw' folder
CSV_FILE = "../../../data/raw/mtg_complete_data.csv"

# Save the log file inside the 'logs' folder
LOG_FILE = "../../../logs/scraping_errors.log"

##################################
# Headers to mimic a web browser

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

######################################
# List of output fields for the CSV

FIELDNAMES = ["Name", "Edition", "Price", "Type", "Mana Cost"]
#####################################
# Set up basic logging configuration

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
