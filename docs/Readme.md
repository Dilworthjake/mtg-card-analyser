# MTG Card Data ETL Pipeline

## MTG Project Overview

This project implements an Extract, Transform, and Load (ETL) pipeline designed to scrape Magic: The Gathering (MTG) card data from an online source and transform it into clean, normalized data. The resulting tables are saved as CSV files, ready for analysis with Pandas to be used with streamlit. The CSV format makes it readily able to be loaded to a relation Database in the future.

The primary goal is to provide a dataset for MTG card analysis, specifically focusing on card characteristics (like mana cost and type) and then use this to power a streamlit app for user accessibility

### ETL Pipeline Architecture

The pipeline is executed via main.py and follows four phases: Scraping, Extraction, Transformation, and Loading.

#### 1. Scraping

- Source: Data is scraped from Deckbox's publicly accessible card listings.

- Method: Uses standard Python libraries (e.g., requests, BeautifulSoup) to iterate through multiple pages of card listings, extracting raw fields: Name, Edition, Price, Type, and Mana Cost.

- Output: The raw, uncleaned data is stored in a single CSV file: data/raw/mtg_complete_data.csv.

- Notes: As this is a long process(4 hours) it can be skipped by commenting out the call function in the main orchestrator the raw data as of 02/12/25 will be saved with the repo but may be out of date
  to test the scraper go to src/extract/scraper.py there you can uncomment line 140 and change line 147 to page_limit you may also want to change line 21 from mtg_complete_data.csv to mtg_test_data.csv as to not overwrite the current raw data

#### 2. Extracting

- This phase is handled by src/extract/extract_mtg_data.py script. It extracts the data from the data/raw/mtg_complete_data.csv by reading the file and saving to a pandas Dataframe

#### 3. Transforming

- This phase is handled by the src/transform/cleaner.py script. It performs cleaning, feature engineering, and data normalization, ensuring data quality and preparing the schema for analysis.

- Main Transformation Steps:

- Split Card Handling: Dual-faced cards (Split, Adventure, MDFCs) are processed by creating a separate row for each face, ensuring accurate representation of names, types, and mana costs.

- Mana Cost Parsing: The raw Mana Cost string is converted into boolean colour columns and other categories:

  - CMC (Converted Mana Cost)

  - Generic_Mana count

  - Colour flags (Is_W, Is_U, Is_B, Is_R, Is_G)

  - Special flags (Is_X, Is_Hybrid)

- Type Parsing: The Type string is split into Super_Type, Primary_Type, and Subtypes_List.

- Normalisation: Two dimension tables (edition_lookup, subtype_lookup) and a join table (card_subtype_link) are created to follow normalisation practices

#### 4. Loading

- The normalised data is saved into the data/clean directory as four separate CSV files, which collectively form the project's final data model.

### Streamlit

- The included Streamlit application (app.py) is a deck-building analysis tool.

- Functionality: The app allows users to filter the entire MTG card database using key attributes like Name, Colour Identity, Type/Subtype, and Converted Mana Cost (CMC). It then provides interactive visualizations, including a Mana Curve histogram and Color/Type Distribution charts, helping players optimise their deck construction and mana base.

## Set up

### basic set up steps

- Clone the repository:

  - git clone [your-repo-link]
  - cd mtg_etl_project

- Create a virtual environment (Recommended):

  - python -m venv venv
  - source venv/bin/activate # On Linux/macOS
  - .\venv\Scripts\activate # On Windows

- Install dependencies:

  - pip install -r requirements.txt

- Running the Pipeline

  - The main entry point for the entire ETL process is main.py.

  - python -m main.py

Upon completion, you will find the processed, normalised data in the data/clean/ directory. Check the project logs for detailed execution reports and any encountered warnings.
At this point you can run the streamlit app

- streamlit run src/app/app.py
