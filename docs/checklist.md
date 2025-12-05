# MTG Card Data Analysis - Project Checklist

## 1. ETL Pipeline (Data Preparation)

### Phase 0: Utility

- [x] Create utility functions such as logger

  - [x] Create formatter function
  - [x] Create handler function
  - [x] create directory check function
  - [x] create success function

- [x] create a main script to run entire ETL pipeline

### Phase 0: Scraping and Raw Data Storage

- [x] Start logging system and data directory structure.

- [x] Function to determine total number of pages for scraping.

- [x] Function to scrape a single card listing page.

- [x] Create loop logic for paginated scraping with delays (to lower stress on site).

- [x] Save all extracted raw data to data/raw/mtg_complete_data.csv.

### Phase 1: Data Extraction (CSV to DataFrame)

- [x] Data loading function to read mtg_complete_data.csv.(pandas)

- [x] Log success message including extracted row and column counts.

- [x] Error handling for file not found or corrupted CSV.

### Phase 2: Data Transformation and Normalization

- [x] Clean the Edition column (removing prefixes).

- [x] Mana Cost parsing.

  - [x] Calculate CMC
  - [x] Find generic mana amount
  - [x] X Cost boolean

- [x] Create logic for setting Colour Flags (Is_W, Is_U, Is_B, Is_R, Is_G).

- [x] Create extraction of Super_Type, Primary_Type, and raw subtypes from Type column.

- [x] Normalise Subtypes: create subtype_lookup table.

- [x] Normalise Editions: create edition_lookup table.
- [x] Create Join Table: generate card_subtype_link table linking Card_ID and Subtype_ID.

- [x] Finalise Fact Table: generate card_details with all Foreign Keys.

- [ ] Create a splitter for cards with two faces

- [ ] Create logic to identify cards that pay with life points

- [x] drop price column due to empty data

### Phase 3: Data Loading (Saving Normalized Data)

- [x] Save edition_lookup.csv to data/clean/.

- [x] Save subtype_lookup.csv to data/clean/.

- [x] Save card_subtype_link.csv to data/clean/.

- [x] Save card_details.csv to data/clean/.

- [ ] Create local Database connection

- [ ] Load tables to database

## 2. Streamlit (Data visualisation)

- [x] Create app.py script and setup path

- [x] Implement data loading (reading all four CSV tables from data/clean).
- [x] Create home page

  - [x] show random card list
  - [x] show basic totals

- [x] Create nav

### Filtering (Finding Cards)

- [x] Create name search and filter

- [x] Create Mana Value filtering (range/slider input for CMC).
- [x] Make Super Type check filter

- [x] Make Type check box filtering.
- [x] Make subtype filtering drop down menu

- [x] Create Colour pages.
- [x] pull image of card being viewed

### Analysis (Statistical Insights)

- [x] Graph Mana Curve Visualization (Histogram).

- [x] Graph Colour Distribution Analysis (donut).

- [x] Graph Card Colour Distribution (Bar Chart).

## 3. Refinement & Testing

- [ ] Test each module for basic happy path
- [x] Create requirements.txt
- [x] Create readme

- [ ] Style the Streamlit app for a better user experience (e.g. using MTG-themed colors).

- [x] User testing of all filters and visualizations.

# Future dev ideas

- re-introduce pricing to allow people to estimate cost of build (will have to find reliable source data)
- add a save card to favourites/ new deck to help build
- properly handle rarity options e.g (foils)
- add cards text/rule to comparison page in case image does not load
- add cards power and toughness as filters

# Challenges

- multi face cards
- file pathing
- stream lit table link
- card convention consistency changes over time adding new rules
- scraping everything would require going into each card link to get all info adding over 30000 pages to scrape
- time needed
