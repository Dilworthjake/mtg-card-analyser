import pandas as pd
import logging
import sys
import pytest
from pathlib import Path
from utils.file_utils import setup_logger

# Uses pathlib to find the project root (one level up from tests/)
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

# Set up a minimal logger for the test script itself
logger = setup_logger(__name__, "test_data.log", level=logging.INFO)

# Import the main transformation function
try:
    from src.transform.cleaner import run_cleaner
except ImportError:
    logger.error("--- CRITICAL ERROR ---")
    logger.error("Could not import 'run_cleaner' from 'src.transform.cleaner'.")
    logger.error(
        "Please ensure you have created the directory 'src/transform' and the file 'cleaner.py' inside it."
    )
    logger.error(
        "The project structure must be: mtg_etl_project/src/transform/cleaner.py"
    )
    sys.exit(1)


# Global DataFrame variables, set to None
RAW_DF = None
CARD_DETAILS_DF = None
EDITION_LOOKUP_DF = None
SUBTYPE_LOOKUP_DF = None
CARD_SUBTYPE_LINK_DF = None


def create_sample_data() -> pd.DataFrame:
    """Creates the raw sample data."""
    logger.info("Creating sample data for cleaner testing.")

    # Raw data format: [Name, Edition, Price, Type, Mana Cost]
    data = [
        # 1. Legendary Creature with Generic, White, Black mana
        {
            "Name": '"""Brims"" Barone, Midway Mobster"',
            "Mana Cost": "sym_3 sym_W sym_B",
            "Type": "Legendary Creature - Human Rogue",
            "Edition": "Cheapest Recent Printing - Unfinity",
            "Price": 0.07,
        },
        # 2. Basic Creature with single Black mana
        {
            "Name": '"""Lifetime"" Pass Holder"',
            "Mana Cost": "sym_B",
            "Type": "Creature - Zombie Guest",
            "Edition": "Cheapest Recent Printing - Unfinity",
            "Price": 0.22,
        },
        # 3. Simple Enchantment
        {
            "Name": '"""Rumors of My Death . . ."""',
            "Mana Cost": "sym_2 sym_B",
            "Type": "Enchantment",
            "Edition": "Cheapest Recent Printing - Unstable",
            "Price": 0.10,
        },
        # 4. Artifact Equipment with Generic and White mana, single subtype
        {
            "Name": "+2 Mace",
            "Mana Cost": "sym_1 sym_W",
            "Type": "Artifact - Equipment",
            "Edition": "Cheapest Recent Printing - Adventures in the Forgotten Realms",
            "Price": 0.05,
        },
        # 5. Scheme (Archenemy card) - typically no mana cost
        {
            "Name": "A Display of My Dark Power",
            "Mana Cost": "",
            "Type": "Scheme",
            "Edition": "Cheapest Recent Printing - Archenemy",
            "Price": 22.95,
        },
    ]
    df = pd.DataFrame(data)

    # Basic structural check
    expected_input_cols = ["Name", "Mana Cost", "Type", "Edition", "Price"]
    assert sorted(df.columns.tolist()) == sorted(
        expected_input_cols
    ), f"Raw data input columns are incorrect. Found: {df.columns.tolist()}"
    logger.info(
        f"Raw input DataFrame has {len(df.columns)} columns (Scraper check passed)."
    )

    return df


def validate_schema(df: pd.DataFrame, expected_schema: list, title: str):
    """Helper function to validate the columns and order of a DataFrame."""
    actual_columns = list(df.columns)
    assert (
        actual_columns == expected_schema
    ), f"Schema validation failed for {title}.\nExpected: {expected_schema}\nActual: {actual_columns}"
    logger.info(f"Schema validation passed for {title}.")
    print(f"Schema for {title} is correct.")


def check_card_attributes(df: pd.DataFrame, name: str, expected: dict):
    """Helper function to check a single card's attributes in the Card Details table."""
    card = df[df["Name"] == name]
    assert len(card) == 1, f"Failed to find unique row for card: {name}"

    for key, expected_value in expected.items():
        actual_value = card.iloc[0][key]
        assert (
            actual_value == expected_value
        ), f"Validation failed for {name} on column '{key}'. Expected: {expected_value}, Actual: {actual_value}"
    logger.info(f"Successfully validated transformation for: {name}")


logger.info("Running initial data setup and transformation...")
RAW_DF = create_sample_data()
CARD_DETAILS_DF, EDITION_LOOKUP_DF, SUBTYPE_LOOKUP_DF, CARD_SUBTYPE_LINK_DF = (
    run_cleaner(RAW_DF)
)
logger.info("Data setup complete. Ready to run tests.")


# --------------------------------------------------------------------------
# MODULAR TEST FUNCTIONS
# --------------------------------------------------------------------------


def test_schema_integrity():
    """Test 1: Check the structure (columns and order) of all four resulting tables."""
    logger.info("--- Running Schema Integrity Test ---")

    # Expected Schemas (including the exact order)
    card_details_schema = [
        "Card_ID",
        "Edition_ID",
        "Name",
        "Super_Type",
        "Primary_Type",
        "CMC",
        "Is_Hybrid",
        "Generic_Mana",
        "Is_X",
        "Is_W",
        "Is_U",
        "Is_B",
        "Is_R",
        "Is_G",
        "Is_C",
    ]
    card_subtype_link_schema = ["Card_ID", "Subtype_ID"]
    edition_lookup_schema = ["Edition_Name", "Edition_ID"]
    subtype_lookup_schema = ["Subtype_Name", "Subtype_ID"]

    validate_schema(
        CARD_DETAILS_DF, card_details_schema, "1. Card Details (Fact Table)"
    )
    validate_schema(
        EDITION_LOOKUP_DF, edition_lookup_schema, "2. Edition Lookup (Dimension Table)"
    )
    validate_schema(
        SUBTYPE_LOOKUP_DF, subtype_lookup_schema, "3. Subtype Lookup (Dimension Table)"
    )
    validate_schema(
        CARD_SUBTYPE_LINK_DF,
        card_subtype_link_schema,
        "4. Card-Subtype Link (Many-to-Many)",
    )


def test_card_1_brims_barone():
    """Test 2: Validate the Legendary Creature card 'Brims Barone'."""
    logger.info("--- Running Card 1: Legendary Creature Test ---")
    check_card_attributes(
        CARD_DETAILS_DF,
        '"""Brims"" Barone, Midway Mobster"',
        {
            "Super_Type": "Legendary",
            "Primary_Type": "Creature",
            "CMC": 5,
            "Generic_Mana": 3,
            "Is_W": True,
            "Is_B": True,
            "Is_U": False,
            "Is_R": False,
            "Is_G": False,
            "Is_C": False,
        },
    )


def test_card_2_lifetime_pass_holder():
    """Test 3: Validate the basic creature card 'Lifetime Pass Holder'."""
    logger.info("--- Running Card 2: Basic Creature Test ---")
    check_card_attributes(
        CARD_DETAILS_DF,
        '"""Lifetime"" Pass Holder"',
        {
            "Super_Type": "",
            "Primary_Type": "Creature",
            "CMC": 1,
            "Generic_Mana": 0,
            "Is_B": True,
            "Is_W": False,
        },
    )


def test_card_3_rumors_of_my_death():
    """Test 4: Validate the simple Enchantment card 'Rumors of My Death...'."""
    logger.info("--- Running Card 3: Enchantment Test ---")
    check_card_attributes(
        CARD_DETAILS_DF,
        '"""Rumors of My Death . . ."""',
        {
            "Super_Type": "",
            "Primary_Type": "Enchantment",
            "CMC": 3,
            "Generic_Mana": 2,
            "Is_B": True,
        },
    )


def test_card_4_plus_two_mace():
    """Test 5: Validate the Artifact Equipment card '+2 Mace'."""
    logger.info("--- Running Card 4: Artifact Test ---")
    check_card_attributes(
        CARD_DETAILS_DF,
        "+2 Mace",
        {
            "Super_Type": "",
            "Primary_Type": "Artifact",
            "CMC": 2,
            "Generic_Mana": 1,
            "Is_W": True,
        },
    )


def test_card_5_dark_power_scheme():
    """Test 6: Validate the special type (Scheme) card with no mana cost."""
    logger.info("--- Running Card 5: Scheme/No Mana Test ---")
    check_card_attributes(
        CARD_DETAILS_DF,
        "A Display of My Dark Power",
        {
            "Super_Type": "",
            "Primary_Type": "Scheme",
            "CMC": 0,  # Should be 0 when Mana Cost is empty
            "Generic_Mana": 0,
            "Is_W": False,
            "Is_B": False,
        },
    )


def test_lookup_table_content():
    """Test 7: Check that required edition and subtype entries exist in lookups."""
    logger.info("--- Running Lookup Table Content Test ---")

    # Check Edition Cleaning
    unfinity_row = EDITION_LOOKUP_DF[EDITION_LOOKUP_DF["Edition_Name"] == "Unfinity"]
    assert len(unfinity_row) == 1, "Edition cleaning failed. 'Unfinity' not found."
    logger.info("Successfully validated Edition Cleaning (Unfinity).")

    # Check Subtypes exist
    required_subtypes = ["Human", "Rogue", "Zombie", "Guest", "Equipment"]
    for subtype in required_subtypes:
        row = SUBTYPE_LOOKUP_DF[SUBTYPE_LOOKUP_DF["Subtype_Name"] == subtype]
        assert len(row) == 1, f"Subtype '{subtype}' not found in Lookup table."
        logger.info(f"Subtype '{subtype}' found successfully.")


def test_linking_table_integrity():
    """Test 8: Check a specific link (e.g., +2 Mace -> Equipment) exists."""
    logger.info("--- Running Linking Table Integrity Test ---")

    # Find IDs for +2 Mace and Equipment
    mace_row = CARD_DETAILS_DF[CARD_DETAILS_DF["Name"] == "+2 Mace"]
    assert not mace_row.empty, "Card ID for +2 Mace not found for linking test."
    mace_id = mace_row.iloc[0]["Card_ID"]

    equipment_row = SUBTYPE_LOOKUP_DF[SUBTYPE_LOOKUP_DF["Subtype_Name"] == "Equipment"]
    assert (
        not equipment_row.empty
    ), "Subtype ID for Equipment not found for linking test."
    equipment_id = equipment_row.iloc[0]["Subtype_ID"]

    # Check the link exists in the link table
    link_found = CARD_SUBTYPE_LINK_DF[
        (CARD_SUBTYPE_LINK_DF["Card_ID"] == mace_id)
        & (CARD_SUBTYPE_LINK_DF["Subtype_ID"] == equipment_id)
    ]
    assert len(link_found) == 1, "Subtype link check failed for +2 Mace (Equipment)."
    logger.info("Successfully validated subtype linking for +2 Mace (Equipment).")


if __name__ == "__main__":
    # If run directly (not via a test runner like pytest), we execute the tests sequentially
    print("\nExecuting tests directly (not using pytest's discovery mode).")
    try:
        test_schema_integrity()
        test_card_1_brims_barone()
        test_card_2_lifetime_pass_holder()
        test_card_3_rumors_of_my_death()
        test_card_4_plus_two_mace()
        test_card_5_dark_power_scheme()
        test_lookup_table_content()
        test_linking_table_integrity()
        logger.info("\n*** SUCCESS: All 8 modular tests passed successfully! ***")
    except AssertionError as e:
        logger.error(f"\n*** FAILURE: A test failed. ***\nDetails: {e}")
