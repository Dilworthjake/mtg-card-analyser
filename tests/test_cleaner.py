import pandas as pd
import pytest
from src.transform.cleaner import (
    clean_types,
    parse_mana_cost,
    process_card_face,
    run_cleaner,
)

##################################
# --- Helper Function Tests ---
##################################

#################
# --- types ---
#################


def test_clean_types_happy_path_all_present():
    # Tests a type string with Super, Primary, and Subtypes.
    type_str = "Legendary Creature - Elf Warrior"
    expected = {
        "Super_Type": "Legendary",
        "Primary_Type": "Creature",
        "Subtypes_List": "Elf,Warrior",
    }
    assert clean_types(type_str) == expected


def test_clean_types_no_supertype():
    # Tests a type string with Primary and Subtypes but no Supertype.
    type_str = "Sorcery - Arcane"
    expected = {
        "Super_Type": "",
        "Primary_Type": "Sorcery",
        "Subtypes_List": "Arcane",
    }
    assert clean_types(type_str) == expected


def test_clean_types_only_primary():
    # Tests a type string with only a Primary type.
    type_str = "Instant"
    expected = {
        "Super_Type": "",
        "Primary_Type": "Instant",
        "Subtypes_List": "",
    }
    assert clean_types(type_str) == expected


def test_clean_types_with_em_dash():
    # Tests splitting using an em-dash instead of a hyphen.
    type_str = "Artifact — Equipment"
    expected = {
        "Super_Type": "",
        "Primary_Type": "Artifact",
        "Subtypes_List": "Equipment",
    }
    assert clean_types(type_str) == expected


################
# --- mana ---
################


def test_parse_mana_cost_generic_and_coloured():
    # Tests a standard mana cost (e.g., 3WB).
    cost_str = "sym_3 sym_w sym_b"
    expected = {
        "CMC": 5,
        "Is_Hybrid": False,
        "Generic_Mana": 3,
        "Is_X": False,
        "Is_W": True,
        "Is_U": False,
        "Is_B": True,
        "Is_R": False,
        "Is_G": False,
        "Is_C": False,
    }
    assert parse_mana_cost(cost_str) == expected


def test_parse_mana_cost_hybrid_and_generic():
    # Tests a cost including a hybrid symbol (e.g., 2/U).
    cost_str = "sym_2 sym_2/u"
    expected = {
        "CMC": 4,
        "Is_Hybrid": True,
        "Generic_Mana": 2,
        "Is_X": False,
        "Is_W": False,
        "Is_U": True,
        "Is_B": False,
        "Is_R": False,
        "Is_G": False,
        "Is_C": False,
    }
    assert parse_mana_cost(cost_str) == expected


def test_parse_mana_cost_x_and_coloured():
    # Tests a cost including X (variable) mana.
    cost_str = "sym_x sym_r"
    expected = {
        "CMC": 1,
        "Is_Hybrid": False,
        "Generic_Mana": 0,
        "Is_X": True,
        "Is_W": False,
        "Is_U": False,
        "Is_B": False,
        "Is_R": True,
        "Is_G": False,
        "Is_C": False,
    }
    assert parse_mana_cost(cost_str) == expected


def test_parse_mana_cost_empty():
    # Tests an empty mana cost string (e.g., for Lands).
    cost_str = ""
    expected = {
        "CMC": 0,
        "Is_Hybrid": False,
        "Generic_Mana": 0,
        "Is_X": False,
        "Is_W": False,
        "Is_U": False,
        "Is_B": False,
        "Is_R": False,
        "Is_G": False,
        "Is_C": False,
    }
    assert parse_mana_cost(cost_str) == expected


######################
# --- face test ---
######################


def test_process_card_face_happy_path():
    # Tests the combination of type and mana parsing for a single face.
    # Create a mock Series for the original row
    mock_row = pd.Series({"Name": "Example Card", "Edition": "Example Set"})
    type_str = "Basic Land - Mountain"
    mana_str = "sym_c"
    face_name = "Example Card Face A"

    result = process_card_face(mock_row, type_str, mana_str, face_name)

    assert result["Name"] == face_name
    assert result["Super_Type"] == "Basic"
    assert result["Primary_Type"] == "Land"
    assert result["Subtypes_List"] == "Mountain"
    assert result["CMC"] == 1
    assert result["Is_C"] == True


#############################
# --- Orchestrator Test ---
#############################


@pytest.fixture
def sample_raw_df() -> pd.DataFrame:
    # Fixture for a sample input DataFrame including a standard and a split card.
    data = [
        {
            "Name": "Standard Card",
            "Edition": "Cheapest Recent Printing - Modern Horizons",
            "Price": 1.5,
            "Type": "Legendary Creature - Goblin Warrior",
            "Mana Cost": "sym_2 sym_r",
        },
        {
            "Name": "Split Face A // Split Face B",
            "Edition": "Cheapest Recent Printing - Adventures in the Forgotten Realms",
            "Price": 3.0,
            "Type": "Instant // Sorcery",
            "Mana Cost": "sym_u // sym_2 sym_g",
        },
    ]
    # NOTE: Card_ID is made from the index during testing.
    return pd.DataFrame(data)


def test_run_cleaner_happy_path_structure_and_data(sample_raw_df):
    """
    Tests the main ETL orchestrator for correct structure, keys, and normalized data.
    """
    (
        card_df,
        edition_df,
        subtype_df,
        link_df,
    ) = run_cleaner(sample_raw_df)

    # 1. Card Details (Final Fact/Dimension Table)
    # The split card (1 raw row) becomes 2 rows, plus the standard card (1 row) = 3 rows total.
    assert len(card_df) == 3
    assert "Card_ID" in card_df.columns
    assert "Edition_ID" in card_df.columns
    assert card_df["Card_ID"].is_unique
    assert card_df["Card_ID"].min() == 1  # Check PK starts at 1
    assert "Edition" not in card_df.columns  # Check raw edition column is removed

    # Check data for the Standard Card
    standard_card = card_df[card_df["Name"] == "Standard Card"].iloc[0]
    assert standard_card["Super_Type"] == "Legendary"
    assert standard_card["Primary_Type"] == "Creature"
    assert standard_card["CMC"] == 3
    assert standard_card["Is_R"] == True
    assert standard_card["Edition_ID"] == 1  # Check FK assignment

    # Check data for Split Face A
    split_card_a = card_df[card_df["Name"] == "Split Face A"].iloc[0]
    assert split_card_a["Primary_Type"] == "Instant"
    assert split_card_a["CMC"] == 1
    assert split_card_a["Is_U"] == True
    assert split_card_a["Edition_ID"] == 2  # Check FK assignment

    # 2. Edition Lookup Table
    assert len(edition_df) == 2  # 2 unique editions
    assert list(edition_df["Edition_Name"]) == [
        "Modern Horizons",
        "Adventures in the Forgotten Realms",
    ]
    assert "Edition_ID" in edition_df.columns
    assert edition_df["Edition_ID"].is_unique

    # 3. Subtype Lookup Table
    # Expected Subtypes: Goblin, Warrior, Sorcery, Instant (should only get Goblin, Warrior for the link table test)
    # The two Primary Types (Instant, Sorcery) are NOT subtypes, they shouldn't be here.
    # The raw data only has subtypes for the first card.
    assert len(subtype_df) == 2  # Goblin, Warrior
    assert "Subtype_ID" in subtype_df.columns
    assert subtype_df["Subtype_ID"].is_unique
    assert sorted(subtype_df["Subtype_Name"].tolist()) == ["Goblin", "Warrior"]

    # 4. Card Subtype Link Table
    # Only the standard card (Card_ID 1) has subtypes.
    # Card_ID 1 should link to 'Goblin' (Subtype_ID 1) and 'Warrior' (Subtype_ID 2)
    assert len(link_df) == 2
    assert link_df.columns.tolist() == ["Card_ID", "Subtype_ID"]
    # Check the links are correct (assuming Card_ID 1, Subtype_ID 1 and 2)
    expected_links = set([(1, 1), (1, 2)])
    actual_links = set(zip(link_df["Card_ID"].tolist(), link_df["Subtype_ID"].tolist()))
    assert actual_links == expected_links
