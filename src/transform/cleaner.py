import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import re

from utils.file_utils import setup_logger, INFO

#####################
#     CONFIGURATION
#####################
# Use Pathlib to stop location errors(etl project logging_utils for ref)
# Define the project root
project_root = Path(__file__).resolve().parents[2]
# define absolute path
data_clean_path = project_root / "data" / "clean"

logger = setup_logger(__name__, "transform_data.log", level=INFO)

# --- Constants ---

COLOURS = ["W", "U", "B", "R", "G", "C"]

PREFIX_TO_REMOVE = "Cheapest Recent Printing - "

SUPER_TYPES_MTG = {
    "BASIC",
    "LEGENDARY",
    "ONGOING",
    "SNOW",
    "WORLD",
    "TRIBAL",
    "PLANE",
}


#########################
#     HELPER FUNCTIONS
#########################


def clean_types(type_str: str) -> Dict[str, str]:
    """
    reads a single MTG type string and turns it into Super_Type, Primary_Type, and a Subtypes_List.
    Uses the list of Super Types to correctly separate the super from primary and use the - to separate from sub types.

    Example: "Legendary Creature - Elf Warrior"
    -> Super_Type: "Legendary"
    -> Primary_Type: "Creature"
    -> Subtypes_List: "Elf,Warrior"
    """
    # standardise type string input
    type_str = type_str.strip()

    # create base dictionary with empty strings
    result = {"Super_Type": "", "Primary_Type": "", "Subtypes_List": ""}

    if not type_str:  # safe guard clause for empty/ null inputs
        return result

    # 1. Separate main types from subtypes at the hyphen or em-dash use regex
    dash_split = re.split(r"\s*[-—]\s*", type_str, 1)
    primary_line = dash_split[
        0
    ].strip()  # first half is the primary types with super and main type

    if len(dash_split) > 1:  # if dash exists it means there is a subtype
        raw_subtypes = dash_split[1].strip()  # second half are the sub types
        # Split subtypes by space and join with comma
        result["Subtypes_List"] = ",".join(raw_subtypes.split())

    # 2. check the primary types (e.g., "Legendary Creature")
    type_parts = primary_line.split()
    # create empty list to store results
    super_types = []
    primary_types = []

    # checks each word
    for part in type_parts:
        # Check against the super type list (case-insensitive check)
        if part.upper() in SUPER_TYPES_MTG:
            super_types.append(part)
        else:
            primary_types.append(part)

    # 3. Assemble the result fields
    # Primary Type is all non-supertype words joined by space
    result["Primary_Type"] = " ".join(primary_types)

    # Super Type is all supertype words joined by space
    result["Super_Type"] = " ".join(super_types)

    return result


def parse_mana_cost(cost_str: str) -> Dict[str, Any]:
    """
    Parses a single mana cost string into colour flags, CMC(calculated mana cost), Generic Mana value, Is_hybrid flag, and Is_X flag.
    Now handles case-insensitivity for colour symbols (e.g., 'u' vs 'U').
    """
    # create base with flags
    row_data = {"CMC": 0, "Is_Hybrid": False, "Generic_Mana": 0, "Is_X": False}
    for colour in COLOURS:
        row_data[f"Is_{colour}"] = False  # faster than writing each separately

    cost_parts = cost_str.split()  # split the string into its parts

    for part in cost_parts:
        # 1. Get the core symbol value, stripped of sym_
        symbol_value_raw = part.strip("sym_")

        # 2. standardise it to uppercase for all checks against COLOURS and flag setting
        symbol_value_normalised = symbol_value_raw.upper()

        if "/" in symbol_value_normalised:
            # --- HYBRID MANA ---
            row_data["Is_Hybrid"] = True  # assigns flag for hybrid mana

            # CMC calculation based on 'sym_2/' prefix check
            if part.startswith(
                "sym_2/"
            ):  # some hybrid may cost 2 generic and one colour - must use higher number for official cmc
                row_data["CMC"] += 2
            else:
                row_data["CMC"] += 1

            hybrid_symbols = symbol_value_normalised.split(
                "/"
            )  # take each colour in the hybrid and assign to its flag
            for symbol in hybrid_symbols:
                if symbol in COLOURS:
                    row_data[f"Is_{symbol}"] = True

        elif (
            symbol_value_normalised in COLOURS
        ):  # if no / in mana then carry on as regular mana
            # --- REGULAR COLOURED MANA ---
            row_data["CMC"] += 1  # add cmc value for each mana
            row_data[f"Is_{symbol_value_normalised}"] = True  # add to flag

        elif symbol_value_raw.isdigit():  # check for generic mana
            # --- GENERIC MANA ---
            generic_value = int(symbol_value_raw)  # turn into an integer
            row_data["CMC"] += generic_value  # add the int value to the cmc
            row_data[
                "Generic_Mana"
            ] += generic_value  # add the in value to the generic mana column

        elif (
            symbol_value_normalised == "X"
        ):  # x cost mean variable cost in official rules
            # --- X MANA (Variable) ---
            row_data["Is_X"] = True  # flag as variable - rules state it has a cmc of 0

    return row_data


# created to help multi faced cards and makes each face a unique entry
def process_card_face(
    original_row: pd.Series, type_str: str, mana_str: str, face_name: str
) -> Dict[str, Any]:
    """Combines type and mana parsing for a single card face into a new dictionary row."""

    new_row = original_row[["Name", "Edition"]].to_dict()
    new_row["Name"] = face_name

    new_row.update(clean_types(type_str))
    new_row.update(parse_mana_cost(mana_str))

    return new_row


def normalise_subtypes(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Splits the 'Subtypes_List' column into Subtype Dimension and Link Tables,
    using Card_ID for linking.
    """
    logger.info("Starting Subtype Normalization (Many-to-Many Split)...")

    subtypes_series = (
        df["Subtypes_List"].str.split(",").explode().str.strip()
    )  # split the row and create new ones for each item
    subtypes_series = subtypes_series[subtypes_series != ""]  # only use non empty rows

    # 1. Create the Subtype Lookup Table
    subtype_lookup_df = (
        subtypes_series.drop_duplicates()  # drop duplicates leaving unique subtypes
        .reset_index(drop=True)  # reset index to 0
        .to_frame(name="Subtype_Name")  # change from a series to a data frame
    )
    subtype_lookup_df["Subtype_ID"] = (
        subtype_lookup_df.index + 1
    )  # create key(unique number) for each sub type

    # 2. Create the Card Subtype Link Table
    card_subtype_link_df = df[
        ["Card_ID", "Subtypes_List"]
    ].copy()  # new dataframe with id and a copy of subtype list
    card_subtype_link_df["Subtype_Name"] = card_subtype_link_df[
        "Subtypes_List"
    ].str.split(
        ","
    )  # split the string with a comma to separate each subtype
    card_subtype_link_df = card_subtype_link_df.explode(
        "Subtype_Name"
    )  # create a new row for each subtype linking it to card id

    # clean whitespace and remove any empty rows
    card_subtype_link_df["Subtype_Name"] = card_subtype_link_df[
        "Subtype_Name"
    ].str.strip()
    card_subtype_link_df = card_subtype_link_df[
        card_subtype_link_df["Subtype_Name"] != ""
    ]
    # merge df so now subtype link table contains card id(represents card name) and sub type id(represents subtypes)
    card_subtype_link_df = card_subtype_link_df.merge(
        subtype_lookup_df, on="Subtype_Name", how="left"
    )
    # make sure there are no duplicates
    card_subtype_link_df = card_subtype_link_df[
        ["Card_ID", "Subtype_ID"]
    ].drop_duplicates()

    # 3. Drop the raw list column
    card_details_df = df.drop(columns=["Subtypes_List"])

    logger.info("Subtype Normalization complete.")
    return card_details_df, subtype_lookup_df, card_subtype_link_df


def normalise_edition(card_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Creates the Edition Table, assigns Edition_ID, and merges
    the Edition_ID (Foreign Key) back into the card details table.
    """
    logger.info("Starting Edition Normalization (Dimension Table creation)...")

    # 1. Edition Lookup Table
    edition_lookup_df = card_df[["Edition"]].drop_duplicates().reset_index(drop=True)
    edition_lookup_df.rename(columns={"Edition": "Edition_Name"}, inplace=True)

    # 2. Assign the Edition_ID (Primary Key)
    edition_lookup_df["Edition_ID"] = edition_lookup_df.index + 1

    # 3. Merge Edition_ID back into the main card_df (Foreign Key Insertion)
    card_df_with_fk = card_df.merge(
        edition_lookup_df[["Edition_Name", "Edition_ID"]],
        left_on="Edition",
        right_on="Edition_Name",
        how="left",
    )

    # 4. Final Cleanup on the card_df (remove redundant string columns)
    card_df_with_fk.drop(
        columns=["Edition", "Edition_Name"], inplace=True, errors="ignore"
    )

    # 5. Reorder columns to put Card_ID (PK) and Edition_ID (FK) up front for clarity
    cols = ["Card_ID", "Edition_ID"] + [
        col for col in card_df_with_fk.columns if col not in ["Card_ID", "Edition_ID"]
    ]
    card_df_with_fk = card_df_with_fk[cols]

    logger.info("Edition Normalization complete.")
    return card_df_with_fk, edition_lookup_df


################################
#     ORCHESTRATOR FUNCTION
################################


def run_cleaner(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Orchestrates all cleaning, transformation, and normalisation steps.
    Returns all four fully processed DataFrames: card_details, edition_lookup,
    subtype_lookup, and card_subtype_link.
    """
    logger.info("Starting data cleaning, transformation, and split card processing.")

    df = df.copy()  # use a copy as a precaution

    # 1. Edition Cleaning (Remove prefix)
    logger.info("Cleaning 'Edition' column by removing prefix.")
    df["Edition"] = (
        df["Edition"]
        .astype(str)
        .str.replace(PREFIX_TO_REMOVE, "", regex=False)
        .str.strip()
    )

    # 2. Drop Column (Price)
    df.drop(columns=["Price"], inplace=True, errors="ignore")

    # Prep columns for iteration basic checks for nulls and white space
    df["Mana Cost"] = df["Mana Cost"].fillna("").astype(str).str.strip()
    df["Type"] = df["Type"].fillna("").astype(str).str.strip()

    final_rows: List[Dict[str, Any]] = (
        []
    )  # store all processed card data ( added to help with multi face cards)

    # 3. Iterate through rows to handle multiface cards and grabs the unqiue values between the faces
    for index, row in df.iterrows():
        original_name = row["Name"]
        type_str = row["Type"]
        mana_str = row["Mana Cost"]

        if "//" in type_str:  # indicates each face
            name_parts = original_name.split("//")  # splits the values of each face
            type_parts = type_str.split("//")

            if (
                "//" in mana_str
            ):  # some cards dont use mana on one face but do on the other
                mana_parts = mana_str.split("//")
            else:
                mana_parts = [mana_str.strip(), ""]

            if not (
                len(name_parts) == len(type_parts) == 2
            ):  # keep getting issue with some cards. done a basic warning and handling until fix is found
                logger.warning(
                    f"Skipping badly formatted split card: {original_name} (Name/Type issue)"
                )
                continue

            final_rows.append(  # append each face as its own card entry
                process_card_face(
                    row,
                    type_parts[0].strip(),
                    mana_parts[0].strip(),
                    name_parts[0].strip(),
                )
            )
            final_rows.append(
                process_card_face(
                    row,
                    type_parts[1].strip(),
                    mana_parts[1].strip(),
                    name_parts[1].strip(),
                )
            )
        else:  # if standard card append
            final_rows.append(process_card_face(row, type_str, mana_str, original_name))

    # 4. Create the clean DataFrame and unique Card_ID (Primary Key)
    clean_df = pd.DataFrame(final_rows)
    clean_df.reset_index(inplace=True)
    clean_df.rename(
        columns={"index": "Card_ID"}, inplace=True
    )  # rename index to card id
    clean_df["Card_ID"] = clean_df["Card_ID"] + 1  # start card id(index) at one
    clean_df.drop(
        columns=["Mana Cost"], inplace=True, errors="ignore"
    )  # remove mana cost as it is now boolean flags

    # 5. Normalise Subtypes (returns Card_DF without raw subtypes)
    card_df_temp, subtype_lookup_df, card_subtype_link_df = normalise_subtypes(clean_df)

    # 6. Normalise Edition (creates lookup and inserts FK into Card_DF)
    final_card_df, edition_lookup_df = normalise_edition(card_df_temp)

    logger.info("Transformation complete. Returning four normalized DataFrames.")

    # Return the four final DataFrames for main.py to handle the strict Load phase
    return final_card_df, edition_lookup_df, subtype_lookup_df, card_subtype_link_df
