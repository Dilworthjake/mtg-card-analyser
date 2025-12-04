import streamlit as st
import pandas as pd
import requests
import time
from pathlib import Path
from typing import Optional, List
import plotly.express as px

# --- CONFIGURATION & FILE PATHS ---
DATA_DIR_NAME = "data/clean"
DATA_DIR = Path(DATA_DIR_NAME)

CARD_DETAILS_FILE = DATA_DIR / "card_details.csv"
EDITION_LOOKUP_FILE = DATA_DIR / "edition_lookup.csv"
SUBTYPE_LOOKUP_FILE = DATA_DIR / "subtype_lookup.csv"
CARD_SUBTYPE_LINK_FILE = DATA_DIR / "card_subtype_link.csv"

# Scryfall API Endpoint
SCRYFALL_API_BASE = "https://api.scryfall.com/cards/named"

# Pagination Constant
PAGE_SIZE = 15

# Colour constants for charts and themes
# Defines the colour flag columns in the DataFrame
COLOUR_COLUMNS = {
    "White": "Is_W",
    "Blue": "Is_U",
    "Black": "Is_B",
    "Red": "Is_R",
    "Green": "Is_G",
    "Colourless": "Is_C",
}
# Defines chart colours, including the new 'Colourless' entry
MTG_CHART_COLOURS = {
    "White": "#F8F3D7",
    "Blue": "#C1D3EF",
    "Black": "#B0ADAD",
    "Red": "#EC9681",
    "Green": "#B1C7A8",
    "Colourless": "#A8A8A8",  # Neutral colour for colourless
}
# Defines page background themes
THEME_COLOURS = {
    "White": "#F0F0E0",
    "Blue": "#D0E0F8",
    "Black": "#D8D8D8",
    "Red": "#F8D0D0",
    "Green": "#D0F8D0",
    "Colourless": "#E8E8E8",
    "Compare": "#F5F5F5",
    "Generic": "#F5F5F5",
    "Home": "#FFFFFF",
}


# --- DATA LOADING FUNCTION ---
@st.cache_data
def load_and_join_all_data() -> Optional[pd.DataFrame]:
    """
    Loads all normalised CSV files and performs the necessary joins
    to create a single denormalized DataFrame for the app.
    """
    try:
        card_df = pd.read_csv(CARD_DETAILS_FILE)
        edition_df = pd.read_csv(EDITION_LOOKUP_FILE)
        subtype_df = pd.read_csv(SUBTYPE_LOOKUP_FILE)
        link_df = pd.read_csv(CARD_SUBTYPE_LINK_FILE)
    except FileNotFoundError as e:
        st.error(f"ERROR: Required file not found: {e}. Run main.py first.")
        return None

    # Join Editions
    final_df = card_df.merge(edition_df, on="Edition_ID", how="left").drop(
        columns=["Edition_ID"]
    )

    # Denormalize Subtypes
    linked_subtypes = link_df.merge(subtype_df, on="Subtype_ID", how="left")
    subtype_groups = (
        linked_subtypes.groupby("Card_ID")["Subtype_Name"]
        .apply(lambda x: ", ".join(sorted(x.astype(str))))
        .reset_index()
    )
    subtype_groups.rename(columns={"Subtype_Name": "All_Subtypes"}, inplace=True)

    final_df = final_df.merge(subtype_groups, on="Card_ID", how="left")

    # Fill NAs
    final_df["All_Subtypes"] = final_df["All_Subtypes"].fillna("")
    final_df["Super_Type"] = final_df["Super_Type"].fillna("")

    # Type Correction
    bool_cols = [col for col in final_df.columns if col.startswith("Is_")]
    for col in bool_cols:
        final_df[col] = final_df[col].astype(bool)

    # Ensure CMC is treated as an integer for charting
    final_df["CMC"] = final_df["CMC"].fillna(-1).astype(int)

    return final_df


# --- SCRYFALL IMAGE FETCHING ---


@st.cache_data(ttl=3600 * 24)  # Cache for 24 hours to reduce API calls
def get_scryfall_image_url(card_name: str) -> Optional[str]:
    """
    Fetches the image URL for a card from the Scryfall API with exponential backoff.
    Returns the 'normal' quality image URL, or None on failure/not found.
    """
    if not card_name:
        return None

    params = {"exact": card_name}
    max_retries = 5
    delay = 1  # Initial delay in seconds

    for attempt in range(max_retries):
        try:
            # Scryfall requests users to wait at least 50-100ms between requests.
            if attempt > 0:
                time.sleep(delay)
                delay *= 2  # Exponential backoff

            response = requests.get(SCRYFALL_API_BASE, params=params, timeout=5)

            if response.status_code == 200:
                data = response.json()

                # Check for transforming cards (e.g., dual-faced cards)
                if "card_faces" in data:
                    # Usually, the front face image is in the first element
                    return data["card_faces"][0]["image_uris"]["normal"]
                else:
                    return data["image_uris"]["normal"]

            elif response.status_code == 404:
                # Card not found
                return None

            # 429 Too Many Requests, or 5xx server errors, retry
            elif response.status_code in (429, 500, 502, 503, 504):
                if attempt < max_retries - 1:
                    st.warning(
                        f"Rate limit or server error ({response.status_code}). Retrying in {delay}s..."
                    )
                    continue
                else:
                    st.error(
                        "Max retries reached. Failed to fetch image from Scryfall."
                    )
                    return None
            else:
                # Handle other client errors (e.g., 400 Bad Request)
                return None

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                st.warning(f"Connection error: {e}. Retrying in {delay}s...")
                continue
            else:
                st.error(f"Failed to connect to Scryfall API after multiple retries.")
                return None
    return None


# --- THEME/COLOUR HELPER ---
def set_page_theme(page_title: str):
    """Injects CSS to set the application background colour based on the MTG colour theme."""
    selected_colour = THEME_COLOURS.get(page_title, "#FFFFFF")

    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {selected_colour};
            transition: background-color 0.5s;
        }}
        .main .block-container {{
            padding-top: 1rem;
            padding-bottom: 1rem;
        }}
        /* Custom style to make the card name button look like a text link */
        div.stButton > button {{
            background-color: transparent;
            border: none;
            color: #1E90FF; /* Dodger Blue for link appearance */
            padding: 0;
            margin: 0;
            text-align: left;
            text-decoration: underline;
            font-size: 1rem;
            white-space: normal; 
            text-overflow: clip; 
        }}
        div.stButton > button:hover {{
            color: #0056b3; /* Darker blue on hover */
        }}
        /* General style to improve image appearance */
        .stImage > img {{
            border-radius: 8px;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# --- HELPER: CARD DATA EXTRACTION ---
def get_card_info(df: pd.DataFrame, card_name: str) -> Optional[pd.Series]:
    """Retrieves all data for a single card by name."""
    if not card_name:
        return None
    card_data = df[df["Name"] == card_name]
    if not card_data.empty:
        # Use reset_index(drop=True) to ensure iloc[0] refers to the first row of the filtered result
        return card_data.reset_index(drop=True).iloc[0]
    return None


# --- HELPER: CHARTS  ---
def render_colour_distribution_chart(df: pd.DataFrame):
    """
    Renders the Colour Distribution Analysis Chart (Global)
    """
    st.subheader("Colour Identity Breakdown (Global)")

    colour_data = []
    colour_cols_keys = list(COLOUR_COLUMNS.values())

    # 1. Monocolour/Multicolour Counts (W, U, B, R, G, C)
    for name, col_key in COLOUR_COLUMNS.items():
        count = df[col_key].sum()
        if count > 0:
            colour_data.append({"Colour_Name": name, "Count": count})

    # 2. Generic Count
    # Card is Generic if ALL colour flags are False (i.e., not W, U, B, R, G, or C )
    generic_mask = (~df[colour_cols_keys]).all(axis=1)
    generic_count = generic_mask.sum()
    if generic_count > 0:
        colour_data.append({"Colour_Name": "Generic", "Count": generic_count})

    colour_counts = pd.DataFrame(colour_data)

    if colour_counts.empty:
        st.info("No colour data to display.")
        return

    fig_colour = px.bar(
        colour_counts,
        x="Colour_Name",
        y="Count",
        title="Card Count by Colour Identity",
        color="Colour_Name",
        color_discrete_map=MTG_CHART_COLOURS,
    )
    fig_colour.update_layout(
        xaxis_title="Colour", yaxis_title="Number of Cards", showlegend=False
    )
    st.plotly_chart(fig_colour, use_container_width=True)


# --- NAVIGATION & INTERACTION CALLBACKS ---


def update_page_state_from_radio():
    """Updates the current_page state when the user clicks the radio button and resets page_number."""
    st.session_state["current_page"] = st.session_state["page_radio"]
    # Reset pagination when switching pages
    st.session_state["page_number"] = 0


def handle_card_name_click(card_name: str):
    """Callback function to handle button click and trigger navigation."""
    st.session_state["compare_card_1"] = card_name
    st.session_state["current_page"] = "Card Compare"


def go_next_page():
    """Increments the current page number."""
    st.session_state["page_number"] += 1


def go_prev_page():
    """Decrements the current page number."""
    st.session_state["page_number"] -= 1


# --- CARD LIST RENDERING FUNCTIONS ---


def render_card_list_with_buttons(df_to_display: pd.DataFrame, list_title: str):
    """Renders the card list using st.columns and st.button for reliable click-to-navigate."""

    # Column definitions
    col_ratios = [0.275, 0.075, 0.15, 0.15, 0.35]

    # Header row
    col_name, col_cmc, col_type, col_edition, col_subtype = st.columns(col_ratios)
    col_name.markdown("**Name**")
    col_cmc.markdown("**CMC**")
    col_type.markdown("**Type**")
    col_edition.markdown("**Edition**")
    col_subtype.markdown("**Subtypes**")

    st.markdown("---")  # Visual separator

    if df_to_display.empty:
        return

    # Data rows
    for index, row in df_to_display.iterrows():
        # Create columns for the row
        col_name, col_cmc, col_type, col_edition, col_subtype = st.columns(col_ratios)

        # 1. Card Name (The clickable button)
        card_name = row["Name"]
        # Use a unique key for each button
        # Use row index from the *original* DataFrame slice for a truly unique key
        button_key = f"compare_btn_{card_name}_{index}"

        with col_name:
            if st.button(
                card_name,
                key=button_key,
                on_click=handle_card_name_click,
                args=(card_name,),
            ):
                pass

        # 2. Other Card Details (Display only)
        col_cmc.markdown(f"**{row.get('CMC', 'N/A')}**")
        col_type.markdown(str(row.get("Primary_Type", "N/A")))
        col_edition.markdown(str(row.get("Edition_Name", "N/A")))

        # Truncate subtypes for better column fit
        subtypes = str(row.get("All_Subtypes", ""))
        col_subtype.markdown(subtypes if len(subtypes) < 30 else f"{subtypes[:27]}...")

        # Add a light separator between rows
        st.markdown(
            '<hr style="margin: 4px 0; border-top: 1px solid #eee;">',
            unsafe_allow_html=True,
        )


def render_paginated_card_list(full_df: pd.DataFrame, list_title: str):
    """Applies pagination and renders the card list with navigation controls."""

    st.subheader(f"{list_title} (Click Name to Compare)")

    total_cards = len(full_df)

    if total_cards == 0:
        st.info("No cards match the current selection.")
        return

    total_pages = (total_cards + PAGE_SIZE - 1) // PAGE_SIZE  # Ceiling division
    current_page = st.session_state.get("page_number", 0)

    # Ensure current_page is within bounds
    if total_pages > 0:
        current_page = max(0, min(current_page, total_pages - 1))
        st.session_state["page_number"] = current_page

    # Calculate slice indices
    start_index = current_page * PAGE_SIZE
    end_index = min(start_index + PAGE_SIZE, total_cards)

    # Slice the DataFrame
    df_to_display = full_df.iloc[start_index:end_index]

    # --- Pagination Controls ---
    if total_pages > 1:
        nav_col1, nav_col2, nav_col3 = st.columns([1, 2, 1])

        with nav_col1:
            st.button(
                "Previous 15",
                on_click=go_prev_page,
                disabled=(current_page == 0),
                use_container_width=True,
                key=f"{list_title}_prev_btn",
            )

        with nav_col2:
            st.markdown(
                f"<div style='text-align: center; margin-top: 0.5rem;'>Page {current_page + 1} of {total_pages}</div>",
                unsafe_allow_html=True,
            )

        with nav_col3:
            st.button(
                "Next 15",
                on_click=go_next_page,
                disabled=(current_page == total_pages - 1),
                use_container_width=True,
                key=f"{list_title}_next_btn",
            )

    # --- Render the Card List ---
    render_card_list_with_buttons(df_to_display, list_title)


# --- PAGE RENDERING FUNCTIONS ---


def render_compare_page(df: pd.DataFrame):
    """
    Renders the Card Comparison page, including Scryfall images.
    FIXED: Replaced deprecated use_column_width with use_container_width.
    """
    st.title("⚖️ Card Compare")
    st.markdown(
        "Select up to two cards to compare their stats side-by-side. Images are loaded from Scryfall."
    )

    all_card_names = sorted(df["Name"].unique().tolist())

    # Initialize comparison card states if they don't exist
    if "compare_card_1" not in st.session_state:
        st.session_state["compare_card_1"] = None
    if "compare_card_2" not in st.session_state:
        st.session_state["compare_card_2"] = None

    # --- Card Selection ---
    col_select1, col_select2 = st.columns(2)

    with col_select1:
        st.subheader("Card 1")
        card_1_name = st.selectbox(
            "Select Card 1",
            options=[None] + all_card_names,
            key="select_card_1",
            # Set the index based on the session state value
            index=(
                ([None] + all_card_names).index(st.session_state.compare_card_1)
                if st.session_state.compare_card_1 in ([None] + all_card_names)
                else 0
            ),
        )
        # Update session state for future selection/clicks (in case of manual selection)
        st.session_state.compare_card_1 = card_1_name
        card_1_info = get_card_info(df, card_1_name)

    with col_select2:
        st.subheader("Card 2")
        card_2_name = st.selectbox(
            "Select Card 2",
            options=[None] + all_card_names,
            key="select_card_2",
            index=(
                ([None] + all_card_names).index(st.session_state.compare_card_2)
                if st.session_state.compare_card_2 in ([None] + all_card_names)
                else 0
            ),
        )
        st.session_state.compare_card_2 = card_2_name
        card_2_info = get_card_info(df, card_2_name)

    st.markdown("---")

    # --- Image and Text Display ---
    img_col1, text_col1, img_col2, text_col2 = st.columns([1, 1, 1, 1])

    # Card 1 Display
    if card_1_name and card_1_info is not None:
        card_1_url = get_scryfall_image_url(card_1_name)
        with img_col1:
            if card_1_url:
                st.image(
                    card_1_url,
                    caption=card_1_name,
                    # FIXED: Use use_container_width
                    use_container_width=True,
                    output_format="JPEG",
                )
            else:
                st.info("Image not found on Scryfall.")
        with text_col1:
            st.markdown(f"**Card Text ({card_1_name}):**")
            st.caption(card_1_info.get("Text", "No card text available."))
    else:
        with img_col1:
            st.info("Select Card 1.")

    # Card 2 Display
    if card_2_name and card_2_info is not None:
        card_2_url = get_scryfall_image_url(card_2_name)
        with img_col2:
            if card_2_url:
                st.image(
                    card_2_url,
                    caption=card_2_name,
                    # FIXED: Use use_container_width
                    use_container_width=True,
                    output_format="JPEG",
                )
            else:
                st.info("Image not found on Scryfall.")
        with text_col2:
            st.markdown(f"**Card Text ({card_2_name}):**")
            st.caption(card_2_info.get("Text", "No card text available."))
    else:
        with img_col2:
            st.info("Select Card 2.")

    st.markdown("---")

    # --- Comparison Table ---
    if card_1_info is None and card_2_info is None:
        return

    # Dynamic column selection to prevent KeyError (e.g., if 'Rarity' is missing in the data)
    required_fields = [
        "Edition_Name",
        "CMC",
        "Super_Type",
        "Primary_Type",
        "All_Subtypes",
        "Generic_Mana",
    ]
    available_cols = df.columns.tolist()
    comparison_fields = [f for f in required_fields if f in available_cols]

    data_to_compare = []

    # Prepare data for Card 1
    if card_1_info is not None:
        # Using .get() for safer access to individual attributes
        data_1 = {field: card_1_info.get(field, "N/A") for field in comparison_fields}
        data_1["Colour Identity"] = ", ".join(
            [c for c, col in COLOUR_COLUMNS.items() if card_1_info.get(col, False)]
        )
        data_to_compare.append(data_1)

    # Prepare data for Card 2
    if card_2_info is not None:
        data_2 = {field: card_2_info.get(field, "N/A") for field in comparison_fields}
        data_2["Colour Identity"] = ", ".join(
            [c for c, col in COLOUR_COLUMNS.items() if card_2_info.get(col, False)]
        )
        data_to_compare.append(data_2)

    if data_to_compare:
        # Transpose the DataFrame for a vertical attribute list
        comparison_df = pd.DataFrame(data_to_compare).T
        comparison_df.columns = [
            card_1_name if card_1_name else "Card 1",
            card_2_name if card_2_name else "Card 2",
        ]
        comparison_df.index.name = "Attribute"

        st.subheader("Card Statistics")
        st.table(comparison_df)


def render_colour_page(df: pd.DataFrame, colour_col: str, page_title: str, emoji: str):
    """
    Renders a specific colour page with its own unique filters and analysis charts.
    """
    st.header(f"{emoji} {page_title} Card Analyser")

    # Flag to reset pagination if filters change (since filtering triggers a rerun)
    filter_state_changed = False

    # 1. Initial Filter:
    colour_cols_keys = list(COLOUR_COLUMNS.values())
    if page_title == "Generic":
        # Card is Generic if all colour flags are False
        page_df = df[(~df[colour_cols_keys]).all(axis=1)].copy()
    else:
        # Data is restricted to cards containing the selected page colour
        page_df = df[df[colour_col] == True].copy()

    # --- DIAGNOSTIC CHECK ---
    if page_df.empty:
        st.warning(
            f"No cards found for the initial filter '{page_title}'. "
            f"Please ensure your raw data file has cards associated with this filter."
        )
        return

    # --- SIDEBAR FILTERS ---
    st.sidebar.markdown("---")
    st.sidebar.subheader(f"🛠️ Filter {page_title} Cards")

    # A. Name Search Filter
    search_term = (
        st.sidebar.text_input(
            "Search Card Name (Partial Match)", "", key=f"{page_title}_search"
        )
        .strip()
        .lower()
    )
    if st.session_state.get(f"{page_title}_search_prev") != search_term:
        st.session_state[f"{page_title}_search_prev"] = search_term
        filter_state_changed = True

    # B. CMC Slider (Range)
    safe_cmc = page_df["CMC"][page_df["CMC"] != -1]

    if safe_cmc.empty:
        min_slider, max_slider = 0, 10
        cmc_slider_disabled = True
    else:
        min_cmc = int(safe_cmc.min())
        max_cmc = int(safe_cmc.max())
        min_slider = min_cmc
        max_slider = max_cmc
        if min_cmc == max_cmc:
            max_slider = max_cmc + 1 if max_cmc < 10 else max_cmc
        cmc_slider_disabled = False

    # Get state values for initialization, otherwise use full range
    default_min = st.session_state.get(f"{page_title}_cmc_min", min_slider)
    default_max = st.session_state.get(f"{page_title}_cmc_max", max_slider)
    # Ensure defaults are within the bounds of the current page_df
    default_min = max(min_slider, min(default_min, max_slider))
    default_max = min(max_slider, max(default_max, min_slider))

    cmc_range = st.sidebar.slider(
        "Mana Value (CMC) Range",
        min_value=min_slider,
        max_value=max_slider,
        value=(default_min, default_max),
        step=1,
        disabled=cmc_slider_disabled,
        key=f"{page_title}_cmc_range",
    )
    if (
        st.session_state.get(f"{page_title}_cmc_min") != cmc_range[0]
        or st.session_state.get(f"{page_title}_cmc_max") != cmc_range[1]
    ):
        st.session_state[f"{page_title}_cmc_min"] = cmc_range[0]
        st.session_state[f"{page_title}_cmc_max"] = cmc_range[1]
        filter_state_changed = True

    # C. Primary Type Filter
    all_types_list = (
        page_df["Primary_Type"]
        .dropna()
        .str.split(" ")
        .explode()
        .str.strip()
        .unique()
        .tolist()
    )
    all_types = sorted([t for t in all_types_list if t])

    selected_types = st.sidebar.multiselect(
        "Primary Type (Select Individual Types)",
        options=all_types,
        default=st.session_state.get(f"{page_title}_types_default", []),
        key=f"{page_title}_types",
        help="Filter cards that contain ALL selected primary types.",
    )
    if st.session_state.get(f"{page_title}_types_default") != selected_types:
        st.session_state[f"{page_title}_types_default"] = selected_types
        filter_state_changed = True

    # D. Edition Filter
    all_editions = page_df["Edition_Name"].dropna().unique().tolist()
    selected_editions = st.sidebar.multiselect(
        "Filter by Edition",
        options=sorted(all_editions),
        default=st.session_state.get(f"{page_title}_editions_default", []),
        key=f"{page_title}_editions",
    )
    if st.session_state.get(f"{page_title}_editions_default") != selected_editions:
        st.session_state[f"{page_title}_editions_default"] = selected_editions
        filter_state_changed = True

    # E. Super Type Filter (Checkboxes in Expander)
    with st.sidebar.expander("Filter by Super Type"):
        all_supers = sorted([x for x in page_df["Super_Type"].unique() if x != ""])
        selected_supers = st.session_state.get(f"{page_title}_supers_default", [])
        newly_selected_supers = []
        for s_type in all_supers:
            # Checkbox state management
            default_checked = s_type in selected_supers
            is_checked = st.checkbox(
                s_type, value=default_checked, key=f"{page_title}_super_{s_type}"
            )
            if is_checked:
                newly_selected_supers.append(s_type)

        # Check if the list of selected supers changed
        if selected_supers != newly_selected_supers:
            st.session_state[f"{page_title}_supers_default"] = newly_selected_supers
            filter_state_changed = True
        selected_supers = newly_selected_supers

    # F. Subtype Filter
    all_subtypes = set()
    for subtype_list in page_df["All_Subtypes"].str.split(", "):
        all_subtypes.update([s.strip() for s in subtype_list if s.strip()])

    selected_subtypes = st.sidebar.multiselect(
        "Filter by Subtype",
        options=sorted(list(all_subtypes)),
        default=st.session_state.get(f"{page_title}_subtypes_default", []),
        key=f"{page_title}_subtypes",
        help="Select one or more subtypes. Cards must contain ALL selected subtypes.",
    )
    if st.session_state.get(f"{page_title}_subtypes_default") != selected_subtypes:
        st.session_state[f"{page_title}_subtypes_default"] = selected_subtypes
        filter_state_changed = True

    # Reset page number if any filter changed
    if filter_state_changed:
        st.session_state["page_number"] = 0

    # --- APPLY FILTERS ---

    filtered_df = page_df.copy()

    # 1. Apply Name Search
    if search_term:
        filtered_df = filtered_df[
            filtered_df["Name"].str.lower().str.contains(search_term, na=False)
        ]

    # 2. Apply CMC
    filtered_df = filtered_df[
        (filtered_df["CMC"] != -1)
        & (filtered_df["CMC"] >= cmc_range[0])
        & (filtered_df["CMC"] <= cmc_range[1])
    ]

    # 3. Apply Primary Type
    if selected_types:
        mask = filtered_df["Primary_Type"].apply(
            lambda x: all(stype in x.split(" ") for stype in selected_types)
        )
        filtered_df = filtered_df[mask]

    # 4. Apply Edition
    if selected_editions:
        filtered_df = filtered_df[filtered_df["Edition_Name"].isin(selected_editions)]

    # 5. Apply Super Type
    if selected_supers:
        filtered_df = filtered_df[filtered_df["Super_Type"].isin(selected_supers)]

    # 6. Apply Subtype Filter
    if selected_subtypes:
        mask = filtered_df["All_Subtypes"].apply(
            lambda subtypes_str: all(
                subtype in subtypes_str.split(", ") for subtype in selected_subtypes
            )
        )
        filtered_df = filtered_df[mask]

    # --- DISPLAY METRICS, ANALYSIS, & DATA ---

    st.subheader(f"Results: {filtered_df.shape[0]} Cards Found")

    col1, col2, col3 = st.columns(3)
    col1.metric("Cards Found", filtered_df.shape[0])

    if not filtered_df.empty and "CMC" in filtered_df.columns:
        col2.metric("Avg CMC", f"{filtered_df['CMC'].mean():.2f}")
    else:
        col2.metric("Avg CMC", "N/A")

    col3.metric("Unique Editions", filtered_df["Edition_Name"].nunique())

    # --- ANALYSIS CHARTS (Mana Curve & Type Distribution) ---
    if not filtered_df.empty:
        chart_col1, chart_col2 = st.columns(2)

        # --- CMC OUTLIER HANDLING ---
        CMC_OUTLIER_THRESHOLD = 20
        # Cards used for the chart (CMC <= 20)
        chart_df = filtered_df[filtered_df["CMC"] <= CMC_OUTLIER_THRESHOLD]
        # Cards excluded from the chart (CMC > 20)
        outlier_df = filtered_df[filtered_df["CMC"] > CMC_OUTLIER_THRESHOLD]

        # 1. Mana Curve Visualization
        with chart_col1:
            st.subheader("Mana Curve (CMC Distribution)")

            cmc_counts = chart_df["CMC"].value_counts().sort_index().reset_index()
            cmc_counts.columns = ["CMC", "Count"]

            fig_cmc = px.bar(
                cmc_counts,
                x="CMC",
                y="Count",
                title=f"Distribution of Converted Mana Cost (up to {CMC_OUTLIER_THRESHOLD})",
                color_discrete_sequence=[MTG_CHART_COLOURS.get(page_title, "#CCCCCC")],
            )
            fig_cmc.update_layout(
                xaxis_title="CMC", yaxis_title="Number of Cards", showlegend=False
            )
            st.plotly_chart(fig_cmc, use_container_width=True)

            # Display Outlier Note if necessary
            if not outlier_df.empty:
                outlier_names = [
                    f"{row['Name']} (CMC: {row['CMC']})"
                    for _, row in outlier_df.iterrows()
                ]
                st.info(
                    f" **CMC Outlier Detected:** The following card(s) were excluded from the chart "
                    f"for having a CMC greater than {CMC_OUTLIER_THRESHOLD}: "
                    f"{'; '.join(outlier_names)}."
                )

        # 2. Card Type Distribution (User Story 7)
        with chart_col2:
            st.subheader("Card Type Breakdown")

            type_counts = (
                filtered_df["Primary_Type"]
                .dropna()
                .str.split(" ")
                .explode()
                .str.strip()
                .value_counts()
                .reset_index()
            )
            type_counts.columns = ["Primary_Type", "Count"]

            fig_type = px.pie(
                type_counts,
                values="Count",
                names="Primary_Type",
                title="Primary Card Type Distribution",
                hole=0.3,
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            st.plotly_chart(fig_type, use_container_width=True)

    else:
        st.warning("Apply filters to see analysis charts.")

    # --- FINAL CARD LIST (PAGINATED) ---
    display_cols = ["Name", "Edition_Name", "Primary_Type", "CMC", "All_Subtypes"]

    final_display_cols = [col for col in display_cols if col in filtered_df.columns]
    full_df_for_list = filtered_df[final_display_cols]

    # Use the new paginated rendering function
    render_paginated_card_list(full_df_for_list, "Filtered Card List")


# End of render_colour_page function


# --- MAIN APP LOGIC ---
def run_app():
    # Set page config globally before any Streamlit commands
    st.set_page_config(page_title="MTG Builder", layout="wide")

    # Initialize Session State
    if "current_page" not in st.session_state:
        st.session_state["current_page"] = "Home"
    if "compare_card_1" not in st.session_state:
        st.session_state["compare_card_1"] = None
    if "compare_card_2" not in st.session_state:
        st.session_state["compare_card_2"] = None
    if "page_number" not in st.session_state:
        st.session_state["page_number"] = 0
    if "last_rendered_page" not in st.session_state:
        st.session_state["last_rendered_page"] = "Home"

    # Load Data Once
    data_df = load_and_join_all_data()
    if data_df is None:
        return

    # --- NAVIGATION SIDEBAR ---
    st.sidebar.title("🧙‍♂️ Planeswalker Nav")

    # Map selection to (Column_Name, Display_Name, Emoji)
    pages = {
        "Home": (None, "Home", "🏠"),
        "White": (COLOUR_COLUMNS["White"], "White", "☀️"),
        "Blue": (COLOUR_COLUMNS["Blue"], "Blue", "💧"),
        "Black": (COLOUR_COLUMNS["Black"], "Black", "💀"),
        "Red": (COLOUR_COLUMNS["Red"], "Red", "🔥"),
        "Green": (COLOUR_COLUMNS["Green"], "Green", "🌳"),
        "Colourless": (COLOUR_COLUMNS["Colourless"], "Colourless", "🪟"),
        "Generic": (None, "Generic", "⚙️"),
        "Card Compare": (None, "Compare", "⚖️"),
    }

    # 1. Determine the index for the radio button based on the *current* state
    current_page = st.session_state["current_page"]
    current_page_index = (
        list(pages.keys()).index(current_page) if current_page in pages else 0
    )

    # 2. Render the radio with the on_change callback for manual navigation
    st.sidebar.radio(
        "Go to:",
        list(pages.keys()),
        index=current_page_index,
        key="page_radio",
        on_change=update_page_state_from_radio,  # Resets page_number on page switch
    )

    # Set the page background colour based on the selection
    set_page_theme(current_page)

    # Unpack selection based on the authoritative current_page
    col_filter, page_name, emoji = pages[current_page]

    # --- RENDER LOGIC BASED ON CURRENT PAGE ---
    if current_page == "Home":
        # Reset page number to 0 only when entering the Home page (if coming from another page)
        if st.session_state["current_page"] != st.session_state.get(
            "last_rendered_page"
        ):
            st.session_state["page_number"] = 0

        st.session_state["last_rendered_page"] = current_page

        st.title("🏠 MTG Collection Dashboard")
        st.markdown("### Welcome, Deck Builder! | Global Stats")
        st.markdown(
            "Use the colour pages and filters to find cards, or click on a card name to enter the comparison tool."
        )

        col1, col2, col3 = st.columns(3)
        col1.info(f"**Total Cards:** {len(data_df)}")
        col2.info(f"**Editions:** {data_df['Edition_Name'].nunique()}")
        col3.info(f"**Unique Subtypes:** {data_df['All_Subtypes'].nunique()}")

        # Colour Distribution Chart
        render_colour_distribution_chart(data_df)

        # --- HOME CARD LIST (PAGINATED RANDOM SAMPLE) ---
        display_cols_home = [
            "Name",
            "Edition_Name",
            "Primary_Type",
            "CMC",
            "All_Subtypes",
        ]
        existing_cols_home = [
            col for col in display_cols_home if col in data_df.columns
        ]

        # Use a consistent random sample for pagination stability across reruns
        if "home_sample_df" not in st.session_state:
            st.session_state["home_sample_df"] = data_df.sample(
                min(100, len(data_df)), random_state=42
            )[  # Added fixed random_state for stability
                existing_cols_home
            ]

        render_paginated_card_list(
            st.session_state["home_sample_df"], "Random Card Sample"
        )

    elif current_page == "Card Compare":
        st.session_state["last_rendered_page"] = current_page
        render_compare_page(data_df)

    else:  # Colour Pages (White, Blue, Black, Red, Green, Colourless)
        # Reset page number only when entering the page
        if st.session_state["current_page"] != st.session_state.get(
            "last_rendered_page"
        ):
            st.session_state["page_number"] = 0
            st.session_state["last_rendered_page"] = current_page

        render_colour_page(data_df, col_filter, page_name, emoji)


if __name__ == "__main__":
    try:
        run_app()
    except Exception as e:
        # A generic catch-all for runtime errors not caught by Streamlit's file-not-found check
        st.error(f"An unexpected error occurred during application runtime: {e}")
        st.stop()
