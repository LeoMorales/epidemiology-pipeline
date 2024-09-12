# -*- coding: utf-8 -*-
# %% tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["get-clean-deceases-data"]
# This is a placeholder, leave it as None
product = None


# %%
from typing import List, Dict, Tuple
import pandas as pd
from isonymic import utils
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
WORKING_COLUMNS: List[str] = [
    "department_id",
    "year",
    "age_group",
    "sex",
    "codigo_defuncion",
    "is_specific",
    "cie_10_category",
    "age_in_years",
    "provincia_id",
    "region_id",
]

# Age and period constants
AGE_RANGES: List[Tuple[int, int]] = [(0, 150), (0, 64), (65, 150)]
TIME_PERIODS: List[Tuple[int, int]] = [(1997, 2017), (1997, 2007), (2007, 2017)]


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load the data from a parquet file and return a pandas DataFrame.

    Args:
        file_path (str): Path to the parquet file.

    Returns:
        pd.DataFrame: DataFrame with the loaded data.
    """
    logger.info(f"Loading data from {file_path}")
    return pd.read_parquet(file_path)


def categorize_codes(code_series: List[str]) -> Dict[str, str]:
    """
    Categorize CIE-10 codes based on their initial character and numeric component.

    Args:
        code_series (List[str]): List of CIE-10 codes.

    Returns:
        Dict[str, str]: Dictionary mapping each code to its respective category.
    """
    df = pd.DataFrame(code_series, columns=["code"]).dropna()
    df["initial"] = df["code"].str[0]
    # codes_serie = df["code"].astype("str").copy()
    # df["main_numbers"] = (
    #    codes_serie.str.extract(r"(\d+)", expand=False).str[:2].astype(int)
    # )
    df["main_numbers"] = (
        df["code"]
        .astype("str")
        .str.extractall("(\d+)")
        .unstack()
        .fillna("")
        .sum(axis=1)
        .astype(int)
        .astype(str)
        .str.slice(0, 2)
        .astype(int)
    )

    # Define the categorization conditions
    categories = {
        "I": df["initial"].isin(["A", "B"]),
        "II": (df["initial"] == "C")
        | ((df["initial"] == "D") & (df["main_numbers"] <= 49)),
        "III": (df["initial"] == "D") & (df["main_numbers"] >= 50),
        "IV": df["initial"] == "E",
        "V": df["initial"] == "F",
        "VI": df["initial"] == "G",
        "VII": (df["initial"] == "H") & (df["main_numbers"] <= 59),
        "VIII": (df["initial"] == "H") & (df["main_numbers"] >= 60),
        "IX": df["initial"] == "I",
        "X": df["initial"] == "J",
        "XI": df["initial"] == "K",
        "XII": df["initial"] == "L",
        "XIII": df["initial"] == "M",
        "XIV": df["initial"] == "N",
        "XV": df["initial"] == "O",
        "XVI": (df["initial"] == "P") & (df["main_numbers"] <= 96),
        "XVII": df["initial"] == "Q",
        "XVIII": df["initial"] == "R",
        "XIX": (df["initial"] == "S")
        | ((df["initial"] == "T") & (df["main_numbers"] <= 98)),
        "XX": df["initial"].isin(["V", "W", "X", "Y"]),
        "XXI": df["initial"] == "Z",
        "XXII": df["initial"] == "U",
    }

    # Generate the dictionary of codes to categories
    code_to_category = {
        code: category
        for category, condition in categories.items()
        for code in df[condition]["code"]
    }
    return code_to_category


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the data by filtering specific years, categorizing codes, and selecting columns.

    Args:
        df (pd.DataFrame): The original DataFrame.

    Returns:
        pd.DataFrame: Preprocessed DataFrame.
    """
    logger.info("Preprocessing data")
    year_min = df[df["is_specific"]]["year"].min()
    year_max = df[df["is_specific"]]["year"].max()

    # Filter by year and province/region information
    processed_df = df[(df["year"] >= year_min) & (df["year"] <= year_max)].copy()
    processed_df["region_id"] = processed_df["provincia_id"].apply(
        utils.REGION_BY_PROVINCE_CODE_DICT.get
    )

    # Apply CIE-10 categorization
    code_categories = categorize_codes(df["codigo_defuncion"].unique().tolist())
    processed_df["cie_10_category"] = processed_df["codigo_defuncion"].map(
        code_categories
    )

    # Select only the working columns
    return processed_df[WORKING_COLUMNS]


def save_data(df: pd.DataFrame, file_path: str) -> None:
    """
    Save the DataFrame to a parquet file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        file_path (str): File path where the parquet will be saved.
    """
    logger.info(f"Saving data to {file_path}")
    df.to_parquet(file_path, index=False)


# %%
# Load input data
previously_cleaned_df: pd.DataFrame = load_data(upstream["get-clean-deceases-data"])
preprocessed_data: pd.DataFrame = preprocess_data(previously_cleaned_df)

# Agrupación de los datos
final_df: pd.DataFrame = (
    preprocessed_data.groupby(
        [
            "department_id",
            "year",
            "age_group",
            "sex",
            "cie_10_category",
            "codigo_defuncion",
            "is_specific",
        ]
    )
    .size()
    .reset_index(name="counts")
)

# Ordenar las categorías de grupo de edad y categoría CIE-10
age_group_order: List[str] = [
    "<=5",
    "6-15",
    "16-35",
    "36-45",
    "46-55",
    "56-65",
    "66-75",
    "76-85",
    "85+",
]

final_df["age_group"] = pd.Categorical(
    final_df["age_group"], categories=age_group_order, ordered=True
)

roman_numeral_order: List[str] = [
    "I",
    "II",
    "III",
    "IV",
    "V",
    "VI",
    "VII",
    "VIII",
    "IX",
    "X",
    "XI",
    "XII",
    "XIII",
    "XIV",
    "XV",
    "XVI",
    "XVII",
    "XVIII",
    "XIX",
    "XX",
    "XXI",
    "XXII",
    "all_codes",
]
final_df["cie_10_category"] = pd.Categorical(
    final_df["cie_10_category"], categories=roman_numeral_order, ordered=True
)

# Ordenar el DataFrame
sorted_df: pd.DataFrame = final_df.sort_values(
    by=["department_id", "year", "age_group", "sex", "cie_10_category", "is_specific"]
).reset_index(drop=True)


# %%
# Visualizar el DataFrame ordenado
sorted_df.head(20)

# %%
assert len(preprocessed_data) == sorted_df["counts"].sum()

# %%
save_data(sorted_df, product["data"])
