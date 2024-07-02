# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["get-clean-deceases-data"]

# This is a placeholder, leave it as None
product = None


# +
import pandas as pd
from surnames_package import utils
from epidemiologic import rates
import sys
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# to log a message, call logger.info
logger.info('Logging up get-rates-departmental')
# -

# Constants
WORKING_COLUMNS = [
    "codigo_defuncion",
    "sex",
    "age_in_years",
    "department_id",
    "provincia_id",
    "year",
]
RANGES_AGE = [(0, 150), (0, 64), (65, 150)]
PERIODS = [(1997, 2017), (1997, 2007), (2007, 2017)]


def load_data(file_path):
    return pd.read_parquet(file_path)


def preprocess_data(df):
    year_min = df[df["is_specific"]]["year"].min()
    year_max = df[df["is_specific"]]["year"].max()

    all_deceases_df = df[(df["year"] >= year_min) & (df["year"] <= year_max)].copy()
    all_deceases_df["region_id"] = all_deceases_df["provincia_id"].apply(
        utils.REGION_BY_PROVINCE_CODE_DICT.get
    )

    specific_deceases_df = df[df["is_specific"]].copy()
    specific_deceases_df["region_id"] = specific_deceases_df["provincia_id"].apply(
        utils.REGION_BY_PROVINCE_CODE_DICT.get
    )

    return all_deceases_df, specific_deceases_df


def melt_data(df):
    cols_totals = [
        "total_deceases",
        "total_deceases_female",
        "total_deceases_male",
    ]

    cols_specific = [
        "specific_deceases",
        "specific_deceases_female",
        "specific_deceases_male",
    ]

    cols_rates = ["csmr", "csmr_female", "csmr_male"]

    if "total_deceases_undetermined" in df.columns:
        cols_totals.append("total_deceases_undetermined")
    if "specific_deceases_undetermined" in df.columns:
        cols_specific.append("specific_deceases_undetermined")
    if "csmr_undetermined" in df.columns:
        cols_rates.append("csmr_undetermined")

    all_deceases_melted_df = (
        pd.melt(
            df,
            id_vars=["department_id"],
            value_vars=cols_totals,
            var_name="sex",
            value_name="deceases",
        )
        .assign(sex=lambda row: row["sex"].str.replace("total_deceases_", ""))
        .assign(sex=lambda row: row["sex"].str.replace("total_deceases", "all"))
    )

    specific_deceases_melted_df = (
        pd.melt(
            df,
            id_vars=["department_id"],
            value_vars=cols_specific,
            var_name="sex",
            value_name="specific",
        )
        .assign(sex=lambda row: row["sex"].str.replace("specific_deceases_", ""))
        .assign(sex=lambda row: row["sex"].str.replace("specific_deceases", "all"))
    )

    rates_melted_df = (
        pd.melt(
            df,
            id_vars=["department_id"],
            value_vars=cols_rates,
            var_name="sex",
            value_name="rate",
        )
        .assign(sex=lambda row: row["sex"].str.replace("csmr_", ""))
        .assign(sex=lambda row: row["sex"].str.replace("csmr", "all"))
    )

    tidy_df = pd.merge(
        pd.merge(
            all_deceases_melted_df,
            specific_deceases_melted_df,
            on=["department_id", "sex"],
        ),
        rates_melted_df,
        on=["department_id", "sex"],
    )

    tidy_df["new_rate"] = tidy_df["specific"] / tidy_df["deceases"] * 1000
    tidy_df.fillna(0, inplace=True)

    assert len(tidy_df[tidy_df["rate"] != tidy_df["new_rate"]]) == 0

    tidy_df = tidy_df.drop(columns="new_rate")

    for col in ["deceases", "specific"]:
        tidy_df[col] = tidy_df[col].fillna(0).astype(int)

    return tidy_df


def process_data(all_df, specific_df, periods, ranges_age):
    datasets = []

    for year_a, year_b in periods:
        
        logger.info(f"Procesando período {year_a}-{year_b}")
        
        for age_min, age_max in ranges_age:
            all_subset = all_df[
                (all_df["year"] >= year_a)
                & (all_df["year"] <= year_b)
                & (all_df["age_in_years"] >= age_min)
                & (all_df["age_in_years"] <= age_max)
            ].copy()

            specific_subset = specific_df[
                (specific_df["year"] >= year_a)
                & (specific_df["year"] <= year_b)
                & (specific_df["age_in_years"] >= age_min)
                & (specific_df["age_in_years"] <= age_max)
            ].copy()

            local_df = rates.get_divisional_csmr(
                all_subset, specific_subset, "department_id"
            )
            flat_df = melt_data(local_df)

            flat_df["period"] = f"{year_a}-{year_b}"
            flat_df["age_group"] = f"{age_min}-{age_max}"

            datasets.append(flat_df)

    return pd.concat(datasets)


def save_data(df, file_path):
    df.to_csv(file_path, index=False)


def rename_columns(df):
    columns_renaming = {
        "department_id": "DEPARTAMENTO",
        "sex": "SEXO",
        "deceases": "FALLECIMIENTOS TOTALES",
        "specific": "FALLECIMIENTOS ESPECÍFICOS",
        "rate": "TASAS POR 1000",
        "period": "PERIODO",
        "age_group": "RANGO EDADES",
    }
    return df.rename(columns=columns_renaming)


clean_df = load_data(upstream["get-clean-deceases-data"])
#data_path = "../../../epidemiology_pipeline/_products/clean/cleaned-deceases-data.parquet"
#clean_df = load_data(data_path)

all_deceases_df, specific_deceases_df = preprocess_data(clean_df)

output_df = process_data(all_deceases_df, specific_deceases_df, PERIODS, RANGES_AGE)

output_df

#output_df = rename_columns(output_df)
save_data(output_df, product["data"])
