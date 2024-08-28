# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% papermill={"duration": 0.0061, "end_time": "2024-05-24T21:03:31.634773", "exception": false, "start_time": "2024-05-24T21:03:31.628673", "status": "completed"}
# Add description here
#
# *Note:* You can open this file as a notebook (JupyterLab: right-click on it in the side bar -> Open With -> Notebook)


# %% papermill={"duration": 0.005062, "end_time": "2024-05-24T21:03:31.641456", "exception": false, "start_time": "2024-05-24T21:03:31.636394", "status": "completed"}
# Uncomment the next two lines to enable auto reloading for imported modules
# # %load_ext autoreload
# # %autoreload 2
# For more info, see:
# https://docs.ploomber.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# %% papermill={"duration": 0.007835, "end_time": "2024-05-24T21:03:31.650679", "exception": false, "start_time": "2024-05-24T21:03:31.642844", "status": "completed"} tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = ["get-clean-deceases-data"]

# This is a placeholder, leave it as None
product = None


# %% papermill={"duration": 0.005444, "end_time": "2024-05-24T21:03:31.657626", "exception": false, "start_time": "2024-05-24T21:03:31.652182", "status": "completed"} tags=["injected-parameters"]
# Parameters
upstream = {
    "get-clean-deceases-data": "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/clean/cleaned-deceases-data.parquet"
}
product = {
    "nb": "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates/get-rates.ipynb",
    "data": "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates/rates.csv",
}


# %% papermill={"duration": 41.485231, "end_time": "2024-05-24T21:04:13.144287", "exception": false, "start_time": "2024-05-24T21:03:31.659056", "status": "completed"}
import pandas
from isonymic import utils
from epidemiologic import rates

# %% papermill={"duration": 41.485231, "end_time": "2024-05-24T21:04:13.144287", "exception": false, "start_time": "2024-05-24T21:03:31.659056", "status": "completed"}
data_path = str(upstream["get-clean-deceases-data"])
# "../../../epidemiology_pipeline/_products/clean/cleaned-deceases-data.parquet"

clean_df = pandas.read_parquet(data_path)

year_min = clean_df[clean_df["is_specific"]]["year"].min()
year_max = clean_df[clean_df["is_specific"]]["year"].max()


all_deceases_df = clean_df[
    (clean_df["year"] >= year_min) & (clean_df["year"] <= year_max)
].copy()

WORK_COL = [
    "codigo_defuncion",
    "sex",
    "age_in_years",
    "department_id",
    "provincia_id",
    "year",
]

# %% papermill={"duration": 41.485231, "end_time": "2024-05-24T21:04:13.144287", "exception": false, "start_time": "2024-05-24T21:03:31.659056", "status": "completed"}
############################################################################
# totales
############################################################################
all_deceases_df = all_deceases_df[WORK_COL].copy()

all_deceases_df["region_id"] = all_deceases_df["provincia_id"].apply(
    utils.REGION_BY_PROVINCE_CODE_DICT.get
)
all_deceases_df = all_deceases_df.sort_values(
    by=["department_id", "codigo_defuncion", "sex"]
)
all_deceases_df = all_deceases_df.reset_index(drop=True)

all_deceases_df = all_deceases_df[WORK_COL + ["region_id"]]

############################################################################
# específicos
############################################################################
specific_deceases_df = clean_df[clean_df["is_specific"]].copy()
specific_deceases_df = specific_deceases_df[WORK_COL].copy()

specific_deceases_df["region_id"] = specific_deceases_df["provincia_id"].apply(
    utils.REGION_BY_PROVINCE_CODE_DICT.get
)

specific_deceases_df = specific_deceases_df.sort_values(
    by=["department_id", "codigo_defuncion", "sex"]
)
specific_deceases_df = specific_deceases_df.reset_index(drop=True)

specific_deceases_df = specific_deceases_df[WORK_COL + ["region_id"]]

# %%
all_deceases_df.head()

# %% papermill={"duration": 41.485231, "end_time": "2024-05-24T21:04:13.144287", "exception": false, "start_time": "2024-05-24T21:03:31.659056", "status": "completed"}
############################################################################
# loop: rangos etarios
############################################################################
rangos_etarios = [(0, 64), (65, 74), (75, 84), (85, 150), (65, 150), (0, 150)]
datasets = []

print("Procesando", end=" ")

for edad_base, edad_tope in rangos_etarios:
    periodo_i = f"{edad_base}-{edad_tope}"
    print(periodo_i, end=" ")

    ############################################################################
    # loop: rangos año
    ############################################################################

    rangos_etarios_de_un_anio = [(year_a, year_a) for year_a in range(1997, 2018)]
    rangos_etarios_de_un_anio.append((1997, 2017))

    for year_a, year_b in rangos_etarios_de_un_anio:
        all_df = all_deceases_df[
            (all_deceases_df["age_in_years"] >= edad_base)
            & (all_deceases_df["age_in_years"] <= edad_tope)
            & (all_deceases_df["year"] >= year_a)
            & (all_deceases_df["year"] <= year_b)
        ].copy()

        specific_df = specific_deceases_df[
            (specific_deceases_df["age_in_years"] >= edad_base)
            & (specific_deceases_df["age_in_years"] <= edad_tope)
            & (specific_deceases_df["year"] >= year_a)
            & (specific_deceases_df["year"] <= year_b)
        ].copy()

        # obtener csmr para provincias
        provincial_csmr_df = rates.get_divisional_csmr(
            all_df, specific_df, "provincia_id"
        )
        provincial_csmr_df["region_id"] = provincial_csmr_df["provincia_id"].apply(
            utils.REGION_BY_PROVINCE_CODE_DICT.get
        )
        provincial_csmr_df["division_type"] = "provincia"

        # obtener csmr para regiones
        regional_csmr_df = rates.get_divisional_csmr(all_df, specific_df, "region_id")
        regional_csmr_df["provincia_id"] = "Total"
        regional_csmr_df["division_type"] = "region"

        df = pandas.concat([regional_csmr_df, provincial_csmr_df])

        df["provincia_nombre"] = df["provincia_id"].apply(
            utils.PROVINCE_NAME_BY_ID_DICT.get
        )
        df["provincia_nombre"] = df["provincia_nombre"].fillna("Total")

        # A las columnas de los totales les ponemos la letra 'B',
        # a las otras, la letra 'A'
        df.loc[df["provincia_nombre"] != "Total", "order_col"] = "A"
        df.loc[df["provincia_nombre"] == "Total", "order_col"] = "B"

        df = df.sort_values(by=["region_id", "order_col", "provincia_nombre"])

        # obtener csmr para el país
        national_csmr_df = rates.get_divisional_csmr(
            all_df.assign(region_id="Argentina"),
            specific_df.assign(region_id="Argentina"),
            "region_id",
        )
        national_csmr_df["provincia_nombre"] = "Total"
        national_csmr_df["division_type"] = "pais"

        columns_order = [
            "region_id",
            "provincia_nombre",
            "total_deceases",
            "specific_deceases",
            "csmr",
            "total_deceases_female",
            "specific_deceases_female",
            "csmr_female",
            "total_deceases_male",
            "specific_deceases_male",
            "csmr_male",
            "division_type",
        ]

        national_csmr_df = national_csmr_df[columns_order]
        df = df[columns_order]

        age_group_rates_df = pandas.concat([df, national_csmr_df]).reset_index(
            drop=True
        )

        age_group_rates_df["age_group"] = periodo_i

        periodo_de_anios = f"{year_a}-{year_b}" if year_a != year_b else str(year_a)
        age_group_rates_df["period"] = periodo_de_anios

        datasets.append(age_group_rates_df)

output_df = pandas.concat(datasets)

# las columnas de fallecimientos son enteras
deceases_columns = [
    deceases_col for deceases_col in output_df.columns if "deceases" in deceases_col
]
for deceases_col_i in deceases_columns:
    output_df[deceases_col_i] = output_df[deceases_col_i].fillna(0).astype(int)

output_df = output_df.reset_index(drop=True)
# %%
output_df.head()

# %%
cols_ordered = [
    "period",
    "age_group",
    "region_id",
    "provincia_nombre",
    "division_type",
    "total_deceases",
    "specific_deceases",
    "csmr",
    "total_deceases_female",
    "specific_deceases_female",
    "csmr_female",
    "total_deceases_male",
    "specific_deceases_male",
    "csmr_male",
]

# %%
output_df = output_df[cols_ordered]

output_df.head()

# %%
output_df.to_csv(product["data"], index=False)
