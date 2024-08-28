# -*- coding: utf-8 -*-
# %%
# Add description here
#
# *Note:* You can open this file as a notebook (JupyterLab: right-click on it in the side bar -> Open With -> Notebook)

# %%
# Uncomment the next two lines to enable auto reloading for imported modules
# %load_ext autoreload
# %autoreload 2
# For more info, see:
# https://docs.ploomber.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# %% tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = ["get-departamental-csmr"]

# This is a placeholder, leave it as None
product = None

# %% [markdown]
# # Lectura de los datos

# %%
import pandas

# %%
# data = pandas.read_csv("../../_products/rates/departamental-rates.csv", dtype={'DEPARTAMENTO': object})
data = pandas.read_csv(
    upstream["get-departamental-csmr"]["data"], dtype={"department_id": object}
)

# %%
data

# %% [markdown]
# ## Agregar códigos de departamento:

# %%
from isonymic import utils

# %%
data = utils.append_cell_description(data, department_code_column="department_id")

# %%
data.columns

# %% [markdown]
# ## Renombrar columnas:

# %%
columns_renaming = {
    "period": "PERIODO",
    "age_group": "RANGO_EDADES",
    "sex": "SEXO",
    "region_nombre": "REGION",
    "provincia_nombre": "PROVINCIA",
    "department_id": "DEPARTAMENTO",
    "departamento_nombre": "DEPARTAMENTO_NOMBRE",
    "deceases": "MUERTES_TOTALES",
    "specific": "MUERTES_ALZHEIMER",
    "rate": "TEA*1000",
}

# %%
columns_order = list(columns_renaming.keys())

# %%
complete_df = (
    data[columns_order]
    .sort_values(
        by=[
            "period",
            "age_group",
            "sex",
            "region_nombre",
            "provincia_nombre",
            "department_id",
        ]
    )
    .reset_index(drop=True)
    .rename(columns=columns_renaming)
)

complete_df

# %% [markdown]
# ## Agrupar los reglones de CABA

# %%
groupby_caba = complete_df[complete_df["DEPARTAMENTO"].str.slice(0, 2) == "02"].groupby(
    ["PERIODO", "RANGO_EDADES", "SEXO", "REGION", "PROVINCIA"]
)

rows = []

for i, slice_df in groupby_caba:
    # ('1997-2007', '0-64', 'all', 'Centro', 'Ciudad Autónoma de Buenos Aires') + (tuple(a.values))
    rows.append(
        i
        + ("02000", "CABA")
        + tuple(slice_df[["MUERTES_TOTALES", "MUERTES_ALZHEIMER"]].sum().values)
    )

# %%
caba_df = pandas.DataFrame(rows, columns=complete_df.columns[:-1])

caba_df.head(3)

# %%
caba_df["TEA*1000"] = caba_df["MUERTES_ALZHEIMER"] / caba_df["MUERTES_TOTALES"] * 1_000

caba_df.head()

# %%
complete_df = complete_df[complete_df["DEPARTAMENTO"].str.slice(0, 2) != "02"].copy()

# %%
output_df = pandas.concat([complete_df, caba_df])

# %% [markdown]
# # Guardar

# %%
output_df = output_df.sort_values(
    by=["PERIODO", "RANGO_EDADES", "SEXO", "REGION", "PROVINCIA", "DEPARTAMENTO"]
).reset_index(drop=True)

output_df

# %%
# complete_df.to_csv("tasas-departmentales-base-completa.csv", index=False)
complete_df.to_csv(product["data"], index=False)

# %%
