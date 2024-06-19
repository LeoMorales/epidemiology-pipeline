# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# Add description here
#
# *Note:* You can open this file as a notebook (JupyterLab: right-click on it in the side bar -> Open With -> Notebook)


# %%
# Uncomment the next two lines to enable auto reloading for imported modules
# # %load_ext autoreload
# # %autoreload 2
# For more info, see:
# https://docs.ploomber.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# %% tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = ["get-rates-departamental"]

# This is a placeholder, leave it as None
product = None


# %%
# your code here...
upstream = {"get-rates-departamental": "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates/departamental-rates.csv"}
product = {"nb": "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates/get-rates.ipynb", "data": "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates/rates.csv"}

# %% [markdown]
# # Lectura de datos

# %%
import pandas

# %%
data = pandas.read_csv(upstream['get-rates-departamental'], dtype={'department_id': object})

# %%
data

# %%
data["age_group"].unique()

# %%
import geopandas

# %%
shape_path = "/home/lmorales/work/pipelines/resources/departamentos.geojson"
shape = geopandas.read_file(shape_path)

# reproyectar la capa:
crs_destino = 'EPSG:22185'
# EPSG:22185 (POSGAR 2007 / Argentina 4) es un sistema de coordenadas proyectado específico para Argentina.
shape = shape.to_crs(crs_destino)
shape.head(3)

# %%
df_1997_2017_female = (
    data[
        (data["age_group"] == "65-150")
        & (data["sex"] == "female")
        & (data["period"] == "1997-2017")
    ]
    .reset_index(drop=True)
    [columns_order]
    .rename(columns=columns_renaming)
    .copy()
)

print(df_1997_2017_female.head(2))

# %%
df_1997_2017_female["DEPARTAMENTO"].value_counts()

# %%
# Encontrar registros duplicados basados en la columna 'codigo'
duplicados = df_1997_2017_female[df_1997_2017_female.duplicated(subset='DEPARTAMENTO', keep=False)]

# Mostrar registros duplicados
print(duplicados)

# %%
# Realizar la combinación (merge) por 'codigo_a' de df1 y 'codigo_b' de df2
merged_shape_df = pandas.merge(
    shape,
    df_1997_2017_female,
    left_on="departamento_id",
    right_on="DEPARTAMENTO",
    how='left',
    indicator=True)

# Identificar y contar los códigos en df1 que no fueron encontrados en df2
codigos_no_encontrados = merged_shape_df[merged_shape_df['_merge'] == 'left_only']['departamento_id']

# Mostrar los códigos no encontrados y la cantidad
print(f"Cantidad de departamentos sin datos: {codigos_no_encontrados.nunique()}")

# %%
merged_shape_df.columns = [col.upper() for col in merged_shape_df.columns]

columnas_seleccionadas = [
    'PROVINCIA_ID',
    'PROVINCIA_NOMBRE',
    'DEPARTAMENTO_ID', 'DEPARTAMENTO_NOMBRE',
    'GEOMETRY', 'PERIODO', 'RANGO EDADES', 'SEXO',
    'MUERTES TOTALES', 'MUERTES ALZHEIMER', 'TEA*1000']

merged_shape_df = merged_shape_df[columnas_seleccionadas].copy()


merged_shape_df['PERIODO'] = '1997-2017'
merged_shape_df['RANGO EDADES'] = '>64'
merged_shape_df['SEXO'] = 'mujeres'
merged_shape_df['MUERTES TOTALES'] = merged_shape_df['MUERTES TOTALES'].fillna("-")
merged_shape_df['MUERTES ALZHEIMER'] = merged_shape_df['MUERTES ALZHEIMER'].fillna("-")
#merged_shape_df['TASA POR 1000'] = merged_shape_df['TASA POR 1000'].fillna("-")

merged_shape_df['LATITUD'] = merged_shape_df['GEOMETRY'].centroid.y
merged_shape_df['LONGITUD'] = merged_shape_df['GEOMETRY'].centroid.x

merged_shape_df = merged_shape_df.set_geometry('GEOMETRY')

# %%
merged_shape_df.sort_values(
    by=["PROVINCIA_NOMBRE", "DEPARTAMENTO_ID"]
).reset_index(
    drop=True
).to_csv(
    "./tasas-departamentales-1997-2017-mujeres-mayores-65.csv",
    index=False
)

# %% [raw]
#
#
#

# %%
df_1997_2017_male = (
    data[
        (data["age_group"] == "65-150")
        & (data["sex"] == "male")
        & (data["period"] == "1997-2017")
    ]
    .reset_index(drop=True)
    [columns_order]
    .rename(columns=columns_renaming)
    .copy()
)

print(df_1997_2017_male.head(2))

print(df_1997_2017_male["DEPARTAMENTO"].value_counts()[:5])

# Encontrar registros duplicados basados en la columna 'codigo'
duplicados = df_1997_2017_male[df_1997_2017_male.duplicated(subset='DEPARTAMENTO', keep=False)]

# Mostrar registros duplicados
print("duplicados:")
print(duplicados)

# Realizar la combinación (merge)
shape_male_plus64_1997_2017_df = pandas.merge(
    shape,
    df_1997_2017_male,
    left_on="departamento_id",
    right_on="DEPARTAMENTO",
    how='left',
    indicator=True)

# Identificar y contar los códigos en df1 que no fueron encontrados en df2
codigos_no_encontrados = shape_male_plus64_1997_2017_df[shape_male_plus64_1997_2017_df['_merge'] == 'left_only']['departamento_id']

# Mostrar los códigos no encontrados y la cantidad
print(f"Cantidad de departamentos sin datos: {codigos_no_encontrados.nunique()}")

shape_male_plus64_1997_2017_df.columns = [col.upper() for col in shape_male_plus64_1997_2017_df.columns]

columnas_seleccionadas = [
    'PROVINCIA_ID',
    'PROVINCIA_NOMBRE',
    'DEPARTAMENTO_ID', 'DEPARTAMENTO_NOMBRE',
    'GEOMETRY', 'PERIODO', 'RANGO EDADES', 'SEXO',
    'MUERTES TOTALES', 'MUERTES ALZHEIMER', 'TEA*1000']

shape_male_plus64_1997_2017_df = shape_male_plus64_1997_2017_df[columnas_seleccionadas].copy()


shape_male_plus64_1997_2017_df['PERIODO'] = '1997-2017'
shape_male_plus64_1997_2017_df['RANGO EDADES'] = '>64'
shape_male_plus64_1997_2017_df['SEXO'] = 'mujeres'
shape_male_plus64_1997_2017_df['MUERTES TOTALES'] = shape_male_plus64_1997_2017_df['MUERTES TOTALES'].fillna("-")
shape_male_plus64_1997_2017_df['MUERTES ALZHEIMER'] = shape_male_plus64_1997_2017_df['MUERTES ALZHEIMER'].fillna("-")
#shape_male_plus64_1997_2017_df['TASA POR 1000'] = shape_male_plus64_1997_2017_df['TASA POR 1000'].fillna("-")

shape_male_plus64_1997_2017_df['LATITUD'] = shape_male_plus64_1997_2017_df['GEOMETRY'].centroid.y
shape_male_plus64_1997_2017_df['LONGITUD'] = shape_male_plus64_1997_2017_df['GEOMETRY'].centroid.x

shape_male_plus64_1997_2017_df = shape_male_plus64_1997_2017_df.set_geometry('GEOMETRY')

shape_male_plus64_1997_2017_df.sort_values(by=["PROVINCIA_NOMBRE", "DEPARTAMENTO_ID"]).reset_index(drop=True)

shape_male_plus64_1997_2017_df.sort_values(
    by=["PROVINCIA_NOMBRE", "DEPARTAMENTO_ID"]
).reset_index(
    drop=True
).to_csv(
    "./tasas-departamentales-1997-2017-varones-mayores-65.csv",
    index=False
)

# %%
import matplotlib.pyplot as plt

# %%
f, ax = plt.subplots(figsize=(6, 12))

merged_shape_df[merged_shape_df["TEA*1000"].isna()].plot(
    ax=ax,
    color='white',
    edgecolor="lightgrey"
)

merged_shape_df.plot(
    column="TEA*1000",
    ax=ax,
    legend=True,
    legend_kwds={'shrink': 0.3}
)

ax.set_axis_off()

ax.set_title("""TEA*1000 por departamentos
Sexo: mujeres
Rango etario: mayores de 64 años
Periodo: 1997-2017""")

plt.show();

# %%
f, ax = plt.subplots(figsize=(6, 12))

shape_male_plus64_1997_2017_df[shape_male_plus64_1997_2017_df["TEA*1000"].isna()].plot(
    ax=ax,
    color='white',
    edgecolor="lightgrey"
)

shape_male_plus64_1997_2017_df.plot(
    column="TEA*1000",
    ax=ax,
    legend=True,
    legend_kwds={'shrink': 0.3}
)

ax.set_axis_off()

ax.set_title("""TEA*1000 por departamentos
Sexo: varones
Rango etario: mayores de 64 años
Periodo: 1997-2017""")

plt.show();

# %%
df_1997_2017_all = (
    data[
        (data["age_group"] == "65-150")
        & (data["sex"] == "all")
        & (data["period"] == "1997-2017")
    ]
    .reset_index(drop=True)
    [columns_order]
    .rename(columns=columns_renaming)
    .copy()
)

print(df_1997_2017_all.head(2))

print(df_1997_2017_all["DEPARTAMENTO"].value_counts()[:5])

# Encontrar registros duplicados basados en la columna 'codigo'
duplicados = df_1997_2017_all[df_1997_2017_all.duplicated(subset='DEPARTAMENTO', keep=False)]

# Mostrar registros duplicados
print("duplicados:")
print(duplicados)

# Realizar la combinación (merge)
shape_plus64_1997_2017_df = pandas.merge(
    shape,
    df_1997_2017_all,
    left_on="departamento_id",
    right_on="DEPARTAMENTO",
    how='left',
    indicator=True)

# Identificar y contar los códigos en df1 que no fueron encontrados en df2
codigos_no_encontrados = shape_plus64_1997_2017_df[shape_plus64_1997_2017_df['_merge'] == 'left_only']['departamento_id']

# Mostrar los códigos no encontrados y la cantidad
print(f"Cantidad de departamentos sin datos: {codigos_no_encontrados.nunique()}")

shape_plus64_1997_2017_df.columns = [col.upper() for col in shape_plus64_1997_2017_df.columns]

columnas_seleccionadas = [
    'PROVINCIA_ID',
    'PROVINCIA_NOMBRE',
    'DEPARTAMENTO_ID', 'DEPARTAMENTO_NOMBRE',
    'GEOMETRY', 'PERIODO', 'RANGO EDADES', 'SEXO',
    'MUERTES TOTALES', 'MUERTES ALZHEIMER', 'TEA*1000']

shape_plus64_1997_2017_df = shape_plus64_1997_2017_df[columnas_seleccionadas].copy()


shape_plus64_1997_2017_df['PERIODO'] = '1997-2017'
shape_plus64_1997_2017_df['RANGO EDADES'] = '>64'
shape_plus64_1997_2017_df['SEXO'] = 'mujeres'
shape_plus64_1997_2017_df['MUERTES TOTALES'] = shape_plus64_1997_2017_df['MUERTES TOTALES'].fillna("-")
shape_plus64_1997_2017_df['MUERTES ALZHEIMER'] = shape_plus64_1997_2017_df['MUERTES ALZHEIMER'].fillna("-")
#shape_plus64_1997_2017_df['TASA POR 1000'] = shape_plus64_1997_2017_df['TASA POR 1000'].fillna("-")

shape_plus64_1997_2017_df['LATITUD'] = shape_plus64_1997_2017_df['GEOMETRY'].centroid.y
shape_plus64_1997_2017_df['LONGITUD'] = shape_plus64_1997_2017_df['GEOMETRY'].centroid.x

shape_plus64_1997_2017_df = shape_plus64_1997_2017_df.set_geometry('GEOMETRY')

shape_plus64_1997_2017_df.sort_values(by=["PROVINCIA_NOMBRE", "DEPARTAMENTO_ID"]).reset_index(drop=True)

shape_plus64_1997_2017_df.sort_values(
    by=["PROVINCIA_NOMBRE", "DEPARTAMENTO_ID"]
).reset_index(
    drop=True
).to_csv(
    "./tasas-departamentales-1997-2017-todos-mayores-65.csv",
    index=False
)

# %%
f, ax = plt.subplots(figsize=(6, 12))

shape_plus64_1997_2017_df[shape_plus64_1997_2017_df["TEA*1000"].isna()].plot(
    ax=ax,
    color='white',
    edgecolor="lightgrey"
)

shape_plus64_1997_2017_df.plot(
    column="TEA*1000",
    ax=ax,
    legend=True,
    legend_kwds={'shrink': 0.3}
)

ax.set_axis_off()

ax.set_title("""TEA*1000 por departamentos
Sexo: mujeres y varones
Rango etario: mayores de 64 años
Periodo: 1997-2017""")

plt.show();

# %%

# %%

# %%
