# -*- coding: utf-8 -*-
# %%

# %% tags=["parameters"]
# Lista de tareas de las cuales depende esta tarea.
# Dejar como None si no hay dependencias.
upstream = ["get-departamental-csmr"]
product = None  # Placeholder para el producto de salida

# %% Importar librerías necesarias
import pandas as pd
from isonymic import utils


# %% Función para leer datos de entrada
def load_data(file_path):
    """
    Carga los datos desde un archivo CSV especificado.

    Args:
    - file_path (str): Ruta del archivo CSV a cargar.

    Returns:
    - pd.DataFrame: DataFrame con los datos cargados.
    """
    return pd.read_csv(file_path, dtype={"department_id": object})


# %% Función para agregar descripciones celulares a los datos
def add_department_codes(data):
    """
    Agrega descripciones regionales basadas en el código del departamento.

    Args:
    - data (pd.DataFrame): DataFrame que contiene la columna 'department_id'.

    Returns:
    - pd.DataFrame: DataFrame con las descripciones añadidas.
    """
    return utils.append_cell_description(data, department_code_column="department_id")


# %% Función para renombrar y reordenar columnas
def rename_and_order_columns(data, columns_renaming, columns_order):
    """
    Renombra y reordena las columnas de un DataFrame según los diccionarios proporcionados.

    Args:
    - data (pd.DataFrame): DataFrame a modificar.
    - columns_renaming (dict): Diccionario de mapeo para renombrar columnas.
    - columns_order (list): Lista de nombres de columnas en el orden deseado.

    Returns:
    - pd.DataFrame: DataFrame modificado.
    """
    return (
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


# %% Función para agrupar y calcular datos de CABA
def process_caba_data(df):
    """
    Procesa y agrupa los datos para CABA.

    Args:
    - df (pd.DataFrame): DataFrame con datos que incluyen a CABA.

    Returns:
    - pd.DataFrame: DataFrame con los datos agregados de CABA.
    """
    groupby_caba = df[df["DEPARTAMENTO"].str.slice(0, 2) == "02"].groupby(
        ["PERIODO", "RANGO_EDADES", "SEXO", "REGION", "PROVINCIA"]
    )

    rows = []
    for i, slice_df in groupby_caba:
        rows.append(
            i
            + ("02000", "CABA")
            + tuple(slice_df[["MUERTES_TOTALES", "MUERTES_ALZHEIMER"]].sum().values)
        )

    caba_df = pd.DataFrame(rows, columns=df.columns[:-1])
    caba_df["TEA*1000"] = (
        caba_df["MUERTES_ALZHEIMER"] / caba_df["MUERTES_TOTALES"] * 1_000
    )
    return caba_df


# %% Función para combinar datos y guardar el DataFrame final
def save_output_data(output_df, product_path):
    """
    Guarda el DataFrame final en un archivo CSV.

    Args:
    - output_df (pd.DataFrame): DataFrame final a guardar.
    - product_path (str): Ruta del archivo de salida.
    """
    output_df = output_df.sort_values(
        by=["PERIODO", "RANGO_EDADES", "SEXO", "REGION", "PROVINCIA", "DEPARTAMENTO"]
    ).reset_index(drop=True)
    output_df.to_csv(product_path, index=False)


# %% Lectura y procesamiento de datos
data = load_data(upstream["get-departamental-csmr"]["data"])
#data = pd.read_csv(
#   "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates/departamental-csmr.csv",
#   dtype={"department_id": object},
#)

print(data.head())

data = add_department_codes(data)
print(data.columns)

# Diccionarios para renombrar columnas y definir el orden
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
columns_order = list(columns_renaming.keys())

complete_df = rename_and_order_columns(data, columns_renaming, columns_order)
print(complete_df)

# Procesamiento de datos específicos de CABA
caba_df = process_caba_data(complete_df)
print(caba_df.head())

# Eliminar datos de CABA del DataFrame completo
complete_df = complete_df[complete_df["DEPARTAMENTO"].str.slice(0, 2) != "02"].copy()

# Combinar y guardar los datos finales
output_df = pd.concat([complete_df, caba_df])

# %%
print(output_df.head())
print("...")
print(output_df.tail())

# %%
save_output_data(output_df, product["data"])
