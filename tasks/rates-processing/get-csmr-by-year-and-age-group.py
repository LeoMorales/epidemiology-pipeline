# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: tags,title,-all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: work
#     language: python
#     name: python3
# ---

# %% tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["get-departamental-deceases-tidy-df"]

# This is a placeholder, leave it as None
product = None

# %% Importar bibliotecas
import pandas as pd
from typing import List, Union

# %% Definir constantes
#DATA_PATH = "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/clean/departamental-deceases-tidy-df.parquet"
DATA_PATH: str = upstream["get-departamental-deceases-tidy-df"]["data"]
RATE_COLUMN: str = "rate"


def safe_division(
    numerator: Union[int, float], denominator: Union[int, float]
) -> float:
    """
    Realiza una división segura entre el numerador y el denominador.

    :param numerator: El numerador de la división.
    :param denominator: El denominador de la división.
    :return: El resultado de la división, o 0 si el denominador es 0.
    """
    return numerator / denominator if denominator != 0 else 0.0


def calculate_rates(df: pd.DataFrame, group_columns: List[str]) -> pd.DataFrame:
    """
    Calcula las tasas dividiendo cantidades específicas por cantidades no específicas,
    agrupadas por las columnas especificadas.

    :param df: DataFrame con las columnas ['department_id', 'period', 'is_specific', 'counts'].
    :param group_columns: Lista de columnas por las cuales agrupar los datos.
    :return: DataFrame con las tasas calculadas.

    :raises ValueError: Si el DataFrame no contiene las columnas requeridas.
    """
    required_columns = {"is_specific", "counts"}.union(group_columns)

    if not required_columns.issubset(df.columns):
        missing_cols = required_columns - set(df.columns)
        raise ValueError(f"El DataFrame debe contener las columnas: {missing_cols}")

    # Filtrar los datos por específicos y no específicos
    specific_df: pd.DataFrame = df[df["is_specific"] == True]
    nonspecific_df: pd.DataFrame = df[df["is_specific"] == False]

    # Agrupar y sumar los 'counts' para cada combinación de columnas en group_columns
    specific_counts: pd.DataFrame = (
        specific_df.groupby(group_columns, observed=True, as_index=False)["counts"]
        .sum()
        .rename(columns={"counts": "specific_counts"})
    )
    nonspecific_counts: pd.DataFrame = (
        nonspecific_df.groupby(group_columns, observed=True, as_index=False)["counts"]
        .sum()
        .rename(columns={"counts": "nonspecific_counts"})
    )

    # Unir ambos DataFrames en función de las group_columns
    merged_df: pd.DataFrame = pd.merge(
        specific_counts, nonspecific_counts, on=group_columns, how="outer"
    )

    # Calcular las tasas de manera segura
    merged_df[RATE_COLUMN] = merged_df.apply(
        lambda row: safe_division(row["specific_counts"], row["nonspecific_counts"]),
        axis=1,
    )

    return merged_df


def add_total_rows(
    df: pd.DataFrame, group_columns: List[str], total_column: str = "age_group"
) -> pd.DataFrame:
    """
    Agrega filas de totales al DataFrame por cada subgrupo de 'year'.

    :param df: DataFrame con los resultados de tasas por grupo.
    :param group_columns: Lista de columnas por las cuales agrupar los datos.
    :param total_column: Nombre de la columna en la que se indicará 'Total'.
    :return: DataFrame con filas de totales añadidas.
    """
    total_rows = []

    # Agrupar por los valores en group_columns menos el último, que sería el "total_column"
    for group_values, group_df in df.groupby(group_columns[:-1]):
        total_row = {col: group_values[i] for i, col in enumerate(group_columns[:-1])}
        total_row[total_column] = "Total"
        total_row["specific_counts"] = group_df["specific_counts"].sum()
        total_row["nonspecific_counts"] = group_df["nonspecific_counts"].sum()
        total_row[RATE_COLUMN] = safe_division(
            total_row["specific_counts"], total_row["nonspecific_counts"]
        )
        total_rows.append(total_row)

    # Concatenar las filas de totales con el DataFrame original
    return pd.concat([df, pd.DataFrame(total_rows)], ignore_index=True)


def sort_age_groups(
    df: pd.DataFrame, age_group_column: str = "age_group"
) -> pd.DataFrame:
    """
    Ordena el DataFrame en función de 'year' y 'age_group', donde 'age_group' tiene un orden específico.

    :param df: DataFrame con las columnas 'year' y 'age_group'.
    :param age_group_column: Columna que contiene los grupos de edad a ordenar.
    :return: DataFrame ordenado.
    """
    # Definir el orden de los grupos de edad
    age_group_order = [
        "<=5",
        "6-15",
        "16-35",
        "36-45",
        "46-55",
        "56-65",
        "66-75",
        "76-85",
        "85+",
        "Total",
    ]

    # Convertir la columna 'age_group' a categórico con el orden específico
    df[age_group_column] = pd.Categorical(
        df[age_group_column], categories=age_group_order, ordered=True
    )

    # Ordenar por 'year' y 'age_group'
    return df.sort_values(by=["year", age_group_column])


# %% Leer el archivo de datos
df: pd.DataFrame = pd.read_parquet(DATA_PATH)

rates_by_age_group: pd.DataFrame = calculate_rates(
    df, group_columns=["year", "age_group"]
)

output_df: pd.DataFrame = add_total_rows(
    rates_by_age_group, group_columns=["year", "age_group"]
)

df_sorted: pd.DataFrame = sort_age_groups(output_df)

df_sorted["specific_counts"] = df_sorted["specific_counts"].fillna(0).astype(int)
df_sorted["rate"] = df_sorted["rate"].fillna(0)
df_sorted = df_sorted.reset_index(drop=True)

# %%
print(df_sorted.head(20))

# %% Guardar el DataFrame en un archivo CSV
df_sorted.to_csv(str(product["data"]), index=False)
