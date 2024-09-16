# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: tags,-all
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

# %% [markdown]
# # CSMR in Choropleth Maps

# %% [markdown]
# ## Importación de bibliotecas

# %%
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
import matplotlib.colors as mcolors
from typing import List, Optional, Union, Tuple, Dict
from matplotlib.cm import ScalarMappable

# %%

DATA_PATH: str = (
    "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/clean/departamental-deceases-tidy-df.parquet"
)
DATA_PATH: str = str(upstream["get-departamental-deceases-tidy-df"]["data"])
SHAPE_PATH: str = "/home/lmorales/work/pipelines/resources/departamentos.geojson"
CRS_DESTINO: str = "EPSG:22185"
BASEMAP_PATH: str = (
    "/home/lmorales/resources/contextly/argentina_CartoDB_PositronNoLabels_basemap_epsg_22185.tif"
)
RATE_COLUMN: str = "rate"


# %% [markdown]
# ## Función: Asignar Etiquetas de Períodos Dinámicos


# %%
def asignar_etiquetas_periodos(
    df: pd.DataFrame, period_bins: List[int], period_labels: List[str]
) -> pd.DataFrame:
    """
    Asigna etiquetas de períodos dinámicos basados en los rangos proporcionados.

    Verifica la existencia de la columna 'year' antes de procesar.

    :param df: DataFrame con la columna 'year'.
    :param period_bins: Lista de enteros que representan los límites de los períodos.
    :param period_labels: Lista de etiquetas para los períodos.
    :return: DataFrame con la columna 'period' asignada.

    :raises ValueError: Si 'year' no está presente o los bins y labels no son coherentes.
    """
    if "year" not in df.columns:
        raise ValueError("La columna 'year' no está presente en el DataFrame.")

    if len(period_bins) - 1 != len(period_labels):
        raise ValueError(
            "El número de etiquetas debe ser uno menos que el número de bins."
        )

    df["period"] = pd.cut(df["year"], bins=period_bins, labels=period_labels)

    return df


# %% [markdown]
# ## Función: Calcular Tasas (Muertes Específicas vs No Específicas)


# %%
def calculate_rates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula las tasas dividiendo cantidades específicas por cantidades no específicas
    agrupadas por department_id y period.

    :param df: DataFrame con las columnas ['department_id', 'period', 'is_specific', 'counts'].
    :return: DataFrame con las tasas calculadas.

    :raises ValueError: Si el DataFrame no contiene las columnas requeridas.
    """
    required_columns = {"department_id", "period", "is_specific", "counts"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"El DataFrame debe contener las columnas: {required_columns}")

    # Filtrar los datos por específicos y no específicos
    specific_df: pd.DataFrame = df[df["is_specific"] == True]
    nonspecific_df: pd.DataFrame = df[df["is_specific"] == False]

    # Agrupar y sumar los 'counts' para cada combinación de 'department_id' y 'period'
    specific_counts: pd.DataFrame = (
        specific_df.groupby(["department_id", "period"], observed=True, as_index=False)[
            "counts"
        ]
        .sum()
        .rename(columns={"counts": "specific_counts"})
    )
    nonspecific_counts: pd.DataFrame = (
        nonspecific_df.groupby(
            ["department_id", "period"], observed=True, as_index=False
        )["counts"]
        .sum()
        .rename(columns={"counts": "nonspecific_counts"})
    )

    # Unir ambos DataFrames sobre department_id y period
    merged_df: pd.DataFrame = pd.merge(
        specific_counts, nonspecific_counts, on=["department_id", "period"], how="outer"
    )

    # Calcular las tasas de manera segura (evitar división por cero)
    merged_df[RATE_COLUMN] = merged_df.apply(
        lambda row: safe_division(row["specific_counts"], row["nonspecific_counts"]),
        axis=1,
    )

    return merged_df


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


# %% [markdown]
# ## Función: Fusionar Datos Geográficos


# %%
def merge_data(
    shape: gpd.GeoDataFrame, data: pd.DataFrame, left_on: str, right_on: str
) -> gpd.GeoDataFrame:
    """
    Une un GeoDataFrame con un DataFrame basado en columnas específicas.

    :param shape: GeoDataFrame a ser unido.
    :param data: DataFrame con datos.
    :param left_on: Nombre de la columna en el GeoDataFrame para la unión.
    :param right_on: Nombre de la columna en el DataFrame para la unión.
    :return: GeoDataFrame combinado.
    """
    return pd.merge(
        shape, data, left_on=left_on, right_on=right_on, how="left", indicator=True
    )


# %% [markdown]
# ## Función: Preparar el GeoDataFrame para Visualización


# %%
def prepare_shape_df(
    shape_df: gpd.GeoDataFrame, crs: str = "EPSG:3857"
) -> gpd.GeoDataFrame:
    """
    Prepara un GeoDataFrame para su visualización.

    :param shape_df: GeoDataFrame con geometrías.
    :param crs: Sistema de referencia de coordenadas objetivo.
    :return: GeoDataFrame preparado.
    """
    shape_df = shape_df.copy()

    if shape_df.crs.to_string() != crs:
        crs_code = int(crs.split(":")[-1])
        shape_df = shape_df.to_crs(epsg=crs_code)

    shape_df["latitud"] = shape_df["geometry"].centroid.y
    shape_df["longitud"] = shape_df["geometry"].centroid.x
    shape_df = shape_df.set_geometry("geometry")
    shape_df = shape_df.sort_values(
        by=["provincia_nombre", "departamento_id"]
    ).reset_index(drop=True)

    return shape_df


# %% [markdown]
# ## Función: Calcular Rango Común de Colores para los Mapas


# %%
def calculate_common_color_range(
    *dfs: gpd.GeoDataFrame, column: str = "rate"
) -> Tuple[float, float]:
    """
    Calcula los valores mínimo y máximo del rango de color en múltiples dataframes.

    :param dfs: GeoDataFrames a analizar.
    :param column: Columna sobre la que se calculará el rango.
    :return: Tupla con el valor mínimo y máximo.
    """
    min_val: float = min(df[column].min() for df in dfs)
    max_val: float = max(df[column].max() for df in dfs)
    return min_val, max_val


# %% [markdown]
# ## Función: Filtrar Datos por Período (de Manera Dinámica)


# %%
def filter_data_by_periods(
    data: pd.DataFrame, periods: List[str]
) -> Dict[str, pd.DataFrame]:
    """
    Filtra los datos por los diferentes períodos y devuelve un diccionario con los DataFrames filtrados.

    :param data: DataFrame con la columna 'period'.
    :param periods: Lista de períodos a filtrar.
    :return: Diccionario donde las llaves son los nombres de los períodos y los valores son los DataFrames filtrados.
    """
    return {
        period: data[data["period"] == period].reset_index(drop=True)
        for period in periods
    }


# %% [markdown]
# ## Función: Visualización de los Mapas


# %%
def plot_data(
    shape_df: gpd.GeoDataFrame,
    title: str,
    ax: plt.Axes,
    color_map: str,
    vmin: float,
    vmax: float,
    plot_column: str = "rate",
    basemap_path: Optional[str] = None,
    label_column: Optional[str] = None,
    label_font_size: int = 8,
    geometry_column: str = "geometry",
) -> plt.Axes:
    """
    Plotea los datos en un mapa con un colormap unificado.

    :param shape_df: GeoDataFrame con datos y geometrías.
    :param title: Título del mapa.
    :param ax: Eje de matplotlib.
    :param color_map: Mapa de colores a usar.
    :param vmin: Valor mínimo para el colormap.
    :param vmax: Valor máximo para el colormap.
    :param plot_column: Columna a visualizar en el mapa.
    :param basemap_path: Path opcional para un mapa base.
    :param label_column: Columna para añadir etiquetas en el mapa.
    :param label_font_size: Tamaño de la fuente de las etiquetas.
    :param geometry_column: Columna con la geometría.
    :return: El objeto Axes con el mapa generado.
    """
    if shape_df.crs.to_string() != "EPSG:3857":
        shape_df = shape_df.to_crs(epsg=3857)

    shape_df[shape_df[plot_column].isna()].plot(
        ax=ax, color="#f0f0f0", edgecolor="#4d4d4d", linewidth=0.75
    )

    im = shape_df.plot(
        column=plot_column,
        ax=ax,
        cmap=color_map,
        edgecolor="#000000",
        linewidth=0.25,
        vmin=vmin,
        vmax=vmax,
    )

    if label_column:
        for _, row in shape_df.iterrows():
            if not pd.isna(row[label_column]):
                ax.text(
                    row[geometry_column].centroid.x,
                    row[geometry_column].centroid.y,
                    s=row[label_column],
                    fontsize=label_font_size,
                    ha="center",
                    color="black",
                )

    ax.set_axis_off()
    ax.set_title(title, fontsize=14)

    ctx.add_basemap(
        ax,
        crs=shape_df.crs.to_string(),
        source=(
            ctx.providers.CartoDB.PositronNoLabels
            if basemap_path is None
            else basemap_path
        ),
    )

    return im


# %%
# ### Función para visualizar los mapas de múltiples períodos
def plot_multiple_periods(
    shape_data_by_periods: Dict[str, gpd.GeoDataFrame],
    periods: List[str],
    color_map: str,
    vmin: float,
    vmax: float,
    basemap_path: str,
    rate_column: str,
    fig_size: Tuple[int, int] = (12, 6),
    cbar_position: Tuple[float, float, float, float] = [0.3, -0.05, 0.4, 0.03],
    output_file: str = "",
) -> None:
    """
    Función que visualiza mapas de múltiples períodos con un colorbar común.

    :param shape_data_by_periods: Diccionario con GeoDataFrames para cada período.
    :param periods: Lista de períodos a visualizar.
    :param color_map: Nombre del colormap a utilizar.
    :param vmin: Valor mínimo para la normalización de colores.
    :param vmax: Valor máximo para la normalización de colores.
    :param basemap_path: Path del basemap.
    :param rate_column: Columna que contiene las tasas a visualizar.
    :param fig_size: Tamaño de la figura de matplotlib.
    :param cbar_position: Posición y tamaño del colorbar.
    """
    # Crear subplots para los diferentes períodos
    fig, axes = plt.subplots(ncols=len(periods), figsize=fig_size)

    for ax, (period, shape) in zip(axes, shape_data_by_periods.items()):
        plot_data(
            shape_df=shape,
            plot_column=rate_column,
            title=period,
            ax=ax,
            color_map=color_map,
            vmin=vmin,
            vmax=vmax,
            basemap_path=basemap_path,
            label_column=None,
        )

    # Añadir barra de color común debajo de los mapas
    cbar_ax = fig.add_axes(cbar_position)
    sm = ScalarMappable(cmap=color_map, norm=mcolors.Normalize(vmin=vmin, vmax=vmax))
    sm._A = []  # Necesario para inicializar el ScalarMappable
    fig.colorbar(sm, cax=cbar_ax, orientation="horizontal", shrink=0.7)

    if len(output_file) != 0:
        plt.savefig(output_file, dpi=300)

    # Ajustar los espacios y mostrar
    plt.tight_layout()
    plt.show()


# %%
if __name__ == "__main__":
    # Cargar el archivo de datos en formato Parquet
    data: pd.DataFrame = pd.read_parquet(DATA_PATH)
    shape: gpd.GeoDataFrame = gpd.read_file(SHAPE_PATH)

    # Definir los períodos dinámicos
    period_bins: List[int] = [1997, 2003, 2010, 2017]
    period_labels: List[str] = ["1997-2003", "2004-2010", "2011-2017"]

    # Asignar los períodos al DataFrame
    data = asignar_etiquetas_periodos(data, period_bins, period_labels)

    # Calcular las tasas
    grouped_data: pd.DataFrame = (
        data.groupby(by=["department_id", "period", "is_specific"], observed=True)[
            "counts"
        ]
        .sum()
        .reset_index()
    )
    rates_df: pd.DataFrame = calculate_rates(grouped_data)

    # Filtrar los datos dinámicamente por períodos
    periods: List[str] = period_labels
    filtered_data_by_periods: Dict[str, pd.DataFrame] = filter_data_by_periods(
        rates_df, periods
    )

    # Preparar el shapefile con los datos
    shape_data_by_periods: Dict[str, gpd.GeoDataFrame] = {
        period: prepare_shape_df(
            merge_data(shape, df, "departamento_id", "department_id")
        )
        for period, df in filtered_data_by_periods.items()
    }

    # Obtener el rango de colores común
    vmin, vmax = calculate_common_color_range(
        *shape_data_by_periods.values(), column=RATE_COLUMN
    )

    # Llamar a la función para generar la visualización de los diferentes períodos
    plot_multiple_periods(
        shape_data_by_periods=shape_data_by_periods,
        periods=periods,
        color_map="Oranges",
        vmin=vmin,
        vmax=vmax,
        basemap_path=BASEMAP_PATH,
        rate_column=RATE_COLUMN,
        fig_size=(12, 6),
        cbar_position=[0.3, 0.05, 0.4, 0.03],  # Posición para el colorbar
        output_file=str(product["img"]),
    )

# %%
