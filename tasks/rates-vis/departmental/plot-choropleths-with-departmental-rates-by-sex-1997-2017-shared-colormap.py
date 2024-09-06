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
upstream = ["merge-and-rename-departmental-csmr"]

# This is a placeholder, leave it as None
product = None

# %% Imports
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
import matplotlib.patheffects as path_effects
import matplotlib.colors as mcolors
from typing import List, Optional, Tuple

# %% Constants
SHAPE_PATH: str = "/home/lmorales/work/pipelines/resources/departamentos.geojson"
CRS_DESTINO: str = "EPSG:22185"
COLUMNAS_SELECCIONADAS: List[str] = [
    "PROVINCIA_ID",
    "PROVINCIA_NOMBRE",
    "DEPARTAMENTO_ID",
    "DEPARTAMENTO_NOMBRE",
    "GEOMETRY",
    "PERIODO",
    "RANGO_EDADES",
    "SEXO",
    "MUERTES_TOTALES",
    "MUERTES_ALZHEIMER",
    "TEA*1000",
]
# DATA_PATH = "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates-processing/departmental-csmr-merged-and-renamed.csv"
DATA_PATH: str = str(upstream["merge-and-rename-departmental-csmr"]["data"])
BASEMAP_PATH: str = (
    "/home/lmorales/resources/contextly/argentina_CartoDB_PositronNoLabels_basemap_epsg_22185.tif"
)


# %% Utility Functions
def load_data(data_path: str) -> pd.DataFrame:
    """Load the CSV data."""
    return pd.read_csv(data_path, dtype={"DEPARTAMENTO": object})


def load_shapefile(shape_path: str) -> gpd.GeoDataFrame:
    """Load and reproject the shapefile."""
    shape = gpd.read_file(shape_path)
    shape = shape.to_crs(CRS_DESTINO)
    return shape


def filter_data(
    data: pd.DataFrame, period: str, age_range: str, sex: str
) -> pd.DataFrame:
    """Filter data based on age range, sex, and period."""
    return (
        data[
            (data["RANGO_EDADES"] == age_range)
            & (data["SEXO"] == sex)
            & (data["PERIODO"] == period)
        ]
        .reset_index(drop=True)
        .copy()
    )


def merge_data(
    shape: gpd.GeoDataFrame, data: pd.DataFrame, left_on: str, right_on: str
) -> gpd.GeoDataFrame:
    """Merge shape and dataframes."""
    return pd.merge(
        shape, data, left_on=left_on, right_on=right_on, how="left", indicator=True
    )


def prepare_shape_df(
    shape_df: gpd.GeoDataFrame, columns: List[str]
) -> gpd.GeoDataFrame:
    """Prepare shape dataframe for plotting."""
    shape_df.columns = [col.upper() for col in shape_df.columns]
    shape_df = shape_df[columns].copy()
    shape_df["LATITUD"] = shape_df["GEOMETRY"].centroid.y
    shape_df["LONGITUD"] = shape_df["GEOMETRY"].centroid.x
    shape_df = shape_df.set_geometry("GEOMETRY")
    shape_df = shape_df.sort_values(
        by=["PROVINCIA_NOMBRE", "DEPARTAMENTO_ID"]
    ).reset_index(drop=True)
    return shape_df


def calculate_common_color_range(
    *dfs: gpd.GeoDataFrame, column: str = "TEA*1000"
) -> Tuple[float, float]:
    """Calculate the min and max values across multiple dataframes."""
    min_val = min(df[column].min() for df in dfs)
    max_val = max(df[column].max() for df in dfs)
    return min_val, max_val


# %% Plotting Functions
def plot_data(
    shape_df: gpd.GeoDataFrame,
    title: str,
    ax: plt.Axes,
    color_map: str,
    vmin: float,
    vmax: float,
    basemap_path: Optional[str] = None,
    label_column: Optional[str] = "DEPARTAMENTO",
    label_font_size: int = 8,
    geometry_column: str = "GEOMETRY",
    windrose: bool = True,
) -> plt.Axes:
    """Plot the data on a map with a unified colormap."""
    if shape_df.crs.to_string() != "EPSG:3857":
        shape_df = shape_df.to_crs(epsg=3857)

    shape_df[shape_df["MUERTES_ALZHEIMER"].isna()].plot(
        ax=ax, color="#f0f0f0", edgecolor="#4d4d4d", linewidth=0.75
    )

    im = shape_df.plot(
        column="TEA*1000",
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
                    path_effects=[
                        path_effects.withStroke(linewidth=2, foreground="white")
                    ],
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

    if windrose:
        add_windrose(ax)

    return im


def add_windrose(
    ax: plt.Axes, position: Tuple[float, float] = (0.8, 0.1), size: float = 0.05
) -> None:
    """Adds a windrose (compass rose) to the map."""
    x, y = position
    windrose_radius = size
    directions = ["S", "W", "N", "E"]
    offsets = [
        (0, windrose_radius),
        (windrose_radius, 0),
        (0, -windrose_radius),
        (-windrose_radius, 0),
    ]

    for dir_text, (dx, dy) in zip(directions, offsets):
        ax.annotate(
            dir_text,
            xy=(x + dx, y + dy),
            xytext=(x - dx * 1.5, y - dy * 1.5),
            ha="center",
            va="center",
            fontsize=12,
            fontweight="bold",
            color="darkgrey",
            arrowprops=dict(facecolor="darkgrey", shrink=0.5, width=1.25, headwidth=5),
            xycoords=ax.transAxes,
        )

    ax.plot(x, y, "o", color="black", transform=ax.transAxes, markersize=4)
    ax.plot(
        [x - windrose_radius * 0.7, x + windrose_radius * 0.7],
        [y - windrose_radius * 0.7, y + windrose_radius * 0.7],
        color="black",
        linewidth=0.5,
        transform=ax.transAxes,
    )
    ax.plot(
        [x - windrose_radius * 0.7, x + windrose_radius * 0.7],
        [y + windrose_radius * 0.7, y - windrose_radius * 0.7],
        color="black",
        linewidth=0.5,
        transform=ax.transAxes,
    )


# %% Load and Prepare Data
data: pd.DataFrame = load_data(DATA_PATH)
shape: gpd.GeoDataFrame = load_shapefile(SHAPE_PATH)
shape = shape.drop(columns=["departamento_nombre"])

df_1997_2017_female: pd.DataFrame = filter_data(data, "1997-2017", "65-150", "female")
df_1997_2017_male: pd.DataFrame = filter_data(data, "1997-2017", "65-150", "male")
df_1997_2017_all: pd.DataFrame = filter_data(data, "1997-2017", "65-150", "all")

shape_female: gpd.GeoDataFrame = merge_data(
    shape, df_1997_2017_female, "departamento_id", "DEPARTAMENTO"
)
shape_male: gpd.GeoDataFrame = merge_data(
    shape, df_1997_2017_male, "departamento_id", "DEPARTAMENTO"
)
shape_all: gpd.GeoDataFrame = merge_data(
    shape, df_1997_2017_all, "departamento_id", "DEPARTAMENTO"
)

shape_female = prepare_shape_df(shape_female, COLUMNAS_SELECCIONADAS)
shape_male = prepare_shape_df(shape_male, COLUMNAS_SELECCIONADAS)
shape_all = prepare_shape_df(shape_all, COLUMNAS_SELECCIONADAS)

# Calculate common color range
vmin, vmax = calculate_common_color_range(shape_female, shape_male, shape_all)

# %% Plot unified map with common color range and single colorbar
f, ax = plt.subplots(ncols=3, nrows=1, figsize=(24, 12))

# Adjust space between subplots
f.subplots_adjust(wspace=-0.5, hspace=0.1)

shapes = [shape_female, shape_male, shape_all]
titles = ["Women", "Men", "Both Women/Men"]
color_map = "Oranges"

for ax, shape, title in zip(ax.flatten(), shapes, titles):
    plot_data(
        shape,
        title,
        ax,
        color_map=color_map,
        vmin=vmin,
        vmax=vmax,
        basemap_path=BASEMAP_PATH,
        label_column=None,
    )

# Add a single colorbar below the three maps
cbar_ax = f.add_axes([0.3, 0.05, 0.4, 0.03])  # Position for colorbar below the plots
sm = plt.cm.ScalarMappable(cmap=color_map, norm=mcolors.Normalize(vmin=vmin, vmax=vmax))
sm._A = []
f.colorbar(sm, cax=cbar_ax, orientation="horizontal", shrink=0.7)

plt.suptitle(
    "",
    ha="center",
    y=0.95,
)

# Save the figure without extra margins
output_file_name = str(product["img"])
plt.savefig(output_file_name, dpi=300, bbox_inches="tight", pad_inches=0.1)

plt.show()
plt.close()
