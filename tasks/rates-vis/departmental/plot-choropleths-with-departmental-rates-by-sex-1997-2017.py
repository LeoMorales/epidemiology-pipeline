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
upstream = ["merge-and-rename-departmental-csmr"]

# This is a placeholder, leave it as None
product = None


# %%
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
import contextily as ctx

# %%
# Constants
SHAPE_PATH = "/home/lmorales/work/pipelines/resources/departamentos.geojson"
CRS_DESTINO = "EPSG:22185"
COLUMNAS_SELECCIONADAS = [
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


# %%
def load_data(data_path):
    """Load the CSV data."""
    return pd.read_csv(data_path, dtype={"DEPARTAMENTO": object})


# %%
def load_shapefile(shape_path):
    """Load and reproject the shapefile."""
    shape = gpd.read_file(shape_path)
    shape = shape.to_crs(CRS_DESTINO)
    return shape


# %%
def filter_data(data, period, age_range, sex):
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


# %%
def merge_data(shape, data, left_on, right_on):
    """Merge shape and dataframes."""
    return pd.merge(
        shape, data, left_on=left_on, right_on=right_on, how="left", indicator=True
    )


# %%
def prepare_shape_df(shape_df, columns):
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


# %%
def save_basemap_only_epsg_3857(
    shape_df, basemap_path, margin_across_the_width=0.25, margin_along_the_length=0.1
):
    """
    Save only the basemap using contextily and store the tiles locally.

    Args:
    - shape_df (gpd.GeoDataFrame): GeoDataFrame to get the bounding box for the basemap.
    - basemap_path (str): Path where the basemap image will be saved.
    - margin_across_the_width (float): Percentage margin to add across the width. Default is 0.25.
    - margin_along_the_length (float): Percentage margin to add along the length. Default is 0.1.

    Returns:
    - None
    """
    filepath = Path(basemap_path)

    if filepath.exists():
        print(f"El archivo {basemap_path} ya existe.")
        return

    print("Reprojecting shape to EPSG:3857")
    # Reproyectar a EPSG:3857 si es necesario
    if shape_df.crs.to_string() != "EPSG:3857":
        shape_df = shape_df.to_crs(epsg=3857)

    print("Calculating bounds with margins in EPSG:3857")
    # Calcular límites y expandir con margen en EPSG:3857
    minx, miny, maxx, maxy = shape_df.total_bounds
    x_margin = (maxx - minx) * margin_across_the_width
    y_margin = (maxy - miny) * margin_along_the_length

    # Ajustar límites con margen
    bounds_3857 = (minx - x_margin, miny - y_margin, maxx + x_margin, maxy + y_margin)

    print("Fetching basemap using contextily with bounds in EPSG:3857")
    # Obtener imagen y extensión de la capa base en EPSG:3857
    img, ext = ctx.bounds2raster(
        *bounds_3857, path=basemap_path, source=ctx.providers.CartoDB.PositronNoLabels
    )

    print(f"Basemap saved to {basemap_path}")


# %%
def save_basemap_only_epsg_22185(
    shape_df, basemap_path, margin_across_the_width=0.25, margin_along_the_length=0.1
):
    """
    Save only the basemap using contextily and store the tiles locally.

    Args:
    - shape_df (gpd.GeoDataFrame): GeoDataFrame to get the bounding box for the basemap.
    - basemap_path (str): Path where the basemap image will be saved.
    - margin_across_the_width (float): Percentage margin to add across the width. Default is 0.25.
    - margin_along_the_length (float): Percentage margin to add along the length. Default is 0.1.

    Returns:
    - None
    """
    filepath = Path(basemap_path)

    if filepath.exists():
        print(f"El archivo {basemap_path} ya existe.")
        return

    print("Reprojecting shape to EPSG:22185")
    # Reproyectar a EPSG:22185 si es necesario
    if shape_df.crs.to_string() != "EPSG:22185":
        shape_df = shape_df.to_crs(epsg=22185)

    print("Reprojecting bounds to EPSG:3857 for contextily")
    # Crear un nuevo GeoDataFrame solo para calcular el bounding box reproyectado
    gdf_bounds = gpd.GeoDataFrame(geometry=[shape_df.unary_union], crs="EPSG:22185")
    gdf_bounds = gdf_bounds.to_crs(epsg=3857)
    minx, miny, maxx, maxy = gdf_bounds.total_bounds

    # Ajustar límites reproyectados con margen
    x_margin = (maxx - minx) * margin_across_the_width
    y_margin = (maxy - miny) * margin_along_the_length
    bounds_3857 = (minx - x_margin, miny - y_margin, maxx + x_margin, maxy + y_margin)

    print("Fetching basemap using contextily with bounds in EPSG:3857")
    # Obtener imagen y extensión de la capa base en EPSG:3857
    img, ext = ctx.bounds2raster(
        *bounds_3857,
        path=basemap_path,
        source=ctx.providers.CartoDB.PositronNoLabels,
    )

    print(f"Basemap saved to {basemap_path}")


# %%
import matplotlib.patheffects as path_effects


def plot_data(
    shape_df,
    title,
    ax,
    basemap_path=None,
    margin_across_the_width=0.25,
    margin_along_the_length=0.1,
    label_column="DEPARTAMENTO",
    label_font_size=8,
    color_map="Oranges",
    windrose=True,
    geometry_column="GEOMETRY",
):
    """
    Plots the data on a map with a base layer using contextily, adding geographic reference annotations
    and an optional windrose (compass rose).

    Args:
    - shape_df (gpd.GeoDataFrame): GeoDataFrame containing the shape data.
    - title (str): Title of the plot.
    - ax (matplotlib.axes.Axes): Matplotlib axis object where the map will be plotted.
    - basemap_path (str): Path to the saved basemap image.
    - margin_across_the_width (float): Margin as a fraction across the width.
    - margin_along_the_length (float): Margin as a fraction along the length.
    - label_column (str): Column name to use for labeling regions on the map.
    - label_font_size (int): Font size for labels on the map.
    - color_map (str): Name of the colormap to use for data visualization.
    - windrose (bool): If True, adds a windrose (compass rose) to the map.

    Returns:
    - None: This function modifies the provided axis object directly.
    """
    try:
        # Reproject to a common CRS for basemap compatibility
        if shape_df.crs.to_string() != "EPSG:3857":
            shape_df = shape_df.to_crs(epsg=3857)
        print("Reprojection to EPSG:3857 complete.")

        # Plot areas with missing data in light grey
        shape_df[shape_df["MUERTES_ALZHEIMER"].isna()].plot(
            ax=ax, color="#f0f0f0", edgecolor="#4d4d4d", linewidth=0.75
        )
        print("Plotted areas with missing data.")

        # Plot data with a color map based on the 'TEA*1000' column
        shape_df.plot(
            column="TEA*1000",
            ax=ax,
            legend=True,
            cmap=color_map,
            edgecolor="#000000",
            linewidth=0.25,
            legend_kwds={
                "shrink": 0.5,
                "label": "TEA*1000",
                "orientation": "horizontal",
                "pad": 0.02,
            },
        )
        print("Data plotted with colormap.")

        if label_column:
            # Add labels for each department with geographic centroids
            for idx, row in shape_df.iterrows():
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
            print("Added labels to map.")

        # Set axis properties and title
        ax.set_axis_off()
        ax.set_title(title, fontsize=14)

        # Calculate bounds and expand by margin
        minx, miny, maxx, maxy = shape_df.total_bounds
        x_margin = (maxx - minx) * margin_across_the_width
        y_margin = (maxy - miny) * margin_along_the_length

        # Set limits with margin
        ax.set_xlim(minx - x_margin, maxx + x_margin)
        ax.set_ylim(miny - y_margin, maxy + y_margin)
        print(f"Set axis limits with margins: x_margin={x_margin}, y_margin={y_margin}")

        # Add basemap using contextily
        if basemap_path:
            ctx.add_basemap(ax, crs=shape_df.crs.to_string(), source=basemap_path)
        else:
            ctx.add_basemap(
                ax,
                crs=shape_df.crs.to_string(),
                source=ctx.providers.CartoDB.PositronNoLabels,
            )
        print("Basemap added successfully.")

        # Add windrose (compass rose) if specified
        if windrose:
            add_windrose(ax)

        print(f"Map plotted successfully with title: {title}")

    except Exception as e:
        print(f"Error in plot_data function: {e}")
        raise


def add_windrose(ax, position=(0.8, 0.1), size=0.05):
    """
    Adds a windrose (compass rose) to the map.

    Args:
    - ax (matplotlib.axes.Axes): Matplotlib axis object where the windrose will be added.
    - position (tuple of float): Position of the windrose in axis coordinates (x, y).
    - size (float): Size of the windrose relative to the axis size.

    Returns:
    - None: This function modifies the provided axis object directly.
    """
    # Draw the compass rose with slight adjustments for aesthetics
    x, y = position
    windrose_radius = size

    # Compass directions
    directions = [
        "S",
        "W",
        "N",
        "E",
    ]
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

    # Draw the center of the windrose
    ax.plot(x, y, "o", color="black", transform=ax.transAxes, markersize=4)

    # Draw diagonal lines for a more complete windrose effect
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

    print("Windrose added to map with enhanced styling.")


# %%
DATA_PATH = str(upstream["merge-and-rename-departmental-csmr"]["data"])

data_path = DATA_PATH
shape_path = SHAPE_PATH

# Load data and shapefile
data = load_data(data_path)
shape = load_shapefile(shape_path)
shape = shape.drop(columns=["departamento_nombre"])

# Filter data for different demographics
df_1997_2017_female = filter_data(data, "1997-2017", "65-150", "female")
df_1997_2017_male = filter_data(data, "1997-2017", "65-150", "male")
df_1997_2017_all = filter_data(data, "1997-2017", "65-150", "all")

# Merge data with shapefile
shape_female = merge_data(shape, df_1997_2017_female, "departamento_id", "DEPARTAMENTO")
shape_male = merge_data(shape, df_1997_2017_male, "departamento_id", "DEPARTAMENTO")
shape_all = merge_data(shape, df_1997_2017_all, "departamento_id", "DEPARTAMENTO")

# Prepare shape dataframes for plotting
shape_female = prepare_shape_df(shape_female, COLUMNAS_SELECCIONADAS)
shape_male = prepare_shape_df(shape_male, COLUMNAS_SELECCIONADAS)
shape_all = prepare_shape_df(shape_all, COLUMNAS_SELECCIONADAS)

# Guardar capa base contextily
save_basemap_only_epsg_3857(
    shape_all,
    "/home/lmorales/resources/contextly/argentina_CartoDB_PositronNoLabels_basemap_epsg_3857.tif",
)

# Guardar capa base contextily
save_basemap_only_epsg_22185(
    shape_all,
    "/home/lmorales/resources/contextly/argentina_CartoDB_PositronNoLabels_basemap_epsg_22185.tif",
    margin_across_the_width=0.5,
    margin_along_the_length=0.5,
)

# Plot data
f, ax = plt.subplots(ncols=3, nrows=1, figsize=(24, 12))
shapes = [shape_female, shape_male, shape_all]
titles = ["Sexo mujeres", "Sexo varones", "Ambos sexos"]

for ax, shape, title in zip(ax.flatten(), shapes, titles):
    plot_data(
        shape,
        title,
        ax,
        basemap_path="/home/lmorales/resources/contextly/argentina_CartoDB_PositronNoLabels_basemap_epsg_22185.tif",
        label_column=None,
    )


plt.suptitle(
    "TEA*1000 por departamentos; Rango etario: mayores de 64 años; Periodo: 1997-2017",
    ha="center",
    y=0.95,
)

output_file_name = str(product["img"])
plt.savefig(output_file_name, dpi=300)
plt.close()

# %%
