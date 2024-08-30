# -*- coding: utf-8 -*-
# %% tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["get-departmental-csmr-presentation"]

# This is a placeholder, leave it as None
product = None


# %%
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

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
def plot_data(shape_df, title, ax):
    """Plot the data on a map."""
    shape_df[shape_df["MUERTES_ALZHEIMER"].isna()].plot(
        ax=ax, color="None", edgecolor="#4d4d4d", linewidth=0.75
    )

    shape_df.plot(
        column="TEA*1000",
        ax=ax,
        legend=True,
        cmap="Oranges",
        edgecolor="#000000",
        linewidth=0.25,
        legend_kwds={"shrink": 0.3},
    )
    ax.set_axis_off()
    ax.set_title(title)


# %%
DATA_PATH = "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates-processing/departmental-csmr-presentation.csv"
#DATA_PATH = str(upstream["get-departmental-csmr-presentation"]["data"])

data_path = DATA_PATH
shape_path = SHAPE_PATH

# Load data and shapefile
data = load_data(data_path)
shape = load_shapefile(shape_path)

# Filter data for different demographics
df_1997_2017_female = filter_data(data, "1997-2017", "65-150", "female")
df_1997_2017_male = filter_data(data, "1997-2017", "65-150", "male")
df_1997_2017_all = filter_data(data, "1997-2017", "65-150", "all")

# Merge data with shapefile
shape_female = merge_data(
    shape, df_1997_2017_female, "departamento_id", "DEPARTAMENTO"
)
shape_male = merge_data(shape, df_1997_2017_male, "departamento_id", "DEPARTAMENTO")
shape_all = merge_data(shape, df_1997_2017_all, "departamento_id", "DEPARTAMENTO")

# Prepare shape dataframes for plotting
shape_female = prepare_shape_df(shape_female, COLUMNAS_SELECCIONADAS)
shape_male = prepare_shape_df(shape_male, COLUMNAS_SELECCIONADAS)
shape_all = prepare_shape_df(shape_all, COLUMNAS_SELECCIONADAS)


# %%
shape_all.head()

# %%
df_1997_2017_all.shape

# %%
df_1997_2017_all.head()

# %%
df_1997_2017_all["PROVINCIA"].unique()

# %%
df_1997_2017_all[df_1997_2017_all["PROVINCIA"] == "Buenos Aires"].sort_values(
    by="MUERTES_TOTALES", ascending=False
)

# %%
data.head()

# %%

# %%

# Plot data
f, ax = plt.subplots(ncols=3, nrows=1, figsize=(24, 12))
shapes = [shape_female, shape_male, shape_all]
titles = ["Sexo mujeres", "Sexo varones", "Ambos sexos"]

for ax, shape, title in zip(ax.flatten(), shapes, titles):
    plot_data(shape, title, ax)

plt.suptitle(
    "TEA*1000 por departamentos; Rango etario: mayores de 64 años; Periodo: 1997-2017",
    ha="center",
    y=0.94,
)

plt.savefig(output_file, dpi=300)
plt.show()
plt.close()


# %%
def main(data_path, shape_path, output_file):
    # Load data and shapefile
    data = load_data(data_path)
    shape = load_shapefile(shape_path)

    # Filter data for different demographics
    df_1997_2017_female = filter_data(data, "65-150", "female", "1997-2017")
    df_1997_2017_male = filter_data(data, "65-150", "male", "1997-2017")
    df_1997_2017_all = filter_data(data, "65-150", "all", "1997-2017")

    # Merge data with shapefile
    shape_female = merge_data(
        shape, df_1997_2017_female, "departamento_id", "DEPARTAMENTO"
    )
    shape_male = merge_data(shape, df_1997_2017_male, "departamento_id", "DEPARTAMENTO")
    shape_all = merge_data(shape, df_1997_2017_all, "departamento_id", "DEPARTAMENTO")

    # Prepare shape dataframes for plotting
    shape_female = prepare_shape_df(shape_female, COLUMNAS_SELECCIONADAS)
    shape_male = prepare_shape_df(shape_male, COLUMNAS_SELECCIONADAS)
    shape_all = prepare_shape_df(shape_all, COLUMNAS_SELECCIONADAS)

    # Plot data
    f, ax = plt.subplots(ncols=3, nrows=1, figsize=(24, 12))
    shapes = [shape_female, shape_male, shape_all]
    titles = ["Sexo mujeres", "Sexo varones", "Ambos sexos"]

    for ax, shape, title in zip(ax.flatten(), shapes, titles):
        plot_data(shape, title, ax)

    plt.suptitle(
        "TEA*1000 por departamentos; Rango etario: mayores de 64 años; Periodo: 1997-2017",
        ha="center",
        y=0.94,
    )

    plt.savefig(output_file, dpi=300)
    plt.show()
    plt.close()


# %%
if __name__ == "__main__":
    # DATA_PATH = "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates-processing/presentation-departmental-csmr.csv"
    DATA_PATH = str(upstream["get-departmental-csmr-presentation"]["data"])
    output_file_name = str(product["img"])
    main(DATA_PATH, SHAPE_PATH, output_file=output_file_name)
