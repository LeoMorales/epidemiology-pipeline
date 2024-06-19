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
shape[shape['provincia_id'] == '02']

# %%
caba_shape = geopandas.read_file(
    "https://cdn.buenosaires.gob.ar/datosabiertos/datasets/ministerio-de-educacion/comunas/comunas.geojson"
)

# EPSG:22185 (POSGAR 2007 / Argentina 4) es un sistema de coordenadas proyectado específico para Argentina.
caba_shape = caba_shape.to_crs(crs_destino)
caba_shape.head(3)


# %%
import matplotlib.pyplot as plt

# %%
f, ax = plt.subplots(figsize=(6, 12))

shape.plot(
    ax=ax,
    color='white',
    edgecolor="lightgrey"
)

shape[shape["departamento_nombre"] == "CABA"].plot(
    ax=ax,
    color='green',
    edgecolor="lightgrey"
)

caba_shape.plot(
    color="None",
    ax=ax,
    edgecolor="white"

)

ax.set_xlim(5500000, 5750000)
ax.set_ylim(6500000, 6000000)
ax.set_axis_off()

plt.show();

# %%
shape[shape["departamento_nombre"] == "CABA"]

# %%
shape[shape["departamento_nombre"] == "CABA"]["geometry"].centroid

# %%
