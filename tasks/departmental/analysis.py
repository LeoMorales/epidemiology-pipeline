import pandas
import geopandas
from surnames_package import geovis
import matplotlib.pyplot as plt
import shutil
from pathlib import Path
from surnames_package import spatial
from surnames_package import spatial_vis


def get_csmr_moran_clustermaps(upstream, product):
    parent = Path(product)
    # clean up products from the previous run, if any. Otherwise they'll be
    # mixed with the current files
    if parent.exists():
        shutil.rmtree(parent)

    # make sure the directory exists
    parent.mkdir(exist_ok=True, parents=True)

    shape_path = "/home/lmorales/work/pipelines/resources/departamentos.geojson"
    shape = geopandas.read_file(shape_path)

    df_csmr = pandas.read_parquet(upstream["get-departmental-csmr-by-period"])

    NEIGHBORHOOD_STRATEGY = "knn"
    NEIGHBORHOOD_PARAM = 6
    USE_MORAN_COL = "proportional_mortality_1000"

    for period_i in df_csmr["period"].unique():
        # period_i = '1997-2001'
        df_period = df_csmr[df_csmr["period"] == period_i].copy()

        # combinar datos y capa
        gdf_epof = pandas.merge(
            shape,
            df_period,
            left_on="departamento_id",
            right_on="department_id",
        )

        # calcular Moran global y locales
        weights, moran, lisa = spatial.get_spatials(
            gdf_epof,
            attribute=USE_MORAN_COL,
            strategy=NEIGHBORHOOD_STRATEGY,
            k_neighbours=NEIGHBORHOOD_PARAM,
            use_moran_bv=False,
            moran_bv_column=None,
            verbose=False,
        )

        SIGNIFICANCE_LIMIT = 0.05
        # asignar etiquetas de moran local a cada departamento
        quadfilter = (lisa.p_sim <= (SIGNIFICANCE_LIMIT)) * (lisa.q)
        spot_labels = ["NS", "HH", "LH", "LL", "HL"]
        moran_lisa_labels = [spot_labels[i] for i in quadfilter]
        gdf_epof["moran_lisa_label"] = moran_lisa_labels

        no_data_shape = shape[
            ~shape["departamento_id"].isin(df_period["department_id"].unique())
        ]

        fig, ax = plt.subplots(figsize=(8, 12))

        spatial_vis.draw_clustermap(
            dataShape=gdf_epof,
            noDataShape=no_data_shape,
            figureTitle=f"Moran Lisa Clustermap: CSMR ({period_i})\nMoran's I: {moran.I:0.2f} p-value: {moran.p_sim:0.3f}",
            outputPath="",
            moranLisaColumn="moran_lisa_label",
            tileFilepath="/home/lmorales/resources/contextly/argentina.tif",
            ax=ax,
            plot_kwargs={
                "tranparency_dict": {
                    "NS": 0.4,
                    "HH": 0.75,
                    "LL": 0.75,
                    "HL": 0.8,
                    "LH": 0.99,
                },
                "linewidth_dict": {
                    "NS": 0.99,
                    "HH": 0.9,
                    "LL": 0.9,
                    "HL": 1.5,
                    "LH": 1.5,
                },
            },
        )
        filename = parent / f"moran-{period_i}.png"
        fig.savefig(filename, dpi=300, bbox_inches="tight")
