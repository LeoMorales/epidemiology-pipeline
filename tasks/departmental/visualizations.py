import pandas
import geopandas
from surnames_package import geovis
import matplotlib.pyplot as plt
import shutil
from pathlib import Path


def get_csmr_clustermaps(upstream, product):
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

    CSMR_MAX_VALUE = df_csmr["proportional_mortality_1000"].max()
    CSMR_MIN_VALUE = df_csmr["proportional_mortality_1000"].min()

    for period_i in df_csmr["period"].unique():
        # period_i = '1997-2001'
        df_period = df_csmr[df_csmr["period"] == period_i].copy()
        # df_period["proportional_mortality_1000"] = df_period[
        #    "proportional_mortality_1000"
        # ].fillna(0)

        # combinar datos y capa
        gdf_epof = pandas.merge(
            shape,
            df_period,
            left_on="departamento_id",
            right_on="department_id",
        )

        fig, ax = plt.subplots(figsize=(8, 12))

        geovis.plot_single_choropleth_map(
            dataShape=gdf_epof,
            backgroundShape=shape,
            ax=ax,
            minValue=CSMR_MIN_VALUE,
            maxValue=CSMR_MAX_VALUE,
            columnPlot="proportional_mortality_1000",
            colorMeaningLabel="CSMR (over 1000)",
            plotWithLegend=True,
            mapTitle=f"Mortalidad EPOF cada 1000 personas\n{period_i}",
            mapArgs={"edgecolor": "#666565", "scheme": None},
        )
        filename = parent / f"choropleth-{period_i}.png"
        fig.savefig(filename, dpi=300, bbox_inches="tight")
