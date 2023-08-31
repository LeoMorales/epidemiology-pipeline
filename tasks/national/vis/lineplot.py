import pandas
from matplotlib import pyplot as plt
from epidemiology_package import vis
import seaborn
from epidemiology_package import cleanning
from matplotlib import ticker


def draw_annual_csmr_lineplots(upstream, product):
    csmr_df = pandas.read_parquet(str(upstream["get-national-annual-csmr"]))
    csmr_df = csmr_df[csmr_df["csmr"] != 0]

    male = csmr_df["csmr_male"].tolist()
    female = csmr_df["csmr_female"].tolist()

    year = [int(year) for year in csmr_df["year"]]

    fig, ax = plt.subplots(figsize=(16, 8), dpi=300)

    vis.draw_lineplot_comparission(
        year,
        male,
        female,
        ax=ax,
        displayValues=False,
        aSerieLabel="CSMR Male",
        bSerieLabel="CSMR Female",
    )

    vis.draw_lineplot(
        year,
        csmr_df["csmr"].tolist(),
        ax=ax,
        valuesLabel="CSMR",
        lineColor="grey",
    )

    plt.suptitle("CSMR by year and sex", fontsize=20)
    ax.set_title("Cause specific deceases over 1,000 deceases")

    plt.savefig(str(product), dpi=300, bbox_inches="tight")
    plt.close()
