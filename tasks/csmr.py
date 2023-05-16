# cause-specific mortality rate

# The measure that divides the number of deaths of a specific cause by all total deaths
# in a population per year is called the cause-specific mortality rate (CSMR).
#
# The cause-specific mortality rate is a measure of the number of deaths from a specific
# cause that occur in a population over a specific period of time, typically one year,
# per 1,000 people in the population. It is calculated by dividing the number of deaths
# from a specific cause by the total number of deaths in the population and then multiplying
# by 1,000. The cause-specific mortality rate is often used to compare the impact of different
# causes of death on a population or to track changes in mortality rates over time.
#
# For example, the CSMR for heart disease would be calculated by dividing the number of deaths
# from heart disease in a population over a specific period of time by the total number of deaths
# in the population over that same period of time, and then multiplying by 1,000. This would give
# the number of deaths from heart disease per 1,000 people in the population per year.

import pandas
from surnames_package import utils
import seaborn
import matplotlib.pyplot as plt


def calculate_annual_csmr_per_province(upstream, product):
    """
    Return:

           year provincia_id       csmr
        0  1997           02   6.637807
        1  1997           06   5.865480
        2  1997           10  14.689880

    """

    # read data:
    df = pandas.read_parquet(upstream["filter-deceases-for-subset-of-causes-1991-2017"])
    df_all_deceases = pandas.read_parquet(
        upstream["get-deceases-with-age-group-label-1991-2017"]
    )

    columns_selection = ["provincia_id", "codigo_defuncion", "year"]

    # clean specific codes data
    df_subset_deceases_by_province = df[columns_selection].copy()
    df_subset_deceases_by_province = (
        df_subset_deceases_by_province.groupby(by=["year", "provincia_id"])
        .count()
        .reset_index()
        .rename(columns={"codigo_defuncion": "subset_deceases"})
    )

    # clean all deceases data
    df_deceases_by_province = df_all_deceases[columns_selection].copy()
    df_deceases_by_province = (
        df_deceases_by_province.groupby(by=["year", "provincia_id"])
        .count()
        .reset_index()
        .rename(columns={"codigo_defuncion": "deceases"})
    )

    data_rates = pandas.merge(
        df_deceases_by_province,
        df_subset_deceases_by_province,
        on=["year", "provincia_id"],
    )

    # calculate rate
    data_rates["csmr"] = (
        data_rates["subset_deceases"] / data_rates["deceases"]
    ) * 1_000

    # create rate dataframe
    df_csmr = data_rates[["year", "provincia_id", "csmr"]]

    # save
    df_csmr.to_parquet(str(product))


def draw_csmr_heatmap_provincial(upstream, product):
    df_csmr = pandas.read_parquet(upstream["get-annual-csmr-per-province"])
    df_csmr_pivot = df_csmr.pivot(index="provincia_id", columns="year", values="csmr")

    df_csmr_pivot.index.name = None
    df_csmr_pivot.columns.name = None

    df_csmr_pivot = df_csmr_pivot.reset_index().rename(
        columns={"index": "provincia_id"}
    )

    df_csmr_pivot = utils.append_province_description(
        df_csmr_pivot, provinceCodeColumn="provincia_id"
    )

    df_csmr_pivot = df_csmr_pivot.sort_values(
        by=["region_nombre", "provincia_nombre"]
    ).drop(columns=["provincia_id", "region_nombre", "region_id"])

    df_csmr_pivot = df_csmr_pivot[~df_csmr_pivot["provincia_nombre"].isna()]

    f, ax = plt.subplots(figsize=(11, 9))

    # Generate a custom diverging colormap
    cmap = seaborn.diverging_palette(230, 20, as_cmap=True)

    df_csmr_pivot["provincia_nombre"] = (
        df_csmr_pivot["provincia_nombre"]
        .str.replace("Ciudad Autónoma de Buenos Aires", "CABA")
        .str.replace(
            "Tierra del Fuego, Antártida e Islas del Atlántico Sur", "Tierra del Fuego"
        )
    )

    # create the heatmap with Seaborn
    seaborn.heatmap(
        df_csmr_pivot.set_index("provincia_nombre"),
        cmap=cmap,
        square=True,
        linewidths=0.5,
        cbar_kws={"shrink": 0.5},
    )

    # set labels and title
    plt.xlabel("Year")
    plt.ylabel("Province")
    plt.title("Cause-specific mortality rate (CSMR) per province")

    plt.savefig(str(product), dpi=300)
    plt.close()


def calculate_annual_csmr_per_province_by_age_group(upstream, product):
    """
    Return:
            year provincia_id age_group        rate
        0   1997           02     0 - 5  118.760757
        1   1997           02   16 - 35    3.787879
        2   1997           02   36 - 45    7.284079

    """
    # read data:
    df_subset_deceases = pandas.read_parquet(
        upstream["filter-deceases-for-subset-of-causes-1991-2017"]
    )
    df_all_deceases = pandas.read_parquet(
        upstream["get-deceases-with-age-group-label-1991-2017"]
    )

    columns_selection = ["provincia_id", "codigo_defuncion", "year", "age_group"]

    # clean specific codes data
    df_subset_deceases_by_province = df_subset_deceases[columns_selection].copy()
    df_subset_deceases_by_province = (
        df_subset_deceases_by_province.groupby(by=["year", "provincia_id", "age_group"])
        .count()
        .reset_index()
        .rename(columns={"codigo_defuncion": "subset_deceases"})
    )

    # clean all deceases data
    df_deceases_by_province = df_all_deceases[columns_selection].copy()
    df_deceases_by_province = (
        df_deceases_by_province.groupby(by=["year", "provincia_id", "age_group"])
        .count()
        .reset_index()
        .rename(columns={"codigo_defuncion": "deceases"})
    )

    data_rates = pandas.merge(
        df_deceases_by_province,
        df_subset_deceases_by_province,
        on=["year", "provincia_id", "age_group"],
    )

    # calculate rate
    data_rates["csmr"] = (
        data_rates["subset_deceases"] / data_rates["deceases"]
    ) * 1_000

    # create rate dataframe
    df_csmr = data_rates[["year", "provincia_id", "age_group", "csmr"]]

    # save
    df_csmr.to_parquet(str(product))


def pivot_provincial_csmr_by_age_group_data(upstream, product):
    df_csmr = pandas.read_parquet(upstream["get-annual-csmr-per-province-by-age-group"])
    df_csmr_pivot = df_csmr.pivot(
        index=["provincia_id", "age_group"], columns="year", values="csmr"
    )

    df_csmr_pivot.index.name = None
    df_csmr_pivot.columns.name = None

    df_csmr_pivot = df_csmr_pivot.reset_index().rename(
        columns={"index": "provincia_id"}
    )

    # add province information
    df_csmr_pivot = utils.append_province_description(
        df_csmr_pivot, provinceCodeColumn="provincia_id"
    )

    # drop none provinces
    df_csmr_pivot = df_csmr_pivot[~df_csmr_pivot["provincia_nombre"].isna()]

    # order records:
    AGE_GROUP_ORDER_DICT = {
        "0 - 5": 1,
        "6 - 15": 2,
        "16 - 35": 3,
        "36 - 45": 4,
        "46 - 55": 5,
        "56 - 65": 6,
        "66 - 75": 7,
        "76 - 85": 8,
        ">85": 9,
        "NOT-ASSIGNED": 10,
    }
    df_csmr_pivot["age_group_order"] = df_csmr_pivot["age_group"].apply(
        lambda group: AGE_GROUP_ORDER_DICT[group]
    )

    df_csmr_pivot = df_csmr_pivot.sort_values(
        by=["region_nombre", "provincia_nombre", "age_group_order"]
    ).drop(columns=["provincia_id", "region_id", "age_group_order"])

    # delete records without age_group field
    df_csmr_pivot = df_csmr_pivot[df_csmr_pivot["age_group"] != "NOT-ASSIGNED"]

    df_csmr_pivot.to_parquet(str(product))


def draw_csmr_heatmap_provincial_of_region_old(upstream, product, regionName):
    df_csmr_pivot = pandas.read_parquet(
        upstream["csmr-per-province-by-age-group-pivoted"]
    )

    # make province names shorter
    df_csmr_pivot["provincia_nombre"] = (
        df_csmr_pivot["provincia_nombre"]
        .str.replace("Ciudad Autónoma de Buenos Aires", "CABA")
        .str.replace(
            "Tierra del Fuego, Antártida e Islas del Atlántico Sur", "Tierra del Fuego"
        )
    )

    # Get the maximum value of all numeric columns
    max_values = df_csmr_pivot.select_dtypes(include="number").max()
    max_value = max_values.max()

    # Get the minimum value of all numeric columns
    min_values = df_csmr_pivot.select_dtypes(include="number").min()
    min_value = min_values.min()

    # filter region data
    regionData = df_csmr_pivot[df_csmr_pivot["region_nombre"] == regionName].copy()

    # get n provinces in the region
    n_provincias = len(regionData["provincia_nombre"].unique())

    if n_provincias == 0:
        print("No hay provincias")
        return

    # create an index column
    regionData["province_and_age_group"] = (
        regionData["provincia_nombre"] + " | " + regionData["age_group"]
    )
    regionData = regionData.drop(
        columns=["provincia_nombre", "age_group", "region_nombre"]
    )
    regionData = regionData.set_index(["province_and_age_group"])

    single_province_info_height = 3
    figWidth = 8
    figHeight = n_provincias * single_province_info_height
    f, ax = plt.subplots(figsize=(figWidth, figHeight), constrained_layout=True)

    # Generate a custom diverging colormap
    cmap = seaborn.diverging_palette(230, 20, as_cmap=True)

    # create the heatmap with Seaborn
    seaborn.heatmap(
        regionData,
        vmin=min_value,
        vmax=max_value,
        annot=True,
        fmt=".1f",
        annot_kws={"fontsize": 4},
        cmap=cmap,
        square=True,
        linewidths=1.5,
        cbar_kws={"shrink": 0.25},
    )

    # set labels and title
    plt.xlabel("Year")
    plt.ylabel("Province")
    plt.title(f"CSMR by age group, region: {regionName}")

    plt.savefig(str(product), dpi=300, bbox_inches="tight")
    plt.close()


def draw_csmr_heatmap_provincial_of_region(upstream, product, regionName):
    df_csmr_pivot = pandas.read_parquet(
        upstream["csmr-per-province-by-age-group-pivoted"]
    )

    # make province names shorter
    df_csmr_pivot["provincia_nombre"] = (
        df_csmr_pivot["provincia_nombre"]
        .str.replace("Ciudad Autónoma de Buenos Aires", "CABA")
        .str.replace(
            "Tierra del Fuego, Antártida e Islas del Atlántico Sur", "Tierra del Fuego"
        )
    )

    # Get the maximum value of all numeric columns
    max_values = df_csmr_pivot.select_dtypes(include="number").max()
    max_value = max_values.max()

    # Get the minimum value of all numeric columns
    min_values = df_csmr_pivot.select_dtypes(include="number").min()
    min_value = min_values.min()

    # filter region data
    regionData = df_csmr_pivot[df_csmr_pivot["region_nombre"] == regionName].copy()

    # get n provinces in the region
    n_provincias = len(regionData["provincia_nombre"].unique())

    if n_provincias == 0:
        print("No hay provincias")
        return

    # create an index column
    regionData["province_and_age_group"] = (
        regionData["provincia_nombre"] + " | " + regionData["age_group"]
    )
    regionData = regionData.drop(
        columns=["provincia_nombre", "age_group", "region_nombre"]
    )
    regionData = regionData.set_index(["province_and_age_group"])

    single_province_info_height = 3
    figWidth = 8
    figHeight = n_provincias * single_province_info_height

    from epidemiology_package import vis

    vis.draw_heatmap(
        pivoted_data=regionData,
        figWidth=figWidth,
        figHeight=figHeight,
        minValue=min_value,
        maxValue=max_value,
    )
    # TODO: Ejecutar pipelina!!
    # set labels and title
    plt.xlabel("Year")
    plt.ylabel("Province")
    plt.title(f"CSMR by age group, region: {regionName}")

    plt.savefig(str(product), dpi=300, bbox_inches="tight")
    plt.close()
