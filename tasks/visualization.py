import pandas
from matplotlib import pyplot as plt
from epidemiology_package import vis
import seaborn
from epidemiology_package import cleanning
from matplotlib import ticker


def draw_deceases_lineplots(upstream, product, causeCodes):
    df_all_deceases = pandas.read_parquet(
        str(upstream["aggr-deceases-1991-2017-by-year-by-sex-arg"])
    )
    df_deceases_of_interest = pandas.read_parquet(
        str(upstream["aggr-cause-specific-deceases-1991-2017-by-year-by-sex-arg"])
    )

    # 3. combine
    df = pandas.merge(df_all_deceases, df_deceases_of_interest, on="year", how="left")

    male = df["deceases_varon"].tolist()
    female = df["deceases_mujer"].tolist()
    male_in_subset_of_causes = df["deceases_subset_causes_varon"].tolist()
    female_in_subset_of_causes = df["deceases_subset_causes_mujer"].tolist()
    year = [int(year) for year in df["year"]]

    fig, ax = plt.subplots(nrows=2, ncols=1, sharex=True, figsize=(16, 16), dpi=300)

    axes = ax.flatten()
    vis.draw_lineplot_comparission(year, male, female, ax=axes[0], displayValues=False)
    axes[0].set_title("Total")
    vis.draw_lineplot_comparission(
        year,
        male_in_subset_of_causes,
        female_in_subset_of_causes,
        ax=axes[1],
        diffColor="#490F62",
        displayValues=False,
    )

    cause_codes_text = ", ".join([str(code) for code in causeCodes[:10]])
    if len(causeCodes) > 10:
        cause_codes_text += "..."
    axes[1].set_title(f"Deceases for selected causes\n{cause_codes_text}")
    plt.suptitle("Deceases by year and gender", fontsize=20)
    # plt.show()
    fig.savefig(str(product), dpi=300)


def draw_incidence_lineplots(upstream, product):
    """Dibuja las incidencias totales, varones y mujeres en dos graficos
    lado a lado, uno para las incidencias generales y otro para las locales.
    """
    incidence_df = pandas.read_parquet(str(upstream["get-subset-of-causes-incidence"]))

    f, ax = plt.subplots(nrows=2, ncols=1, sharex=True, sharey=True, figsize=(8, 14))

    axes = ax.flatten()

    vis.draw_lineplot_comparission(
        incidence_df["year"].astype(int).values,
        incidence_df["causes_incidence"].values,
        incidence_df["causes_incidence_female"].values,
        ax=axes[0],
        aSerieLabel="Causes incidence",
        bSerieLabel="Subset of causes\nincidence in females",
        aSerieColor="grey",
        bSerieColor="#f46d43",
        diffColor="white",
        plotTitle=None,
        displayValues=False,
    )

    vis.draw_lineplot_comparission(
        incidence_df["year"].astype(int).values,
        incidence_df["causes_incidence_female"].values,
        incidence_df["causes_incidence_male"].values,
        ax=axes[0],
        aSerieLabel="",
        bSerieLabel="Subset of causes\nincidence in males",
        aSerieColor="#f46d43",
        bSerieColor="#5bc0de",
        diffColor="#490F62",
        plotTitle="Male and Female causes incidence:\nCases (total, male and female) per 100,000 deaths",
        displayValues=False,
        formatFloatValues=True,
    )

    title = """
    Causes incidence: Cases per 100,000 deaths
    Causes Local Male incidence: Cases (male) per 100,000 male deaths
    Causes Local Female incidence: Cases (female) per 100,000 female deaths
    """

    ax = vis.draw_lineplot_comparission(
        incidence_df["year"].astype(int).values,
        incidence_df["causes_incidence_male_local"].values,
        incidence_df["causes_incidence_female_local"].values,
        aSerieLabel="Causes Local Male incidence",
        bSerieLabel="Causes Local Female incidence",
        ax=axes[1],
        aSerieColor="#5bc0de",
        bSerieColor="#f46d43",
        diffColor="#490F62",
        displayValues=False,
    )

    vis.draw_lineplot_comparission(
        incidence_df["year"].astype(int).values,
        incidence_df["causes_incidence"].values,
        incidence_df["causes_incidence_female_local"].values,
        ax=axes[1],
        aSerieColor="#919191",
        bSerieColor="#f46d43",
        aSerieLabel="Selected causes incidence",
        bSerieLabel="",
        diffColor="#80638c",
        plotTitle=title,
        displayValues=False,
    )

    plt.savefig(str(product), dpi=300, bbox_inches="tight")
    plt.close()


def draw_barchart_for_deceases_from_specific_causes_by_sex_and_grouped_by_age(
    upstream, product, categoryDisplayOrder, causeName, catplotArgs
):
    """
    Dibuja un grafico de barras categorizadas por grupo etario.
    """
    df = pandas.read_parquet(str(upstream["filter-cause-specific-deceases-1991-2017"]))

    df["sex"] = cleanning.get_sex_correspondence(df["sexo"])
    counts_df = (
        df[["provincia_id", "age_group", "sex"]]
        .groupby(["age_group", "sex"])
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "count"})
        .replace({"mujer": "Female", "varon": "Male"})
    )

    counts_df = counts_df[~(counts_df["sex"] == "indeterminado")]

    # Draw a nested barplot by age group and sex
    seaborn.set_theme(style="whitegrid")
    catplotPalette = catplotArgs["palette_two_soft"]
    g = seaborn.catplot(
        data=counts_df,
        kind="bar",
        x="age_group",
        y="count",
        hue="sex",
        alpha=0.6,
        height=6,
        order=categoryDisplayOrder,
        palette=catplotPalette,
    )

    g.despine(left=True)
    g.set_axis_labels("", "Deceases (u)")
    g.legend.set_title("")

    ticklabelsSize = 12 if len(categoryDisplayOrder) < 5 else 8
    g.ax.set_xticklabels(categoryDisplayOrder, size=ticklabelsSize)

    g.fig.suptitle(f"{causeName} deceases by age group and sex (1991-2017)", y=1.01)
    g.savefig(str(product), dpi=300)


def draw_barchart_for_all_deceases_by_sex_and_grouped_by_age(
    upstream, product, categoryDisplayOrder, catplotArgs
):
    """
    Dibuja todos los fallecimientos de la Argentina en el período de estudio en un
    grafico de barras categorizadas por grupo etario.
    """
    df = pandas.read_parquet(str(upstream["get-deceases-1991-2017"]))

    df["sex"] = cleanning.get_sex_correspondence(df["sexo"])
    counts_df = (
        df[["provincia_id", "age_group", "sex"]]
        .groupby(["age_group", "sex"])
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "count"})
        .replace({"mujer": "Female", "varon": "Male"})
    )

    # renombramos provincia_id pero podría haber sido cualquiera de las columnas
    counts_df = counts_df[~(counts_df["sex"] == "indeterminado")]

    #   age_group     sex   count
    # 1     0 - 5  female  151127
    # 2     0 - 5    male  193697
    # 3   16 - 35  female  103172
    # 4   ...

    # Draw a nested barplot by age group and sex
    seaborn.set_theme(style="whitegrid")
    catplotPalette = catplotArgs["palette_two"]
    g = seaborn.catplot(
        data=counts_df,
        kind="bar",
        x="age_group",
        y="count",
        hue="sex",
        alpha=0.6,
        height=6,
        order=categoryDisplayOrder,
        palette=catplotPalette,
    )

    g.despine(left=True)
    g.set_axis_labels("", "Deceases (u)")
    ticklabelsSize = 12 if len(categoryDisplayOrder) < 5 else 8
    g.ax.set_xticklabels(categoryDisplayOrder, size=ticklabelsSize)
    g.legend.set_title("")
    g.fig.suptitle("Deceases in Argentina by age group and sex (1991-2017)")
    g.ax.get_yaxis().set_major_formatter(
        ticker.FuncFormatter(lambda x, p: format(int(x), ","))
    )

    g.savefig(str(product), dpi=300)


def draw_barchart_for_deceases_by_sex_and_grouped_by_age(
    upstream, product, categoryDisplayOrder, causeName, catplotArgs
):
    """
    Dibuja todos los fallecimientos de la Argentina en el período de estudio en un
    grafico de barras categorizadas por grupo etario.
    """
    df = pandas.read_parquet(str(upstream["get-deceases-1991-2017"]))
    df_specific_causes = pandas.read_parquet(
        str(upstream["filter-cause-specific-deceases-1991-2017"])
    )

    df["sex"] = cleanning.get_sex_correspondence(df["sexo"])
    counts_df = (
        df[["provincia_id", "age_group", "sex"]]
        .groupby(["age_group", "sex"])
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "count"})
        .replace({"mujer": "Female", "varon": "Male"})
    )
    # renombramos provincia_id pero podría haber sido cualquiera de las columnas
    counts_df = counts_df[~(counts_df["sex"] == "indeterminado")]

    df_specific_causes["sex"] = cleanning.get_sex_correspondence(
        df_specific_causes["sexo"]
    )
    counts_df_specific_causes = (
        df_specific_causes[["provincia_id", "age_group", "sex"]]
        .groupby(["age_group", "sex"])
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "count"})
        .replace({"mujer": f"Female ({causeName})", "varon": f"Male ({causeName})"})
    )
    counts_df_specific_causes = counts_df_specific_causes[
        ~(counts_df_specific_causes["sex"] == "indeterminado")
    ]

    # Draw a nested barplot by age group and sex
    catplotData = counts_df.append(counts_df_specific_causes).sort_values(by="sex")
    catplotPalette = catplotArgs["palette_four"]
    seaborn.set_theme(style="whitegrid")
    g = seaborn.catplot(
        data=catplotData,
        kind="bar",
        x="age_group",
        y="count",
        hue="sex",
        hue_order=["Female", f"Female ({causeName})", "Male", f"Male ({causeName})"],
        alpha=0.6,
        height=8,
        order=categoryDisplayOrder,
        palette=catplotPalette,
    )

    g.despine(left=True)
    g.set_axis_labels("", "Deceases (u)")
    ticklabelsSize = 12 if len(categoryDisplayOrder) < 5 else 8
    g.ax.set_xticklabels(categoryDisplayOrder, size=ticklabelsSize)
    g.legend.set_title("")
    g.fig.suptitle("Deceases in Argentina. By age group and sex (1991-2017)")
    g.ax.get_yaxis().set_major_formatter(
        ticker.FuncFormatter(lambda x, p: format(int(x), ","))
    )

    g.savefig(str(product), dpi=300)


def draw_age_grouping_rates_lineplot(upstream, product, ageCategoriesOrder):
    rates_df = pandas.read_parquet(str(upstream["get-annual-rates-grouping-by-age"]))
    rates_df_wide = rates_df.pivot(
        index="year", columns="age_group", values="rate_1000"
    )
    rates_df_wide.index.name = None
    rates_df_wide.columns.name = None

    col_order = ageCategoriesOrder

    rates_df_wide = rates_df_wide[col_order]
    plt.style.use("fivethirtyeight")
    f, ax = plt.subplots(figsize=(18, 12))

    lp = seaborn.lineplot(data=rates_df_wide, ax=ax)

    # labels = [
    #     label.get_text()
    #     for label
    #     in list(ax.get_legend().get_texts())
    # ]

    plt.legend(
        title="Age groups (ranges in years)",
        bbox_to_anchor=(1.01, 0.8),
        borderaxespad=0,
    )

    ax.spines[["right", "top"]].set_visible(False)

    plt.xticks(fontsize=12)
    plt.savefig(str(product), dpi=300, bbox_inches="tight")
    plt.close()
