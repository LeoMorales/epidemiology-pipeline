import pandas
from matplotlib import pyplot as plt
from epidemiology_package import vis
import seaborn
from epidemiology_package import cleanning
from matplotlib import ticker


def draw_all_and_specific_deceases_barplots(
    upstream, product, causeCodes, applyDataNormalization
):
    df_all_deceases = pandas.read_parquet(
        str(upstream["aggr-deceases-by-sex-by-year-arg"])
    )
    df_cause_specific_deceases = pandas.read_parquet(
        str(upstream["aggr-cause-specific-deceases-by-sex-by-year-arg"])
    )

    all_decease_cols = ["deceases_undetermined", "deceases_female", "deceases_male"]
    all_deceases_data = df_all_deceases[all_decease_cols].set_index(
        df_all_deceases["year"]
    )
    cause_specific_deceases_cols = [
        "cause_specific_deceases_undetermined",
        "cause_specific_deceases_female",
        "cause_specific_deceases_male",
    ]
    cause_specific_deceases_data = df_cause_specific_deceases[
        cause_specific_deceases_cols
    ].set_index(df_cause_specific_deceases["year"])

    fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(20, 10), sharey=True)
    axes = ax.flatten()

    causeCodes = sorted(causeCodes)
    causeCodesTextHead = causeCodes[:5]
    causeCodesTextTail = causeCodes[-5:]
    causeCodesText = list(set(causeCodesTextHead + causeCodesTextTail))

    causeCodesTitleText = ", ".join(
        [str(code) for code in causeCodesText[: len(causeCodesText) // 2]]
    )
    causeCodesTitleText += "..."
    causeCodesTitleText += ", ".join(
        [str(code) for code in causeCodesText[len(causeCodesText) // 2 :]]
    )

    vis.draw_barplot(
        all_deceases_data,
        ax=axes[0],
        displayTotals=False,
        applyDataNormalization=applyDataNormalization,
        displayBarsPercentages=False,
    )

    plotTitle = (
        f"Deceases for selected causes, by year and gender\n{causeCodesTitleText}"
    )
    vis.draw_barplot(
        cause_specific_deceases_data,
        ax=axes[1],
        labelPresentationMapping={
            "cause_specific_deceases_male": "Male",
            "cause_specific_deceases_female": "Female",
            "cause_specific_deceases_undetermined": "Undetermined",
        },
        labelColorMapping={
            "cause_specific_deceases_male": "#5bc0de",
            "cause_specific_deceases_female": "#f46d43",
            "cause_specific_deceases_undetermined": "#abdda4",
        },
        plotTitle=plotTitle,
        displayTotals=False,
        applyDataNormalization=applyDataNormalization,
        displayBarsPercentages=False,
    )

    axes[0].tick_params(axis="x", which="major", labelsize=5)
    axes[1].tick_params(axis="x", which="major", labelsize=5)

    fig.savefig(str(product), dpi=300)


def draw_total_deceases_barplot(upstream, product, applyDataNormalization):
    df_all_deceases = pandas.read_parquet(
        str(upstream["aggr-deceases-by-sex-by-year-arg"])
    )

    all_decease_cols = ["deceases_female", "deceases_male"]
    all_deceases_data = df_all_deceases[all_decease_cols].set_index(
        df_all_deceases["year"]
    )

    fig, ax = plt.subplots(figsize=(18, 8))

    plotTitle = f"Deceases grouped by year and gender"

    vis.draw_barplot(
        all_deceases_data,
        ax=ax,
        plotTitle=plotTitle,
        applyDataNormalization=applyDataNormalization,
    )

    fig.savefig(str(product), dpi=300)


def draw_cause_specific_deceases_barplot(
    upstream, product, causeCodes, applyDataNormalization
):
    df_cuase_specific_deceases = pandas.read_parquet(
        str(upstream["aggr-cause-specific-deceases-by-sex-by-year-arg"])
    )

    all_decease_cols = [
        "cause_specific_deceases_female",
        "cause_specific_deceases_male",
    ]
    all_deceases_data = df_cuase_specific_deceases[all_decease_cols].set_index(
        df_cuase_specific_deceases["year"]
    )

    fig, ax = plt.subplots(figsize=(18, 8))

    causeCodes = sorted(causeCodes)
    causeCodesTextHead = causeCodes[:5]
    causeCodesTextTail = causeCodes[-5:]
    causeCodesText = list(set(causeCodesTextHead + causeCodesTextTail))

    causeCodesTitleText = ", ".join(
        [str(code) for code in causeCodesText[: len(causeCodesText) // 2]]
    )
    causeCodesTitleText += "..."
    causeCodesTitleText += ", ".join(
        [str(code) for code in causeCodesText[len(causeCodesText) // 2 :]]
    )

    plotTitle = (
        f"Deceases for selected causes, by year and gender\n{causeCodesTitleText}"
    )

    vis.draw_barplot(
        all_deceases_data,
        ax=ax,
        plotTitle=plotTitle,
        labelPresentationMapping={
            "cause_specific_deceases_male": "Male",
            "cause_specific_deceases_female": "Female",
            "cause_specific_deceases_undetermined": "Undetermined",
        },
        labelColorMapping={
            "cause_specific_deceases_male": "#5bc0de",
            "cause_specific_deceases_female": "#f46d43",
            "cause_specific_deceases_undetermined": "#abdda4",
        },
        applyDataNormalization=applyDataNormalization,
    )

    fig.savefig(str(product), dpi=300)


def draw_cause_specific_deceases_barplot_categorized_by_sex_by_age(
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


def draw_deceases_barplot_categorized_by_sex_by_age(
    upstream, product, categoryDisplayOrder, catplotArgs
):
    """
    Este gráfico contempla todos los fallecimientos de la Argentina en el período de estudio.
    Dibuja las cantidades contabilizadas según sexo y grupo etario.
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


def draw_all_and_specific_deceases_barchart_by_sex_by_age(
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
