import pandas
from matplotlib import pyplot as plt
from epidemiology_package import vis
import seaborn
from epidemiology_package import cleanning
from matplotlib import ticker


def draw_deceases_barplots(upstream, product, causeCodes):
    df_all_deceases = pandas.read_parquet(
        str(upstream["aggr-deceases-1991-2017-by-year-by-sex-arg"])
    )
    df_deceases_of_interest = pandas.read_parquet(
        str(upstream["aggr-cause-specific-deceases-1991-2017-by-year-by-sex-arg"])
    )

    # 3. combine
    df = pandas.merge(df_all_deceases, df_deceases_of_interest, on="year", how="left")

    decease_cols = ["deceases_indeterminado", "deceases_mujer", "deceases_varon"]
    decease_data = df[decease_cols].set_index(df["year"])
    cause_deceases_cols = [
        "deceases_subset_causes_indeterminado",
        "deceases_subset_causes_mujer",
        "deceases_subset_causes_varon",
    ]
    cause_deceases_cols = [col for col in cause_deceases_cols if (col in df.columns)]
    cause_deceases_data = df[cause_deceases_cols].set_index(df["year"])

    fig, ax = plt.subplots(nrows=2, ncols=1, figsize=(20, 16), sharex=True)
    axes = ax.flatten()

    cause_codes_text = ", ".join([str(code) for code in causeCodes[:10]])

    vis.draw_barplot(decease_data, ax=axes[0])
    vis.draw_barplot(
        cause_deceases_data,
        ax=axes[1],
        labelPresentationMapping={
            "deceases_subset_causes_varon": "Male",
            "deceases_subset_causes_mujer": "Female",
            "deceases_subset_causes_indeterminado": "Undetermined",
        },
        labelColorMapping={
            "deceases_subset_causes_varon": "#5bc0de",
            "deceases_subset_causes_mujer": "#f46d43",
            "deceases_subset_causes_indeterminado": "#abdda4",
        },
        plotTitle=f"Deceases for selected causes, by year and gender\n{cause_codes_text}",
    )

    fig.savefig(str(product), dpi=300)


def draw_total_deceases_barplot(upstream, product, applyDataNormalization):
    df_all_deceases = pandas.read_parquet(
        str(upstream["aggr-deceases-1991-2017-by-year-by-sex-arg"])
    )

    decease_cols = ["deceases_mujer", "deceases_varon"]
    decease_data = df_all_deceases[decease_cols].set_index(df_all_deceases["year"])

    fig, ax = plt.subplots(figsize=(18, 8))

    plotTitle = f"Deceases  bygrouped by year and gender"

    vis.draw_barplot(
        decease_data,
        ax=ax,
        plotTitle=plotTitle,
        applyDataNormalization=applyDataNormalization,
    )

    fig.savefig(str(product), dpi=300)


def draw_cause_specific_deceases_barplot(
    upstream, product, causeCodes, applyDataNormalization
):
    df_cuase_specific_deceases = pandas.read_parquet(
        str(upstream["aggr-cause-specific-deceases-1991-2017-by-year-by-sex-arg"])
    )

    decease_cols = [
        "deceases_subset_causes_mujer",
        "deceases_subset_causes_varon",
    ]
    decease_data = df_cuase_specific_deceases[decease_cols].set_index(
        df_cuase_specific_deceases["year"]
    )

    fig, ax = plt.subplots(figsize=(18, 8))

    causeCodesText = ", ".join([str(code) for code in causeCodes[:10]])
    plotTitle = f"Deceases for selected causes, by year and gender\n{causeCodesText}"

    vis.draw_barplot(
        decease_data,
        ax=ax,
        plotTitle=plotTitle,
        labelPresentationMapping={
            "deceases_subset_causes_varon": "Male",
            "deceases_subset_causes_mujer": "Female",
            "deceases_subset_causes_indeterminado": "Undetermined",
        },
        labelColorMapping={
            "deceases_subset_causes_varon": "#5bc0de",
            "deceases_subset_causes_mujer": "#f46d43",
            "deceases_subset_causes_indeterminado": "#abdda4",
        },
        applyDataNormalization=applyDataNormalization,
    )

    fig.savefig(str(product), dpi=300)
