import numpy
from matplotlib import pyplot as plt
import matplotlib
import seaborn
import pandas


def __normalize_data(df):
    """Normaliza los valores de un dataframe por fila.

    Args:
        df (pandas.Dataframe): Dataframe con esquema: cada fila tiene la información para un año.
        Cada columna contiene una variable, la suma por fila da el 100% para el año.

    Returns:
        pandas.Dataframe: Normalizado por fila.
    """
    normalized_df = df.copy()
    df_columns = df.columns

    normalized_df["snum"] = normalized_df.sum(axis=1)

    for col in df_columns:
        # pisar las variables originales por su versión normalizada
        normalized_df[col] = normalized_df[col] / normalized_df["snum"] * 100

    normalized_df = normalized_df.drop(columns="snum")
    return normalized_df


def draw_barplot(
    data: pandas.core.frame.DataFrame,
    labelPresentationMapping: dict = {
        "deceases_varon": "Male",
        "deceases_mujer": "Female",
        "deceases_indeterminado": "Undetermined",
    },
    labelColorMapping: dict = {
        "deceases_varon": "#5bc0de",
        "deceases_mujer": "#f46d43",
        "deceases_indeterminado": "#abdda4",
    },
    plotTitle: str = "Deceases by year and gender",
    yLabel: str = "Deceases",
    barLabelFontSize: int = 8,
    applyDataNormalization: bool = False,
    displayTotals: bool = True,
    displayBarsPercentages: bool = True,
    ax: matplotlib.axes._subplots.Axes = None,
):
    """
    Genera un grafico de barras apiladas. Cada barra representa a un año.
    Cada valor apilado es el valor de la columna para ese año.

    Args:
        data (pandas.core.frame.DataFrame): Una barra por cada renglon. Una sub-barra apilada por cada columna.
        labelPresentationMapping (_type_, optional): Cómo se va a presentar cada columna. Defaults to { "deceases_varon": "Male", "deceases_mujer": "Female", "deceases_indeterminado": "Undetermined", }.
        labelColorMapping (_type_, optional): El color que va a tener cada columna. Defaults to { "deceases_varon": "#5bc0de", "deceases_mujer": "#f46d43", "deceases_indeterminado": "#abdda4", }.
        plotTitle (str, optional): Título del gráfico. Defaults to "Deceases by year and gender".
        yLabel (str, optional): Denominación del eje y. Defaults to "Deceases".
        barLabelFontSize (int, optional): Tamaño de fuente de las etiquetas de las barras. Defaults to 8.
        applyDataNormalization (bool, optional): Indican si se deben apilar barras que representan porcentajes o no. Defaults to False.
        ax (matplotlib.axes._subplots.Axes, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    if applyDataNormalization:
        data = __normalize_data(data)

    if not ax:
        fig, ax = plt.subplots(figsize=(18, 8))

    # Initialize the bottom at zero for the first set of bars.
    bottom = numpy.zeros(len(data))

    # Plot each layer of the bar, adding each bar to the "bottom" so
    # the next bar starts higher.
    for i, col in enumerate(data.columns[::-1]):
        ax.bar(
            data.index,
            data[col],
            bottom=bottom,
            label=labelPresentationMapping[col],
            color=labelColorMapping[col],
        )

        if applyDataNormalization and displayBarsPercentages:
            for i_label, base, value in zip(data.index, bottom, data[col]):
                ypos = base + (value / 2)
                ax.text(i_label, ypos, "%.1f" % value, ha="center", va="center")

        bottom += numpy.array(data[col])

    if (not applyDataNormalization) and displayTotals:
        # Sum up the rows of our data to get the total value of each bar.
        totals = data.sum(axis=1)
        # Set an offset that is used to bump the label up a bit above the bar.
        y_offset = max(totals) * 0.01
        # Add labels to each bar.
        for i, total in enumerate(totals):
            ax.text(
                totals.index[i],
                total + y_offset,
                round(total),
                ha="center",
                fontsize=barLabelFontSize,
                weight="bold",
            )

    if plotTitle:
        ax.set_title(plotTitle)

    ax.spines[["right", "top"]].set_visible(False)

    ax.set_ylabel(yLabel, rotation=90, labelpad=10)
    xticks_labels = [str(index) for index in data.index]
    ax.legend()

    return ax


def draw_lineplot_comparission(
    xSerie,
    aSerie,
    bSerie,
    ax=None,
    aSerieLabel="Male",
    bSerieLabel="Female",
    aSerieColor="#5bc0de",
    bSerieColor="#f46d43",
    diffColor="grey",
    plotTitle="-",
    displayValues=False,
    formatFloatValues=False,
):
    if not ax:
        _, ax = plt.subplots(figsize=(16, 16), dpi=300)

    ymax = max(aSerie)
    ymin = min(bSerie)

    ax.plot(xSerie, aSerie, marker=".", color=aSerieColor, linewidth=3)
    ax.plot(xSerie, bSerie, marker=".", color=bSerieColor, linewidth=3)

    legend_label_xpos = xSerie[-1] + 0.2
    ax.text(
        legend_label_xpos,
        aSerie[-1] - (aSerie[-1] * 0.01),
        aSerieLabel,
        size=12,
        color=aSerieColor,
        wrap=True,
    )
    ax.text(
        legend_label_xpos,
        bSerie[-1] - (bSerie[-1] * 0.01),
        bSerieLabel,
        size=12,
        color=bSerieColor,
        wrap=True,
    )

    if displayValues:
        offsetFactor = 0.01 if aSerie[-1] > 100_000 else 0.1
        yOffset = aSerie[-1] * offsetFactor
        for i, xValue in enumerate(xSerie):
            yValueA = aSerie[i]
            yValueB = bSerie[i]

            yOffsetA = yOffset * -1 if yValueA < yValueB else yOffset
            yOffsetB = yOffsetA * -1
            ax.text(
                xValue,
                yValueA + yOffsetA,
                f"{yValueA:,.2f}" if formatFloatValues else f"{yValueA:,}",
                size=5,
                horizontalalignment="center",
                verticalalignment="bottom",
                color="black",
                wrap=True,
            )

            ax.text(
                xValue,
                yValueB + yOffsetB,
                f"{yValueB:,.2f}" if formatFloatValues else f"{yValueB:,}",
                size=5,
                horizontalalignment="center",
                verticalalignment="bottom",
                color="black",
                wrap=True,
            )

    ax.fill_between(xSerie, aSerie, bSerie, color=diffColor, alpha=0.3)
    ax.spines[["right", "top"]].set_visible(False)

    # Rotates and right-aligns the x labels so they don't crowd each other.
    ax.set_xticks(xSerie)
    ax.set_xticklabels(xSerie, rotation=45, ha="center")

    ax.grid(True, color="lightgrey", alpha=0.5)

    if plotTitle:
        ax.set_title(plotTitle)

    return ax


def draw_lineplot(
    xPositions,
    values,
    ax=None,
    valuesLabel="Male",
    lineColor="#5bc0de",
):
    if not ax:
        _, ax = plt.subplots(figsize=(16, 16), dpi=300)

    ax.plot(xPositions, values, marker=".", color=lineColor, linewidth=3)

    legend_label_xpos = xPositions[-1] + 0.2
    ax.text(
        legend_label_xpos,
        values[-1] - (values[-1] * 0.01),
        valuesLabel,
        size=16,
        color=lineColor,
        wrap=True,
    )

    ax.spines[["right", "top"]].set_visible(False)
    return ax


def draw_stacked_plot(
    sorted_plot_data,
    only_save=False,
    output_file="delete.png",
    title="Stacked barplot",
    colors=["#7fc97f", "#beaed4", "#fdc086", "#ffff99", "#386cb0", "#f0027f"],
    legend_n_rows=2,
    legend_font_size=12,
):
    seaborn.set()
    font_color = "#525252"
    csfont = {"fontname": "Calibri"}  # title font
    hfont = {"fontname": "Calibri"}  # main font

    ax = sorted_plot_data.plot.barh(
        align="center",
        stacked=True,
        figsize=(10, 0.45 * len(sorted_plot_data)),
        color=colors,
    )

    title = plt.title(title, pad=60, fontsize=18, color=font_color, **csfont)

    title.set_position([0.5, 0.85])

    # Adjust the subplot so that the title would fit
    # plt.subplots_adjust(top=0.8, left=0.26)

    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_fontsize(15)

    plt.xticks(color=font_color, **hfont)
    plt.yticks(color=font_color, **hfont)

    data_n_cols = len(sorted_plot_data.columns)
    n_col_legend = (
        int(data_n_cols / legend_n_rows)
        if data_n_cols % legend_n_rows == 0
        else (data_n_cols // legend_n_rows) + 1
    )
    print(n_col_legend)
    # legend = plt.legend(
    #     loc='center',
    #     frameon=False,
    #     bbox_to_anchor=(0., 0., 0., .102),
    #     mode='expand',
    #     ncol=n_col_legend,
    #     borderaxespad=0,
    #     prop={'size': legend_font_size, 'family':'Calibri'})

    legend = plt.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.05),
        frameon=False,
        fancybox=True,
        ncol=n_col_legend,
        prop={"size": legend_font_size, "family": "Calibri"},
    )
    for text in legend.get_texts():
        plt.setp(text, color=font_color)  # legend font color

    # plt.tight_layout()

    if only_save:
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()
        return None
    else:
        plt.show()
        return ax


def draw_heatmap(
    pivoted_data, figWidth, figHeight, minValue=None, maxValue=None, cellFontSize=4
):
    """
    Dibuja un mapa de calor para los datos recibidos.
    Es posible especificar los valores maximo y minimo porque existe la posibilidad de
    crear multiples graficos separados pero que mantengan la misma escala. Sino se los
    especifica, se calculan. <-- no es necesario!
    """
    f, ax = plt.subplots(figsize=(figWidth, figHeight), constrained_layout=True)

    # Generate a custom diverging colormap
    cmap = seaborn.diverging_palette(230, 20, as_cmap=True)

    heatmap_args = {
        "annot": True,
        "fmt": ".1f",
        "annot_kws": {"fontsize": cellFontSize},
        "cmap": cmap,
        "square": True,
        "linewidths": 1.5,
        "cbar_kws": {"shrink": 0.25},
    }

    if minValue and maxValue:
        heatmap_args["vmin"] = minValue
        heatmap_args["vmax"] = maxValue

    # create the heatmap with Seaborn
    plot_axes = seaborn.heatmap(pivoted_data, **heatmap_args)

    return plot_axes
