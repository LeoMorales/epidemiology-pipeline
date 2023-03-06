import numpy
from matplotlib import pyplot as plt

def draw_barplot(
        data, ax,
        label_mapping = {
            'deceases_varon': 'Male',
            'deceases_mujer': 'Female',
            'deceases_indeterminado': 'Undetermined',
        },
        color_mapping = {
            'deceases_varon': '#5bc0de',
            'deceases_mujer': '#f46d43',
            'deceases_indeterminado': '#abdda4',
        },
        plotTitle="Deceases by year and gender",
        yLabel='Deceases'
    ):
    """
    Args:
        data (pandas.Dataframe):
            Una barra apilada por cada columna.
            Una columna por cada renglon (index).
    """
    # Initialize the bottom at zero for the first set of bars.
    bottom = numpy.zeros(len(data))

    
    # Plot each layer of the bar, adding each bar to the "bottom" so
    # the next bar starts higher.
    for i, col in enumerate(data.columns[::-1]):
        ax.bar(
            data.index,
            data[col],
            bottom=bottom,
            label=label_mapping[col],
            color=color_mapping[col]
        )
        
        bottom += numpy.array(data[col])

    # Sum up the rows of our data to get the total value of each bar.
    totals = data.sum(axis=1)
    # Set an offset that is used to bump the label up a bit above the bar.
    y_offset = max(totals) * .01
    # Add labels to each bar.
    for i, total in enumerate(totals):
        ax.text(
            totals.index[i],
            total + y_offset,
            round(total),
            ha='center',
            fontsize=8,
            weight='bold')

    if plotTitle:
        ax.set_title(plotTitle)

    ax.spines[['right', 'top']].set_visible(False)

    ax.set_ylabel(yLabel, rotation=90, labelpad=10)
    xticks_labels = [str(index) for index in data.index]
    #plt.xticks(xticks_labels, size=8, rotation=45)

    ax.legend()

def draw_lineplot_comparission(
        xSerie, aSerie, bSerie,
        ax=None,
        aSerieLabel="Male",
        bSerieLabel="Female",
        aSerieColor = '#5bc0de',
        bSerieColor = '#f46d43',
        diffColor='grey',
        plotTitle="-",
        displayValues=False,
        formatFloatValues=False
    ):

    if not ax:
        _, ax = plt.subplots(
            figsize=(16,16), dpi=300)

    ymax = max(aSerie)
    ymin = min(bSerie)
    
    ax.plot(xSerie, aSerie, marker=".", color=aSerieColor, linewidth=3)
    ax.plot(xSerie, bSerie, marker=".", color=bSerieColor, linewidth=3)

    legend_label_xpos = xSerie[-1] + .2
    ax.text(
        legend_label_xpos,
        aSerie[-1] - (aSerie[-1] * .01),
        aSerieLabel,
        size=16,
        color=aSerieColor,
        wrap=True)
    ax.text(
        legend_label_xpos,
        bSerie[-1] - (bSerie[-1] * .01),
        bSerieLabel,
        size=16,
        color=bSerieColor,
        wrap=True)

    if displayValues:
        offsetFactor = .01 if aSerie[-1] > 100_000 else .1
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
                horizontalalignment='center',
                verticalalignment='bottom',
                color="black",
                wrap=True)

            ax.text(
                xValue,
                yValueB + yOffsetB,
                f"{yValueB:,.2f}" if formatFloatValues else f"{yValueB:,}",
                size=5,
                horizontalalignment='center',
                verticalalignment='bottom',
                color="black",
                wrap=True)
        
    ax.fill_between(xSerie, aSerie, bSerie, color=diffColor, alpha=0.3)
    ax.spines[['right', 'top']].set_visible(False)

    # Rotates and right-aligns the x labels so they don't crowd each other.
    ax.set_xticks(xSerie)
    ax.set_xticklabels(xSerie, rotation=45, ha='right')
    
    ax.grid(True, color="lightgrey", alpha=0.5)

    if plotTitle:
        ax.set_title(plotTitle)
    
    return ax


def draw_lineplot(
        xPositions, values,
        ax=None,
        valuesLabel="Male",
        lineColor='#5bc0de',
    ):

    if not ax:
        _, ax = plt.subplots(
            figsize=(16,16), dpi=300)

    ax.plot(xPositions, values, marker=".", color=lineColor, linewidth=3)

    legend_label_xpos = xPositions[-1] + .2
    ax.text(
        legend_label_xpos,
        values[-1] - (values[-1] * .01),
        valuesLabel,
        size=16,
        color=lineColor,
        wrap=True)
    
    ax.spines[['right', 'top']].set_visible(False)
    return ax