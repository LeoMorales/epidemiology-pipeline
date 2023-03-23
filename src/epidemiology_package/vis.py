import numpy
from matplotlib import pyplot as plt
import matplotlib as mpl
import seaborn


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


def draw_stacked_plot(
        sorted_plot_data,
        only_save=False,
        output_file='delete.png',
        title='Stacked barplot',
        colors=['#7fc97f', '#beaed4', '#fdc086', '#ffff99', '#386cb0', '#f0027f'],
        legend_n_rows=2,
        legend_font_size=12
    ):
    seaborn.set()
    font_color = '#525252'
    csfont = {'fontname':'Calibri'} # title font
    hfont = {'fontname':'Calibri'} # main font
    

    ax = sorted_plot_data\
        .plot.barh(
            align='center',
            stacked=True,
            figsize=(10, .45*len(sorted_plot_data)),
            color=colors)

    title = plt.title(
        title,
        pad=60,
        fontsize=18,
        color=font_color,
        **csfont)

    title.set_position([.5, 1.05])

    # Adjust the subplot so that the title would fit
    plt.subplots_adjust(top=0.8, left=0.26)

    for label in (ax.get_xticklabels() + ax.get_yticklabels()):
        label.set_fontsize(15)

    plt.xticks(color=font_color, **hfont)
    plt.yticks(color=font_color, **hfont)

    data_n_cols = len(sorted_plot_data.columns)
    n_col_legend = (
        int(data_n_cols / legend_n_rows) 
        if data_n_cols % legend_n_rows == 0
        else (data_n_cols // legend_n_rows) +1 )
    print(n_col_legend)
    legend = plt.legend(
        loc='center',
        frameon=False,
        bbox_to_anchor=(0., 1., 1., .102), 
        mode='expand', 
        ncol=n_col_legend, 
        borderaxespad=-.46,
        prop={'size': legend_font_size, 'family':'Calibri'})

    for text in legend.get_texts():
        plt.setp(text, color=font_color) # legend font color

    plt.tight_layout()

    if only_save:
        plt.savefig(output_file)
        plt.close()
        return None
    else:
        plt.show()
        return ax