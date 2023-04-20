# cause-specific mortality rate
import pandas
from surnames_package import utils
import seaborn
import matplotlib.pyplot as plt

def calculate_csmr_per_province(upstream, product):
    """
    The measure that divides the number of deaths of a specific cause by all total deaths
    in a population per year is called the cause-specific mortality rate (CSMR).

    The cause-specific mortality rate is a measure of the number of deaths from a specific
    cause that occur in a population over a specific period of time, typically one year,
    per 1,000 people in the population. It is calculated by dividing the number of deaths
    from a specific cause by the total number of deaths in the population and then multiplying
    by 1,000. The cause-specific mortality rate is often used to compare the impact of different
    causes of death on a population or to track changes in mortality rates over time.

    For example, the CSMR for heart disease would be calculated by dividing the number of deaths
    from heart disease in a population over a specific period of time by the total number of deaths
    in the population over that same period of time, and then multiplying by 1,000. This would give
    the number of deaths from heart disease per 1,000 people in the population per year.
    """
    
    # read data:    
    df = pandas.read_parquet(upstream['filter-deceases-for-subset-of-causes-1991-2017'])
    df_all_deceases = pandas.read_parquet(upstream['get-deceases-with-age-group-label-1991-2017'])

    columns_selection = ['provincia_id', 'codigo_defuncion', 'year']

    # clean specific codes data
    df_subset_deceases_by_province = df[columns_selection].copy()
    df_subset_deceases_by_province = df_subset_deceases_by_province\
        .groupby(by=['year', 'provincia_id'])\
            .count()\
        .reset_index()\
        .rename(columns={'codigo_defuncion': 'subset_deceases'})

    # clean all deceases data
    df_deceases_by_province = df_all_deceases[columns_selection].copy()
    df_deceases_by_province = df_deceases_by_province\
        .groupby(by=['year', 'provincia_id'])\
            .count()\
        .reset_index()\
        .rename(columns={'codigo_defuncion': 'deceases'})

    data_rates = pandas.merge(
        df_deceases_by_province,
        df_subset_deceases_by_province,
        on=['year', 'provincia_id']
    )

    # calculate rate
    data_rates['csmr'] = (data_rates['subset_deceases'] / data_rates['deceases']) * 1_000

    # create rate dataframe
    df_csmr = data_rates[['year', 'provincia_id', 'csmr']]
    
    # save
    df_csmr.to_parquet(str(product))
    
    
def draw_csmr_heatmap_provincial(upstream, product):
    
    df_csmr = pandas.read_parquet(upstream['csmr-per-province'])
    df_csmr_pivot = df_csmr.pivot(index='provincia_id', columns='year', values='csmr')

    df_csmr_pivot.index.name = None
    df_csmr_pivot.columns.name = None

    df_csmr_pivot = df_csmr_pivot.reset_index().rename(columns={'index': 'provincia_id'})

    df_csmr_pivot = utils.append_province_description(
        df_csmr_pivot,
        provinceCodeColumn='provincia_id')

    df_csmr_pivot = df_csmr_pivot\
        .sort_values(by=['region_nombre', 'provincia_nombre'])\
        .drop(columns=['provincia_id', 'region_nombre', 'region_id'])

    df_csmr_pivot = df_csmr_pivot[~df_csmr_pivot['provincia_nombre'].isna()]

    f, ax = plt.subplots(figsize=(11, 9))

    # Generate a custom diverging colormap
    cmap = seaborn.diverging_palette(230, 20, as_cmap=True)

    # create the heatmap with Seaborn
    seaborn.heatmap(
        df_csmr_pivot.set_index('provincia_nombre'),
        cmap=cmap,
        square=True,
        linewidths=.5,
        cbar_kws={"shrink": .5})

    # set labels and title
    plt.xlabel("Year")
    plt.ylabel("Province")
    plt.title("Cause-specific mortality rate (CSMR) per province")

    plt.savefig(str(product), dpi=300)
    plt.close()