import pandas

def get_causes_incidence(upstream, product):
    df_all_deceases = pandas.read_parquet(
        str(upstream['get-deceases-1991-2017-by-year-by-sex-arg']))
    df_causes_subset_deceases = pandas.read_parquet(
        str(upstream['get-deceases-selected-causes-1991-2017-by-year-by-sex-arg']))

    # 3. combine
    df = pandas.merge(
        df_all_deceases,
        df_causes_subset_deceases,
        on='year',
        how='left'
    )
    
    incidence_df = df[['year']].copy()

    incidence_df['causes_incidence'] = \
        (df['deceases_subset_causes'] / df['deceases']) * 100_000
    
    incidence_df['causes_incidence_male'] = \
        (df['deceases_subset_causes_varon'] / df['deceases']) * 100_000
    
    incidence_df['causes_incidence_female'] = \
        (df['deceases_subset_causes_mujer'] / df['deceases']) * 100_000

    incidence_df['causes_incidence_male_local'] = \
        (df['deceases_subset_causes_varon'] / df['deceases_varon']) * 100_000
    
    incidence_df['causes_incidence_female_local'] = \
        (df['deceases_subset_causes_mujer'] / df['deceases_mujer']) * 100_000

    incidence_df.to_parquet(str(product))