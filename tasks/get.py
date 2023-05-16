import pandas
from epidemiology_package import cleanning


def get_deceases_by_year_by_sex_arg(upstream, product, causeCodes):
    
    df = pandas.read_parquet(str(upstream['get-deceases-with-age-group-label-1991-2017']))
    
    # 1. get dataset: 
    #   year  deceases
    #0  1991          255609
    #1  1992          262287
    #2  1993          267286
    df_deceases_year = df['year'].value_counts()\
        .reset_index()\
            .rename(columns={'index': 'year', 'year': 'deceases'})\
                .sort_values(by='year').reset_index(drop=True)

    # create column with sex description in text
    sex_mapping = {
        '1': 'varon',
        '2': 'mujer',
        '9': 'indeterminado',
        '3': 'indeterminado',
        '99': 'indeterminado'
    }

    df['sex_correspondence'] = \
        df['sexo'].apply(
            lambda sex_label: sex_mapping.get(str(sex_label).strip()))

    # df['sex_correspondence'].value_counts()

    # create dataset:
    #    year  deceases_indeterminado  deceases_mujer   deceases_varon  
    # 0  1991                          1613                112619                 141377  
    # 1  1992                          2136                115692                 144459  
    # 2  1993                          2916                117129                 147241

    df_deceases_year_by_sex = pandas.DataFrame({})

    for sex_label, df_sex in df.groupby("sex_correspondence"):
        deceases_col_label = f"deceases_{sex_label}"
        
        df_slice = \
            df_sex['year'].value_counts()\
                .reset_index()\
                    .rename(columns={
                        'index': 'year',
                        'year': deceases_col_label
                    })\
                    .sort_values(by='year')\
                    .reset_index(drop=True)
        
        if len(df_deceases_year_by_sex) == 0:
            df_deceases_year_by_sex = df_slice
        else:
            df_deceases_year_by_sex = pandas.merge(
                df_deceases_year_by_sex,
                df_slice,
                on="year"
            )

    # append total deceases column
    df_deceases_all = pandas.merge(
        df_deceases_year,
        df_deceases_year_by_sex,
        on="year"
    )
    
    # 2. causes subset    

    # df[df['codigo_defuncion'].isin(causeCodes)].head()
    causeCodes = [str(code) for code in causeCodes]
    df_causes_subset_deceases_year_by_sex = pandas.DataFrame({})
    df['codigo_defuncion'] = df['codigo_defuncion'].astype(str)
    df_causes_subset = df[df['codigo_defuncion'].isin(causeCodes)]
    causes_subset_deceases_df = \
        df_causes_subset['year'].value_counts()\
            .reset_index()\
                .rename(columns={'index': 'year', 'year': 'deceases_subset_causes'})\
                    .sort_values(by='year').reset_index(drop=True)

    for sex_label, df_sex in df_causes_subset.groupby("sex_correspondence"):
        deceases_col_label = f"deceases_subset_causes_{sex_label}"
        
        df_slice = \
            df_sex['year'].value_counts()\
                .reset_index()\
                    .rename(columns={'index': 'year', 'year': deceases_col_label})\
                        .sort_values(by='year').reset_index(drop=True)
       
        if len(df_causes_subset_deceases_year_by_sex) == 0:
            df_causes_subset_deceases_year_by_sex = df_slice
        else:
            df_causes_subset_deceases_year_by_sex = pandas.merge(
                df_causes_subset_deceases_year_by_sex,
                df_slice,
                on="year",
                how='right'
            )
    
    df_causes_subset_all = pandas.merge(causes_subset_deceases_df, df_causes_subset_deceases_year_by_sex, on=['year'], how='left').fillna(0)

    df_causes_subset_all['deceases_subset_causes_indeterminado'] = df_causes_subset_all['deceases_subset_causes_indeterminado'].astype('int64')
    df_causes_subset_all['deceases_subset_causes_mujer'] = df_causes_subset_all['deceases_subset_causes_mujer'].astype('int64')
    df_causes_subset_all['deceases_subset_causes_varon'] = df_causes_subset_all['deceases_subset_causes_varon'].astype('int64')
    
    # 3. combine
    output_df = pandas.merge(
        df_deceases_all,
        df_causes_subset_all,
        on='year',
        how='left'
    )
    
    output_df = output_df.fillna(0)
    
    cols = [
        'deceases_subset_causes',
        'deceases_subset_causes_indeterminado',
        'deceases_subset_causes_mujer',
        'deceases_subset_causes_varon',
    ]

    for col in cols:
        output_df[col] = output_df[col].astype('int64')

    output_df.to_parquet(str(product))


def aggr_deceases_by_year_by_sex_arg_all(upstream, product):
    """
    Create dataset:
    #    year  deceases_indeterminado  deceases_mujer   deceases_varon  
    # 0  1991                          1613                112619                 141377  
    # 1  1992                          2136                115692                 144459  
    # 2  1993                          2916                117129                 147241
    """
    
    df = pandas.read_parquet(str(upstream['get-deceases-with-age-group-label-1991-2017']))

    df['sex_correspondence'] = cleanning.get_sex_correspondence(df['sexo'].values)
    # df['sex_correspondence'].value_counts()
    
    df_deceases_year_by_sex = pandas.DataFrame({})

    for sex_label, df_sex in df.groupby("sex_correspondence"):
        deceases_col_label = f"deceases_{sex_label}"
        
        df_slice = \
            df_sex['year'].value_counts()\
                .reset_index()\
                    .rename(columns={
                        'index': 'year',
                        'year': deceases_col_label
                    })\
                    .sort_values(by='year')\
                    .reset_index(drop=True)
        
        if len(df_deceases_year_by_sex) == 0:
            df_deceases_year_by_sex = df_slice
        else:
            df_deceases_year_by_sex = pandas.merge(
                df_deceases_year_by_sex,
                df_slice,
                on="year"
            )

    # get all deceases dataset: 
    #   year  deceases
    #0  1991          255609
    #1  1992          262287
    #2  1993          267286
    df_deceases_year = df['year'].value_counts()\
        .reset_index()\
            .rename(columns={'index': 'year', 'year': 'deceases'})\
                .sort_values(by='year').reset_index(drop=True)

    # append total deceases column
    df_deceases_all = pandas.merge(
        df_deceases_year,
        df_deceases_year_by_sex,
        on="year"
    )
    
    # convert data types
    cols = [
        'deceases',
        'deceases_indeterminado',
        'deceases_mujer',
        'deceases_varon',
    ]
    for col in cols:
        df_deceases_all[col] = df_deceases_all[col].astype('int64')

    df_deceases_all.to_parquet(str(product))


def aggr_deceases_selected_causes_by_year_by_sex_arg(upstream, product):
    df_causes_subset = pandas.read_parquet(str(upstream['filter-deceases-for-subset-of-causes-1991-2017']))
    
    df_causes_subset['sex_correspondence'] = \
        cleanning.get_sex_correspondence(df_causes_subset['sexo'])

    df_causes_subset_deceases_year_by_sex = None
    
    for sex_label, df_sex_slice in df_causes_subset.groupby("sex_correspondence"):
        
        df_slice = \
            df_sex_slice['year'].value_counts()\
                .reset_index()\
                .rename(
                    columns={
                        'index': 'year',
                        'year': f"deceases_subset_causes_{sex_label}"
                    })\
                .sort_values(by='year').reset_index(drop=True)

        not_first_dataframe_collected = df_causes_subset_deceases_year_by_sex is None
        
        if not_first_dataframe_collected:
            df_causes_subset_deceases_year_by_sex = df_slice
        else:
            # combine
            df_causes_subset_deceases_year_by_sex = pandas.merge(
                df_causes_subset_deceases_year_by_sex,
                df_slice,
                on="year",
                how='right'
            )

    # get all deaths for causes subset:
    total_subset_causes_deceases_df = \
        df_causes_subset['year'].value_counts()\
            .reset_index()\
            .rename(columns={
                'index': 'year',
                'year': 'deceases_subset_causes'
                })\
            .sort_values(by='year').reset_index(drop=True)
    
    # combine
    df_causes_subset_all = pandas.merge(
        total_subset_causes_deceases_df,
        df_causes_subset_deceases_year_by_sex,
        on=['year'],
        how='left').fillna(0)

    df_causes_subset_all = df_causes_subset_all.fillna(0)
    
    # convert data types
    cols = [
        'deceases_subset_causes',
        'deceases_subset_causes_indeterminado',
        'deceases_subset_causes_mujer',
        'deceases_subset_causes_varon',
    ]
    
    cols = [ col for col in cols if (col in df_causes_subset_all.columns) ] 
    for col in cols:
        df_causes_subset_all[col] = df_causes_subset_all[col].astype('int64')

    df_causes_subset_all.to_parquet(str(product))
    

def get_deceases_with_age_group_label_1991_2017(upstream, product, ageGroupMapping):
    """
    Interpreta (en años) la columna edad segun la columna unidad_edad.
    Utiliza el ageGroupMapping para asignar una etiqueta de grupo etario.
    """
    df = pandas.read_parquet(str(upstream['get-cleaned-data-1991-2017']))

    df['age_in_years'] = \
        cleanning.get_age_in_years(df['edad'], df['unidad_edad'])
    
    df['age_group'] = "NOT-ASSIGNED"

    for label, label_range in ageGroupMapping.items():
        
        range_a, range_b = label_range
        age_range = list(range(range_a, range_b+1))
        
        range_mask = df['age_in_years'].isin(age_range)
        df.loc[range_mask, 'age_group'] = label
    
    df.to_parquet(str(product))