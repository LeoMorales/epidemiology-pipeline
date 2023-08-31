import pandas
from epidemiology_package import cleanning


def aggr_deceases_annually_by_sex(upstream, product):
    """Genera el dataset con el recuento de falleciminetos totales y los fallecimentos deagrupados segun seexo.

    <class 'pandas.core.frame.DataFrame'>
    #   Column                  Non-Null Count  Dtype
    ---  ------                  --------------  -----
    0   year                    27 non-null     object
    1   deceases                27 non-null     int64
    2   deceases_undetermined   27 non-null     int64
    3   deceases_female         27 non-null     int64
    4   deceases_male           27 non-null     int64
    dtypes: int64(4), object(1)

    Args:
        upstream (_type_): input
        product (_type_): output
    """
    df = pandas.read_parquet(str(upstream["get-deceases-1991-2017"]))

    df["sex_correspondence"] = cleanning.get_sex_correspondence(df["sexo"].values)
    # df['sex_correspondence'].value_counts()

    df_deceases_year_by_sex = pandas.DataFrame({})

    for sex_label, df_sex in df.groupby("sex_correspondence"):
        deceases_col_label = f"deceases_{sex_label}"

        df_slice = (
            df_sex["year"]
            .value_counts()
            .reset_index()
            .rename(columns={"index": "year", "year": deceases_col_label})
            .sort_values(by="year")
            .reset_index(drop=True)
        )

        if len(df_deceases_year_by_sex) == 0:
            df_deceases_year_by_sex = df_slice
        else:
            df_deceases_year_by_sex = pandas.merge(
                df_deceases_year_by_sex, df_slice, on="year"
            )

    # get all deceases dataset:
    #   year  deceases
    # 0  1991          255609
    # 1  1992          262287
    # 2  1993          267286
    df_deceases_year = (
        df["year"]
        .value_counts()
        .reset_index()
        .rename(columns={"index": "year", "year": "deceases"})
        .sort_values(by="year")
        .reset_index(drop=True)
    )

    # append total deceases column
    df_deceases_all = pandas.merge(df_deceases_year, df_deceases_year_by_sex, on="year")

    # convert data types
    cols = [
        "deceases",
        "deceases_undetermined",
        "deceases_female",
        "deceases_male",
    ]
    for col in cols:
        df_deceases_all[col] = df_deceases_all[col].astype("int64")

    df_deceases_all.to_parquet(str(product))


def aggr_deceases_by_sex_by_period(upstream, product, groupingOfYears):
    """Genera el dataset con el recuento de falleciminetos totales y los fallecimentos deagrupados segun sexo.

    <class 'pandas.core.frame.DataFrame'>
    #   Column                  Non-Null Count  Dtype
    ---  ------                  --------------  -----
    0   year_group              27 non-null     object
    1   deceases                27 non-null     int64
    2   deceases_undetermined   27 non-null     int64
    3   deceases_female         27 non-null     int64
    4   deceases_male           27 non-null     int64
    dtypes: int64(4), object(1)

    Args:
        upstream (_type_): input
        product (_type_): output
    """
    df = pandas.read_parquet(str(upstream["get-deceases-1991-2017"]))

    df["sex_correspondence"] = cleanning.get_sex_correspondence(df["sexo"].values)
    # df['sex_correspondence'].value_counts()

    # obtener la variable que agrupe registros por año
    df["year_group"] = df["year"].replace(groupingOfYears)

    df_deceases_year_by_sex = pandas.DataFrame({})

    for sex_label, df_sex in df.groupby("sex_correspondence"):
        deceases_col_label = f"deceases_{sex_label}"

        df_slice = (
            df_sex["year_group"]
            .value_counts()
            .reset_index()
            .rename(columns={"index": "year_group", "year_group": deceases_col_label})
            .sort_values(by="year_group")
            .reset_index(drop=True)
        )

        if len(df_deceases_year_by_sex) == 0:
            df_deceases_year_by_sex = df_slice
        else:
            df_deceases_year_by_sex = pandas.merge(
                df_deceases_year_by_sex, df_slice, on="year_group"
            )

    # get all deceases dataset:
    df_deceases_year = (
        df["year_group"]
        .value_counts()
        .reset_index()
        .rename(columns={"index": "year_group", "year_group": "deceases"})
        .sort_values(by="year_group")
        .reset_index(drop=True)
    )

    # append total deceases column
    df_deceases_all = pandas.merge(
        df_deceases_year, df_deceases_year_by_sex, on="year_group"
    )

    # convert data types
    cols = [
        "deceases",
        "deceases_undetermined",
        "deceases_female",
        "deceases_male",
    ]
    for col in cols:
        df_deceases_all[col] = df_deceases_all[col].astype("int64")

    df_deceases_all.to_parquet(str(product))


def aggr_cause_specific_deceases_by_sex_by_year(upstream, product):
    """Contabiliza los fallecimientos por causas especificas para la Argentina completa por cada año.

    Args:
        upstream (_type_): Input
        product (_type_): Output
    """

    # leer los fallecimientos por causas específicas
    cause_specifict_deceases_df = pandas.read_parquet(
        str(upstream["filter-cause-specific-deceases-1991-2017"])
    )

    # limpiar la variable numerica que representa el sexo
    cause_specifict_deceases_df[
        "sex_correspondence"
    ] = cleanning.get_sex_correspondence(cause_specifict_deceases_df["sexo"])

    cause_specific_deceases_year_by_sex_df = None

    for sex_label, records_by_sex_df in cause_specifict_deceases_df.groupby(
        "sex_correspondence"
    ):
        records_by_sex_by_year_df = (
            records_by_sex_df["year"]
            .value_counts()
            .reset_index()
            .rename(
                columns={
                    "index": "year",
                    "year": f"cause_specific_deceases_{sex_label}",
                }
            )
            .sort_values(by="year")
            .reset_index(drop=True)
        )

        not_first_dataframe_collected = cause_specific_deceases_year_by_sex_df is None

        if not_first_dataframe_collected:
            cause_specific_deceases_year_by_sex_df = records_by_sex_by_year_df
        else:
            # combine
            cause_specific_deceases_year_by_sex_df = pandas.merge(
                cause_specific_deceases_year_by_sex_df,
                records_by_sex_by_year_df,
                on="year",
                how="right",
            )

    # get all deaths for causes subset:
    all_cause_specific_deceases_by_year_df = (
        cause_specifict_deceases_df["year"]
        .value_counts()
        .reset_index()
        .rename(columns={"index": "year", "year": "cause_specific_deceases"})
        .sort_values(by="year")
        .reset_index(drop=True)
    )

    # combine
    cause_specifict_deceases_df = pandas.merge(
        all_cause_specific_deceases_by_year_df,
        cause_specific_deceases_year_by_sex_df,
        on=["year"],
        how="left",
    ).fillna(0)

    cause_specifict_deceases_df = cause_specifict_deceases_df.fillna(0)

    # convert data types
    cols = [
        "cause_specific_deceases",
        "cause_specific_deceases_undetermined",
        "cause_specific_deceases_female",
        "cause_specific_deceases_male",
    ]

    cols = [
        col for col in cols if (col in cause_specifict_deceases_df.columns)
    ]  # asegurarse de que las columnas se encuentren en el dataframe

    for col in cols:
        cause_specifict_deceases_df[col] = cause_specifict_deceases_df[col].astype(
            "int64"
        )

    cause_specifict_deceases_df.to_parquet(str(product))


def aggr_cause_specific_deceases_by_sex_by_period(upstream, product, groupingOfYears):
    """Contabiliza los fallecimientos por causas especificas para la Argentina completa por cada periodos.

    Args:
        upstream (_type_): Input
        product (_type_): Output
    """

    # leer los fallecimientos por causas específicas
    cause_specifict_deceases_df = pandas.read_parquet(
        str(upstream["filter-cause-specific-deceases-1991-2017"])
    )

    # limpiar la variable numerica que representa el sexo
    cause_specifict_deceases_df[
        "sex_correspondence"
    ] = cleanning.get_sex_correspondence(cause_specifict_deceases_df["sexo"])

    # obtener la variable que agrupe registros por año
    cause_specifict_deceases_df["year_group"] = cause_specifict_deceases_df[
        "year"
    ].replace(groupingOfYears)

    cause_specific_deceases_by_sex_by_period_df = None

    for sex_label, records_by_sex_df in cause_specifict_deceases_df.groupby(
        "sex_correspondence"
    ):
        records_by_sex_by_period_df = (
            records_by_sex_df["year_group"]
            .value_counts()
            .reset_index()
            .rename(
                columns={
                    "index": "year_group",
                    "year_group": f"cause_specific_deceases_{sex_label}",
                }
            )
            .sort_values(by="year_group")
            .reset_index(drop=True)
        )

        not_first_dataframe_collected = (
            cause_specific_deceases_by_sex_by_period_df is None
        )

        if not_first_dataframe_collected:
            cause_specific_deceases_by_sex_by_period_df = records_by_sex_by_period_df
        else:
            # combine
            cause_specific_deceases_by_sex_by_period_df = pandas.merge(
                cause_specific_deceases_by_sex_by_period_df,
                records_by_sex_by_period_df,
                on="year_group",
                how="right",
            )

    # get all deaths for causes subset:
    all_cause_specific_deceases_by_period_df = (
        cause_specifict_deceases_df["year_group"]
        .value_counts()
        .reset_index()
        .rename(
            columns={"index": "year_group", "year_group": "cause_specific_deceases"}
        )
        .sort_values(by="year_group")
        .reset_index(drop=True)
    )

    # combine
    cause_specifict_deceases_df = pandas.merge(
        all_cause_specific_deceases_by_period_df,
        cause_specific_deceases_by_sex_by_period_df,
        on=["year_group"],
        how="left",
    ).fillna(0)

    cause_specifict_deceases_df = cause_specifict_deceases_df.fillna(0)

    # convert data types
    cols = [
        "cause_specific_deceases",
        "cause_specific_deceases_undetermined",
        "cause_specific_deceases_female",
        "cause_specific_deceases_male",
    ]

    cols = [
        col for col in cols if (col in cause_specifict_deceases_df.columns)
    ]  # asegurarse de que las columnas se encuentren en el dataframe

    for col in cols:
        cause_specifict_deceases_df[col] = cause_specifict_deceases_df[col].astype(
            "int64"
        )

    cause_specifict_deceases_df.to_parquet(str(product))


def aggrDeceasesAnnuallyBySexByAgeGroup(upstream, product, age_category_order):
    """
    Retorna las cantidades anuales de fallecimientos en toda la Argentina, agrupadas por sex y grupo etario.

    Args:
        upstream (_type_): Input
        product (_type_): Output
        age_category_order (list): Lista con el order de las etiquetas de agrupamiento etario

    Returns:
        pandas.DataFrame: Datos agrupados.
           year     sex age_group  cause_specific_deceases
        0  1997  female     0 - 4                       XX
        1  1997  female     5 - 9                       XX
        2  1997  female   10 - 14                       XX
    """
    df = pandas.read_parquet(str(upstream["get-deceases-1991-2017"]))

    SEX_CODE_MAPPING = {
        "1": "male",
        "2": "female",
        "9": "undetermined",
        "3": "undetermined",
        "99": "undetermined",
    }

    df["sex"] = df["sexo"].astype(str).map(SEX_CODE_MAPPING)

    workingColumns = ["codigo_defuncion", "sex", "year", "age_group"]
    # la columna codigo_defuncion se utiliza solo para contar

    df = df[workingColumns]
    df = df[df["sex"].isin(["male", "female"])]

    # asignar un 0 a '0-4', un 1 a '5-9', etc...
    age_category_order_dict = {
        category: order
        for category, order in zip(age_category_order, range(len(age_category_order)))
    }

    counts_df = (
        df.groupby(["year", "sex", "age_group"])
        .count()
        .rename(columns=dict(codigo_defuncion="deceases"))
        .reset_index()
    )

    output_df = (
        counts_df.assign(
            age_group_order=counts_df["age_group"].map(age_category_order_dict)
        )
        .sort_values(by=["year", "sex", "age_group_order"])
        .drop(columns="age_group_order")
        .reset_index(drop=True)
    )

    output_df.to_parquet(str(product))


def aggrCauseSpecificDeceasesAnnuallyBySexByAgeGroup(
    upstream, product, age_category_order
):
    """Retorna las cantidades anuales de fallecimientos por causas específicas en toda la Argentina, agrupadas por sex y grupoetario.

    Args:
        upstream (_type_): Input
        product (_type_): Output
        age_category_order (list): Lista con el order de las etiquetas de agrupamiento etario

    Returns:
        pandas.DataFrame: Datos agrupados.
               year     sex age_group  cause_specific_deceases
            0  1997  female     0 - 4                       XX
            1  1997  female     5 - 9                       XX
            2  1997  female   10 - 14                       XX
    """
    df = pandas.read_parquet(str(upstream["filter-cause-specific-deceases-1991-2017"]))

    SEX_CODE_MAPPING = {
        "1": "male",
        "2": "female",
        "9": "undetermined",
        "3": "undetermined",
        "99": "undetermined",
    }

    df["sex"] = df["sexo"].astype(str).map(SEX_CODE_MAPPING)

    workingColumns = ["codigo_defuncion", "sex", "year", "age_group"]

    df = df[workingColumns]
    df = df[df["sex"].isin(["male", "female"])]

    age_category_order_dict = {
        category: order
        for category, order in zip(age_category_order, range(len(age_category_order)))
    }

    counts_df = (
        df.groupby(["year", "sex", "age_group"])
        .count()
        .rename(columns=dict(codigo_defuncion="cause_specific_deceases"))
        .reset_index()
    )

    output_df = (
        counts_df.assign(
            age_group_order=counts_df["age_group"].map(age_category_order_dict)
        )
        .sort_values(by=["year", "sex", "age_group_order"])
        .drop(columns="age_group_order")
        .reset_index(drop=True)
    )

    output_df.to_parquet(str(product))
