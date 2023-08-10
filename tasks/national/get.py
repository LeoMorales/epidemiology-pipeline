import pandas
from epidemiology_package import cleanning


def aggr_deceases_by_year_by_sex_arg(upstream, product):
    """Genera el dataset con el recuento de falleciminetos totales y los fallecimentos deagrupados segun seexo.

    <class 'pandas.core.frame.DataFrame'>
    #   Column                  Non-Null Count  Dtype
    ---  ------                  --------------  -----
    0   year                    27 non-null     object
    1   deceases                27 non-null     int64
    2   deceases_indeterminado  27 non-null     int64
    3   deceases_mujer          27 non-null     int64
    4   deceases_varon          27 non-null     int64
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
        "deceases_indeterminado",
        "deceases_mujer",
        "deceases_varon",
    ]
    for col in cols:
        df_deceases_all[col] = df_deceases_all[col].astype("int64")

    df_deceases_all.to_parquet(str(product))


def aggr_cause_specific_deceases_by_year_by_sex_arg(upstream, product):
    df_causes_subset = pandas.read_parquet(
        str(upstream["filter-cause-specific-deceases-1991-2017"])
    )

    df_causes_subset["sex_correspondence"] = cleanning.get_sex_correspondence(
        df_causes_subset["sexo"]
    )

    df_causes_subset_deceases_year_by_sex = None

    for sex_label, df_sex_slice in df_causes_subset.groupby("sex_correspondence"):
        df_slice = (
            df_sex_slice["year"]
            .value_counts()
            .reset_index()
            .rename(
                columns={"index": "year", "year": f"deceases_subset_causes_{sex_label}"}
            )
            .sort_values(by="year")
            .reset_index(drop=True)
        )

        not_first_dataframe_collected = df_causes_subset_deceases_year_by_sex is None

        if not_first_dataframe_collected:
            df_causes_subset_deceases_year_by_sex = df_slice
        else:
            # combine
            df_causes_subset_deceases_year_by_sex = pandas.merge(
                df_causes_subset_deceases_year_by_sex, df_slice, on="year", how="right"
            )

    # get all deaths for causes subset:
    total_subset_causes_deceases_df = (
        df_causes_subset["year"]
        .value_counts()
        .reset_index()
        .rename(columns={"index": "year", "year": "deceases_subset_causes"})
        .sort_values(by="year")
        .reset_index(drop=True)
    )

    # combine
    df_causes_subset_all = pandas.merge(
        total_subset_causes_deceases_df,
        df_causes_subset_deceases_year_by_sex,
        on=["year"],
        how="left",
    ).fillna(0)

    df_causes_subset_all = df_causes_subset_all.fillna(0)

    # convert data types
    cols = [
        "deceases_subset_causes",
        "deceases_subset_causes_indeterminado",
        "deceases_subset_causes_mujer",
        "deceases_subset_causes_varon",
    ]

    cols = [col for col in cols if (col in df_causes_subset_all.columns)]
    for col in cols:
        df_causes_subset_all[col] = df_causes_subset_all[col].astype("int64")

    df_causes_subset_all.to_parquet(str(product))
