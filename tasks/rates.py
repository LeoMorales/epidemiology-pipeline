import pandas


def get_annual_rates_grouping_by_age(upstream, product, causesCodes: list):
    """
    Genera el dataset de tasas anuales por grupo etario.
    La tasa esta expresada en fallecidos debido a causas con los
    códigos recibidos por cada 1000 fallecimientos.

    Returns:
            year age_group  rate_1000
        0   1991     0 - 5  123.45
        1   1991   16 - 35  123.45
        2   1991   36 - 45  123.45
        ...
        266 2017   66 - 75  123.45
        267 2017   76 - 85  123.45
        268 2017       >85  123.45
    """
    df = pandas.read_parquet(
        str(upstream["get-deceases-with-age-group-label-1991-2017"])
    )

    data = []
    for _year, df_year in df.groupby("year"):
        # 1991, 1992, ...
        all_deceases_n = len(df_year)

        for _age_group, df_year_age_group in df_year.groupby("age_group"):
            # '0 - 5', '6 - 15', ..., '>85'
            df_year_age_group_ = df_year_age_group[
                df_year_age_group["codigo_defuncion"].isin(causesCodes)
            ]
            subset_deceases_n = len(df_year_age_group_)
            if subset_deceases_n > 0:
                proportion = (subset_deceases_n / all_deceases_n) * 1_000
                data.append((_year, _age_group, proportion))

    rates_df = pandas.DataFrame(data, columns=["year", "age_group", "rate_1000"])

    rates_df = rates_df[rates_df["age_group"] != "NOT-ASSIGNED"]

    rates_df.to_parquet(str(product))
