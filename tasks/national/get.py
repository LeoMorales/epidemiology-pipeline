import pandas


def getAnnualCauseSpecificMortalityRate(
    upstream, product, rate_multiplier: float = 1_000
):
    """Calcula las tasas de mortalidad anual por causas específicas (csmr) para la Argentina.

    Args:
        upstream (_type_): Input
        product (_type_): Output
    """
    all_deceases_df = pandas.read_parquet(
        str(upstream["aggr-deceases-annually-by-sex-arg"])
    )
    causes_specific_deceases_df = pandas.read_parquet(
        str(upstream["aggr-cause-specific-deceases-by-sex-by-year-arg"])
    )

    if (not ("year" in all_deceases_df.columns)) & (
        not ("year" in causes_specific_deceases_df.columns)
    ):
        raise ValueError(
            "No se encuentra la columna 'year' en alguno de los datasets de entrada"
        )

    # 3. combine
    df = pandas.merge(
        all_deceases_df, causes_specific_deceases_df, on="year", how="left"
    )

    csmr_df = df[["year"]].copy()

    csmr_df["csmr"] = (df["cause_specific_deceases"] / df["deceases"]) * rate_multiplier

    csmr_df["csmr_male"] = (
        df["cause_specific_deceases_male"] / df["deceases"]
    ) * rate_multiplier

    csmr_df["csmr_female"] = (
        df["cause_specific_deceases_female"] / df["deceases"]
    ) * rate_multiplier

    csmr_df["csmr_male_local"] = (
        df["cause_specific_deceases_male"] / df["deceases_male"]
    ) * rate_multiplier

    csmr_df["csmr_female_local"] = (
        df["cause_specific_deceases_female"] / df["deceases_female"]
    ) * rate_multiplier

    csmr_df = csmr_df.fillna(0)

    csmr_df.to_parquet(str(product))


def getPeriodicalCauseSpecificMortalityRate(
    upstream, product, rate_multiplier: float = 1_000
):
    """Calcula las tasas de mortalidad por causas específicas (csmr) para la Argentina, por periodos.

    Args:
        upstream (_type_): Input
        product (_type_): Output
    """
    all_deceases_df = pandas.read_parquet(
        str(upstream["aggr-deceases-by-sex-by-period-arg"])
    )
    causes_specific_deceases_df = pandas.read_parquet(
        str(upstream["aggr-cause-specific-deceases-by-sex-by-period"])
    )

    # TODO: Revisar si se realiza el filtrado de registros pertenecientes a provincias válidas y correspondientes a sexos fem masc.
    # en la vieja tarea mortality se usaban estos metodos:
    # total_deceases_df = __filter_sex_province(total_deceases_df)
    # cause_specific_deceases = __filter_sex_province(cause_specific_deceases)

    if (not ("year_group" in all_deceases_df.columns)) & (
        not ("year_group" in causes_specific_deceases_df.columns)
    ):
        raise ValueError(
            "No se encuentra la columna 'year_group' en alguno de los datasets de entrada"
        )

    # 3. combine
    df = pandas.merge(
        all_deceases_df, causes_specific_deceases_df, on="year_group", how="left"
    )

    csmr_df = df[["year_group"]].copy()

    csmr_df["csmr"] = (df["cause_specific_deceases"] / df["deceases"]) * rate_multiplier

    csmr_df["csmr_male"] = (
        df["cause_specific_deceases_male"] / df["deceases"]
    ) * rate_multiplier

    csmr_df["csmr_female"] = (
        df["cause_specific_deceases_female"] / df["deceases"]
    ) * rate_multiplier

    csmr_df["csmr_male_local"] = (
        df["cause_specific_deceases_male"] / df["deceases_male"]
    ) * rate_multiplier

    csmr_df["csmr_female_local"] = (
        df["cause_specific_deceases_female"] / df["deceases_female"]
    ) * rate_multiplier

    csmr_df = csmr_df.fillna(0)

    csmr_df.to_parquet(str(product))
