import pandas


def get_cause_specific_rate_by_age_group_and_sex(product, upstream, age_group_mapping):
    df_fallecimientos = pandas.read_parquet(str(upstream["get-clean-deceases-data"]))
    df_causas_especificas = pandas.read_parquet(
        str(upstream["get-cause-specific-deceases-data"])
    )

    # df_fallecimientos['year'] = df_fallecimientos['year'].astype(int)
    # df_causas_especificas['year'] = df_causas_especificas['year'].astype(int)
    # se hace en el clean...
    df_fallecimientos = df_fallecimientos[
        (df_fallecimientos["year"] >= df_causas_especificas["year"].min())
        & (df_fallecimientos["year"] <= df_causas_especificas["year"].max())
    ].reset_index(drop=True)

    age_group_order_mapping = {
        key: order
        for key, order in zip(age_group_mapping.keys(), range(len(age_group_mapping)))
    }

    # filtrar las columnas de interes (como buena práctica)
    interest_cols = ["codigo_defuncion", "sex", "age_group"]
    df_fallecimientos_aggr = df_fallecimientos[interest_cols].copy()

    df_fallecimientos_aggr = (
        df_fallecimientos_aggr.groupby(by=["sex", "age_group"])
        .count()
        .reset_index()
        .rename(columns={"codigo_defuncion": "deceases"})
    )

    df_fallecimientos_aggr = df_fallecimientos_aggr.pivot(
        index="age_group", columns="sex", values="deceases"
    ).reset_index()

    df_fallecimientos_aggr = (
        df_fallecimientos_aggr.assign(
            row_order=df_fallecimientos_aggr["age_group"].map(
                age_group_order_mapping.get
            )
        )
        .sort_values(by="row_order")
        .drop(columns="row_order")
        .reset_index(drop=True)
    )

    df_fallecimientos_aggr.columns.name = None

    df_fallecimientos_aggr = df_fallecimientos_aggr.rename(
        columns={
            "female": "deceases_female",
            "male": "deceases_male",
            "undetermined": "deceases_undetermined",
        }
    )

    # filtrar las columnas de interes (como buena práctica)
    df_causas_especificas_aggr = df_causas_especificas[interest_cols].copy()

    df_causas_especificas_aggr = (
        df_causas_especificas_aggr.groupby(by=["sex", "age_group"])
        .count()
        .reset_index()
        .rename(columns={"codigo_defuncion": "deceases"})
    )

    df_causas_especificas_aggr = df_causas_especificas_aggr.pivot(
        index="age_group", columns="sex", values="deceases"
    ).reset_index()

    df_causas_especificas_aggr = (
        df_causas_especificas_aggr.assign(
            row_order=df_causas_especificas_aggr["age_group"].map(
                age_group_order_mapping.get
            )
        )
        .sort_values(by="row_order")
        .drop(columns="row_order")
        .reset_index(drop=True)
    )

    df_causas_especificas_aggr.columns.name = None

    df_causas_especificas_aggr = df_causas_especificas_aggr.rename(
        columns={
            "female": "cause_specific_deceases_female",
            "male": "cause_specific_deceases_male",
            "undetermined": "cause_specific_deceases_undetermined",
        }
    )

    df = pandas.merge(
        df_fallecimientos_aggr.drop(columns=["deceases_undetermined"]),
        df_causas_especificas_aggr.drop(
            columns=["cause_specific_deceases_undetermined"]
        ),
    )

    df["percentage_female"] = (
        df["cause_specific_deceases_female"] * 100 / df["deceases_female"]
    )
    df["rate_female"] = (
        df["cause_specific_deceases_female"] / df["deceases_female"] * 1_000
    )

    df["percentage_male"] = (
        df["cause_specific_deceases_male"] * 100 / df["deceases_male"]
    )
    df["rate_male"] = df["cause_specific_deceases_male"] / df["deceases_male"] * 1_000

    cols_renaming = {
        "age_group": "EDADES",
        "deceases_female": "Fallecimientos mujeres",
        "deceases_male": "Fallecimientos varones",
        "cause_specific_deceases_female": "Fallecimientos EPOF mujeres",
        "cause_specific_deceases_male": "Fallecimientos EPOF varones",
        "percentage_female": "Porcentaje sobre total mujeres",
        "percentage_male": "Porcentaje sobre total varones",
        "rate_female": "Tasa por cada 1000 mujeres",
        "rate_male": "Tasa por cada 1000 varones",
    }

    df_final = df.rename(columns=cols_renaming)[cols_renaming.values()]

    df_final.to_parquet(str(product))
