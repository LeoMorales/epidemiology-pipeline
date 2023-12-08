import pandas
from surnames_package import utils


def get_cause_specific_rate_for_divisions(upstream, product):
    fallecimientos_df = pandas.read_parquet(str(upstream["get-clean-deceases-data"]))
    causas_especificas_df = pandas.read_parquet(
        str(upstream["get-cause-specific-deceases-data"])
    )

    fallecimientos_por_provincia_por_anio = (
        fallecimientos_df.groupby(["provincia_id", "year"])["department_id"]
        .count()
        .reset_index()
        .rename(columns={"department_id": "deceases"})
    )

    causas_especificas_por_provincia_por_anio = (
        causas_especificas_df.groupby(["provincia_id", "year"])["department_id"]
        .count()
        .reset_index()
        .rename(columns={"department_id": "cause_specific_deceases"})
    )

    # filtrar años comunes
    fallecimientos_por_provincia_por_anio = fallecimientos_por_provincia_por_anio[
        fallecimientos_por_provincia_por_anio["year"].isin(
            causas_especificas_por_provincia_por_anio["year"].unique()
        )
    ]

    df = pandas.merge(
        fallecimientos_por_provincia_por_anio,
        causas_especificas_por_provincia_por_anio,
        on=["provincia_id", "year"],
        how="left",
    ).fillna(0)

    assert len(set(df["provincia_id"].value_counts())) == 1

    df_provincial = df.groupby("provincia_id").sum().reset_index()

    df["region_nombre"] = df["provincia_id"].map(utils.REGION_BY_PROVINCE_CODE.get)

    df_regional = (
        df.drop(columns="provincia_id").groupby("region_nombre").sum().reset_index()
    )

    df_provincial["percentage_of_specific_causes"] = (
        df_provincial["cause_specific_deceases"] * 100 / df_provincial["deceases"]
    )
    df_provincial["rate_per_thousand_people"] = (
        df_provincial["cause_specific_deceases"] / df_provincial["deceases"] * 1_000
    )

    df_regional["percentage_of_specific_causes"] = (
        df_regional["cause_specific_deceases"] * 100 / df_regional["deceases"]
    )
    df_regional["rate_per_thousand_people"] = (
        df_regional["cause_specific_deceases"] / df_regional["deceases"] * 1_000
    )

    df_regional["division"] = df_regional["region_nombre"].copy()
    df_regional = df_regional.rename(columns={"region_nombre": "nombre_orden"})

    df_provincial["division"] = df_provincial["provincia_id"].map(
        utils.PROVINCE_NAME_BY_ID.get
    )
    df_provincial["nombre_orden"] = (
        df_provincial["provincia_id"].map(utils.REGION_BY_PROVINCE_CODE.get)
        + df_provincial["division"]
    )
    df_provincial = df_provincial.drop(columns=["provincia_id"])

    df_final = (
        pandas.concat([df_regional, df_provincial])
        .sort_values(by="nombre_orden")
        .drop(columns="nombre_orden")
    )

    valores_argentina = {
        "division": "Argentina",
        "deceases": df_final["deceases"].sum(),
        "cause_specific_deceases": df_final["cause_specific_deceases"].sum(),
    }
    valores_argentina["percentage_of_specific_causes"] = (
        valores_argentina["cause_specific_deceases"]
        * 100
        / valores_argentina["deceases"]
    )
    valores_argentina["rate_per_thousand_people"] = (
        valores_argentina["cause_specific_deceases"]
        / valores_argentina["deceases"]
        * 1000
    )

    df_final = df_final.append(valores_argentina, ignore_index=True)

    cols_renaming = {
        "division": "REGIONES / PROVINCIAS",
        "deceases": "Numero fallecidos totales",
        "cause_specific_deceases": "Numero fallecidos EPOF",
        "percentage_of_specific_causes": "% / Total fallecidos",
        "rate_per_thousand_people": "Tasa * 1000 individuos",
    }

    df_final = df_final.rename(columns=cols_renaming)[cols_renaming.values()]
    df_final = df_final.reset_index(drop=True)

    df_final.to_parquet(str(product))

    # +
    # agrupar por provincia y contar
    # obtener tasa de mortalidad especifica
    # obtener poblacion
