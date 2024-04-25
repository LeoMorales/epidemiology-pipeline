# Tareas de cálculo de mortalidad para resumenes, tomando / comparando
# divisiones en general.
import pandas
from surnames_package import utils


def get_csmr_for_divisions(upstream, product):
    """
    Retorna la tabla por divisiones (es decir genérica y para departamentos,
    provincias, regiones) con las tasas de muertes específicas.

    Args:
        product (ploomber): entrada
        upstream (ploomber): salida
    """
    # get data
    fallecimientos_df = pandas.read_parquet(
        str(upstream["get-clean-deceases-data"]))
    causas_especificas_df = pandas.read_parquet(
        str(upstream["get-cause-specific-deceases-data"])
    )

    # obtener fallecimientos anuales
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
    fallecimientos_por_provincia_por_anio = (
        fallecimientos_por_provincia_por_anio[
            fallecimientos_por_provincia_por_anio["year"].isin(
                causas_especificas_por_provincia_por_anio["year"].unique()
            )
        ])

    # fallecimientos totales + específicos
    df = pandas.merge(
        fallecimientos_por_provincia_por_anio,
        causas_especificas_por_provincia_por_anio,
        on=["provincia_id", "year"],
        how="left",
    ).fillna(0)

    print("Años:", df['year'].unique())
    assert len(set(df["provincia_id"].value_counts())) == 1

    # contar provinciales
    df_provincial = df.groupby("provincia_id").sum().reset_index()

    df_provincial["division"] = df_provincial["provincia_id"].map(
        utils.PROVINCE_NAME_BY_ID.get
    )
    df_provincial["nombre_orden"] = (
        df_provincial["provincia_id"].map(utils.REGION_BY_PROVINCE_CODE.get)
        + df_provincial["division"]
    )
    df_provincial = df_provincial.drop(columns=["provincia_id"])

    # contar regionales
    df["region_nombre"] = df["provincia_id"].map(utils.REGION_BY_PROVINCE_CODE.get)
    df_regional = (
        df.drop(columns="provincia_id").groupby("region_nombre").sum().reset_index()
    )

    df_regional["division"] = df_regional["region_nombre"].copy()
    df_regional = df_regional.rename(columns={"region_nombre": "nombre_orden"})

    # provinciales + regionales + nacional
    df_final = (
        pandas.concat([df_regional, df_provincial])
        .sort_values(by="nombre_orden")
        .drop(columns="nombre_orden")
    )
    
    #
    # contar nacionales
    valores_argentina = {
        "division": "Argentina",
        "deceases": df_provincial["deceases"].sum(),
        "cause_specific_deceases": df_provincial["cause_specific_deceases"].sum(),
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

    df_nacional = pandas.DataFrame([valores_argentina])
    # df_final = df_final.append(valores_argentina, ignore_index=True)
    df_final = pandas.concat([df_final, df_nacional])

    df_final["percentage_of_specific_causes"] = (
        df_final["cause_specific_deceases"] * 100 / df_final["deceases"]
    )
    df_final["rate_per_thousand_people"] = (
        df_final["cause_specific_deceases"] / df_final["deceases"] * 1_000
    )
    
    cols_renaming = {
        "division": "REGIONES / PROVINCIAS",
        "deceases": "Numero fallecidos totales",
        "cause_specific_deceases": "Numero fallecidos EPOF",
        "percentage_of_specific_causes": "% / Total fallecidos",
        "rate_per_thousand_people": "Tasa * 1000 individuos",
    }

    df_final = df_final.rename(columns=cols_renaming)[cols_renaming.values()]
    df_final = df_final.reset_index(drop=True)

    df_final.to_csv(str(product["csv"]))
    df_final.to_latex(str(product["latex"]))
