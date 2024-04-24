# Calcular tasas / indicadores anualmente.
# Modularizado de esta forma se busca encapsular tasas generales, por sexo, por edad, por sexo y edad, etc
import pandas


def get_national_csmr(product, upstream):
    """
    CSMR national para todos los años disponibles en el dataset.
    """
    # leer registros
    df_defunciones_totales = pandas.read_parquet(
        str(upstream["get-clean-deceases-data"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["get-cause-specific-deceases-data"])
    )

    df_defunciones_totales_por_anio = (
        df_defunciones_totales.groupby(["year"])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "total_deceases"})
    )

    df_defunciones_especificas_por_anio = (
        df_defunciones_especificas.groupby(["year"])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "specific_deceases"})
    )

    df = pandas.merge(
        df_defunciones_totales_por_anio,
        df_defunciones_especificas_por_anio,
        on=["year"],
        how="left",
    )

    # posiblemente hayan registros con fallecimientos totales pero no especificos, completamos con cero.
    df["specific_deceases"] = df["specific_deceases"].fillna(0)

    df["csmr_1000"] = (
        df["specific_deceases"] / df["total_deceases"]
    ) * 1_000
    df["csmr_1000"] = df["csmr_1000"].fillna(0)

    df.to_csv(str(product))