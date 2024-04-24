import pandas


CSMR_COLUM = "csmr_1000"


def get_csmr_by_year(upstream, product):
    """ """
    # leer registros

    df_defunciones_totales = pandas.read_parquet(
        str(upstream["get-clean-deceases-data"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["get-cause-specific-deceases-data"])
    )

    df_defunciones_totales_por_provincia = (
        df_defunciones_totales.groupby("provincia_id")["department_id"]
        .count()
        .reset_index()
        .rename(columns={"department_id": "total_deceases"})
    )

    df_defunciones_especificas_por_provincia = (
        df_defunciones_especificas.groupby("provincia_id")["department_id"]
        .count()
        .reset_index()
        .rename(columns={"department_id": "specific_deceases"})
    )

    df = pandas.merge(
        df_defunciones_especificas_por_provincia,
        df_defunciones_totales_por_provincia,
        on="provincia_id",
        how="left",
    )
    # posiblemente hayan registros con fallecimientos totales pero no especificos, completamos con cero.
    df["specific_deceases"] = df["specific_deceases"].fillna(0)

    df[CSMR_COLUM] = (
        df["specific_deceases"] / df["total_deceases"]
    ) * 1_000

    df.to_parquet(str(product))
