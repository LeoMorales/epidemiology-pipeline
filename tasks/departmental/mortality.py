import pandas


def get_proportional_mortality(upstream, product):
    """
    Mortalidad proporcional para cada departamento
    considerando todos los registros de 1991 a 2017.
    """
    df_defunciones_totales = pandas.read_parquet(
        str(upstream["get-deceases-with-age-group-label-1991-2017"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["filter-deceases-for-subset-of-causes-1991-2017"])
    )

    df_defunciones_totales_por_departamento = (
        df_defunciones_totales.groupby("department_id")["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "total_deceases"})
    )

    df_defunciones_especificas_por_departamento = (
        df_defunciones_especificas.groupby("department_id")["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "specific_deceases"})
    )

    df = pandas.merge(
        df_defunciones_especificas_por_departamento,
        df_defunciones_totales_por_departamento,
        on="department_id",
        how="left",
    )

    df["tasa_mortalidad"] = (df["specific_deceases"] / df["total_deceases"]) * 1_000

    df.to_parquet(str(product))
