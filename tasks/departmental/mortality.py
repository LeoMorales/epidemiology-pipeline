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
        df_defunciones_totales_por_departamento,
        df_defunciones_especificas_por_departamento,
        on="department_id",
        how="left",
    )

    # posiblemente hayan registros con fallecimientos totales pero no especificos, completamos con cero.
    df = df.fillna(0)

    df["proportional_mortality_1000"] = (
        df["specific_deceases"] / df["total_deceases"]
    ) * 1_000

    # TODO: Comprobar acá si los id de departamento son válidos?
    df.to_parquet(str(product))


def get_proportional_mortality_per_period(upstream, product, groupingOfYears):
    """
    Mortalidad proporcional para cada provincia
    considerando todos los registros agrupados por periodos.

    Args:
        groupingOfYears (dict): Agrupamiento de los registros
            {
                "1991": "1991-1993",
                "1992": "1991-1993",
                "1993": "1991-1993",
                "1994": "1994-1997",
                "1995": "1994-1997",
                ...
            }

    """
    # leer registros
    df_defunciones_totales = pandas.read_parquet(
        str(upstream["get-deceases-with-age-group-label-1991-2017"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["filter-deceases-for-subset-of-causes-1991-2017"])
    )

    # armar la etiqueta
    df_defunciones_totales["year_group"] = df_defunciones_totales["year"].replace(
        groupingOfYears
    )
    df_defunciones_especificas["year_group"] = df_defunciones_especificas[
        "year"
    ].replace(groupingOfYears)

    # agrupar por departamento y grupo de años
    df_defunciones_totales_por_departamento_por_periodo = (
        df_defunciones_totales.groupby(["department_id", "year_group"])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "total_deceases"})
    )

    df_defunciones_especificas_por_departamento_por_periodo = (
        df_defunciones_especificas.groupby(["department_id", "year_group"])[
            "provincia_id"
        ]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "specific_deceases"})
    )

    df_periodos = pandas.merge(
        df_defunciones_totales_por_departamento_por_periodo,
        df_defunciones_especificas_por_departamento_por_periodo,
        on=["department_id", "year_group"],
        how="left",
    )

    df_periodos = df_periodos.fillna(0)

    df_periodos["proportional_mortality_1000"] = (
        df_periodos["specific_deceases"] / df_periodos["total_deceases"]
    ) * 1_000

    df_periodos.to_parquet(str(product))
