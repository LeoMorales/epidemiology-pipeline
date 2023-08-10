"""
1. Tasa bruta (cruda) de mortalidad
Relaciona todas las muertes acaecidas en la población de un área geográfica
dada durante un período de tiempo determinado con la población total de esa
área en el mismo período. Habitualmente el resultado de esta relación se
multiplica por 1000.

2. Tasa de mortalidad (según causa)
Relaciona las muertes acaecidas en la población por una causa específica
(o grupos de causas) con la población total del área.

3. Mortalidad proporcional (según causa)
Expresa el porcentaje de defunciones por una causa (o grupo de causas)
en relación al total de muertes. Este indicador no mide riesgo de muerte
sino la importancia relativa de cada causa (o grupo de causas) respecto
al total de muertes.
"""
import pandas


def get_proportional_mortality(upstream, product):
    """
    Mortalidad proporcional para cada provincia
    considerando todos los registros del período.
    """
    df_defunciones_totales = pandas.read_parquet(
        str(upstream["get-deceases-1991-2017"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["filter-cause-specific-deceases-1991-2017"])
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

    df["proportional_mortality_1000"] = (
        df["specific_deceases"] / df["total_deceases"]
    ) * 1_000

    df.to_parquet(str(product))


def get_proportional_mortality_per_period(upstream, product, groupingOfYears):
    """Mortalidad proporcional para cada provincia considerando todos los registros agrupados por periodos.

    Args:
        upstream (_type_): Destino
        product (_type_): Entradas
        groupingOfYears (dict): Agrupamiento de los registros. Por ejemplo
            {
                "1991": "1991-1993",
                "1992": "1991-1993",
                "1993": "1991-1993",
                "1994": "1994-1997",
                "1995": "1994-1997",
                ...
            }
    """
    # lectura de los registros
    df_defunciones_totales = pandas.read_parquet(
        str(upstream["get-deceases-1991-2017"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["filter-cause-specific-deceases-1991-2017"])
    )

    # obtención de la columna grupo de años
    df_defunciones_totales["year_group"] = df_defunciones_totales["year"].replace(
        groupingOfYears
    )
    df_defunciones_especificas["year_group"] = df_defunciones_especificas[
        "year"
    ].replace(groupingOfYears)

    # agrupamiento por provincia y periodo de años (general)
    df_defunciones_totales_por_provincia_por_periodo = (
        df_defunciones_totales.groupby(["provincia_id", "year_group"])["department_id"]
        .count()
        .reset_index()
        .rename(columns={"department_id": "total_deceases"})
    )

    # agrupamiento por provincia y periodo de años (especificas)
    df_defunciones_especificas_por_provincia_por_periodo = (
        df_defunciones_especificas.groupby(["provincia_id", "year_group"])[
            "department_id"
        ]
        .count()
        .reset_index()
        .rename(columns={"department_id": "specific_deceases"})
    )

    # combinación de datos agrupados
    df_periodos = pandas.merge(
        df_defunciones_especificas_por_provincia_por_periodo,
        df_defunciones_totales_por_provincia_por_periodo,
        on=["provincia_id", "year_group"],
        how="left",
    )

    # calculo de mortalidad especifica
    df_periodos["proportional_mortality_1000"] = (
        df_periodos["specific_deceases"] / df_periodos["total_deceases"]
    ) * 1_000

    # guardado
    df_periodos.to_parquet(str(product))
