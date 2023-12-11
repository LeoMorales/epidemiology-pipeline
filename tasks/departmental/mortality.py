import pandas


def get_csmr_by_period(upstream, product):
    """
    Mortalidad proporcional para cada provincia considerando todos los registros agrupados por periodos.

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
        str(upstream["get-clean-deceases-data"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["get-cause-specific-deceases-data"])
    )

    # Definir los límites de los bins
    bins = [1990, 1996, 2003, 2010, 2017]

    # Definir las etiquetas para los bins
    labels = ["1990-1996", "1997-2003", "2004-2010", "2011-2017"]

    # Asignar una nueva columna con los periodos
    df_defunciones_totales["period"] = pandas.cut(
        df_defunciones_totales["year"], bins=bins, labels=labels
    )

    df_defunciones_especificas["period"] = pandas.cut(
        df_defunciones_especificas["year"], bins=bins, labels=labels
    )

    df_defunciones_totales_por_departamento = (
        df_defunciones_totales.groupby(["department_id", "period"])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "total_deceases"})
    )

    df_defunciones_especificas_por_departamento = (
        df_defunciones_especificas.groupby(["department_id", "period"])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "specific_deceases"})
    )

    df = pandas.merge(
        df_defunciones_totales_por_departamento,
        df_defunciones_especificas_por_departamento,
        on=["department_id", "period"],
        how="left",
    )

    # drop previous records
    df = df[df["period"] != "1990-1996"].copy()

    # posiblemente hayan registros con fallecimientos totales pero no especificos, completamos con cero.
    df["specific_deceases"] = df["specific_deceases"].fillna(0)

    df["proportional_mortality_1000"] = (
        df["specific_deceases"] / df["total_deceases"]
    ) * 1_000
    df["proportional_mortality_1000"] = df["proportional_mortality_1000"].fillna(0)

    # Chascomus y Tierra del Fuego
    df["department_id"] = df["department_id"].str.replace("06218", "06217", regex=False)
    df["department_id"] = df["department_id"].str.replace("94007", "94008", regex=False)
    df["department_id"] = df["department_id"].str.replace("94014", "94015", regex=False)

    df.to_parquet(str(product))
