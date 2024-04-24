import pandas


CSMR_COLUM = "csmr_1000"


def get_csmr_by_year(upstream, product):
    """
    CSMR departamental para todos los años disponibles en el dataset.
    """
    # leer registros
    df_defunciones_totales = pandas.read_parquet(
        str(upstream["get-clean-deceases-data"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["get-cause-specific-deceases-data"])
    )

    df_defunciones_totales_por_departamento = (
        df_defunciones_totales.groupby(["department_id", "year"])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "total_deceases"})
    )

    df_defunciones_especificas_por_departamento = (
        df_defunciones_especificas.groupby(["department_id", "year"])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "specific_deceases"})
    )

    df = pandas.merge(
        df_defunciones_totales_por_departamento,
        df_defunciones_especificas_por_departamento,
        on=["department_id", "year"],
        how="left",
    )

    # posiblemente hayan registros con fallecimientos totales pero no especificos, completamos con cero.
    df["specific_deceases"] = df["specific_deceases"].fillna(0)

    df[CSMR_COLUM] = (
        df["specific_deceases"] / df["total_deceases"]
    ) * 1_000
    df[CSMR_COLUM] = df[CSMR_COLUM].fillna(0)

    # Chascomus y Tierra del Fuego
    df["department_id"] = df["department_id"].str.replace("06218", "06217", regex=False)
    df["department_id"] = df["department_id"].str.replace("94007", "94008", regex=False)
    df["department_id"] = df["department_id"].str.replace("94014", "94015", regex=False)

    df.to_parquet(str(product))

