import pandas


CSMR_COLUM = "csmr_1000"


def get_csmr_by_periods(upstream, product, bins_values, bins_labels, period_denomination:str = 'period'):
    """Calcula las CSMR departamental por periodo.
    
    Al utilizar esta función, el período se especifica enviando los puntos de corte (bins_values) y las etiquetas de cada corte correspondiente a período (bins_labels).
    La lista bins_values debe tener como primer elemento el año mas chico y, como elementos siguientes, los topes de cada período. Así el n=B.
    La lista bins_labels debe tener las etiquetas de cada período, entonces el n=B-1.
    

    Args:
        upstream (ploomber): Entrada
        product (ploomber): Salida
        bins_values (_type_): Por ejemplo: [1990, 1996, 2003, 2010, 2017]
        bins_labels (_type_): Por ejemplo: ['1990-1996', '1997-2003', '2004-2010', '2011-2017']
        period_denomination (str): Por ejemplo: "Septenio"
    """

    df_defunciones_totales = pandas.read_parquet(
        str(upstream['get-clean-deceases-data'])
    )

    # Asignar una nueva columna con los períodos
    df_defunciones_totales[period_denomination] = pandas.cut(
        df_defunciones_totales['year'],
        bins=bins_values,
        labels=bins_labels)

    df_defunciones_especificas = pandas.read_parquet(
        str(upstream['get-cause-specific-deceases-data'])
    )

    # Asignar una nueva columna con los períodos (2)
    df_defunciones_especificas[period_denomination] = pandas.cut(
        df_defunciones_especificas['year'],
        bins=bins_values,
        labels=bins_labels)

    # agrupar
    df_defunciones_totales_por_departamento = (
        df_defunciones_totales.groupby(["department_id", period_denomination])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "total_deceases"})
    )

    # agrupar
    df_defunciones_especificas_por_departamento = (
        df_defunciones_especificas.groupby(["department_id", period_denomination])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "specific_deceases"})
    )

    # combinar
    df = pandas.merge(
        df_defunciones_totales_por_departamento,
        df_defunciones_especificas_por_departamento,
        on=["department_id", period_denomination],
        how="left",
    )

    df['specific_deceases'] = df['specific_deceases'].fillna(0).astype(int)
    
    df["csmr_1000"] = (
        df["specific_deceases"] / df["total_deceases"]
    ) * 1_000

    df["department_id"] = (
        df["department_id"]
            .str.replace("06218", "06217", regex=False))
    
    # corregir códigos de departamento de Tierra del Fuego
    df["department_id"] = (
        df["department_id"]
            .str.replace("94007", "94008", regex=False))

    df["department_id"] = (
        df["department_id"]
            .str.replace("94014", "94015", regex=False))

    df.to_parquet(str(product))
