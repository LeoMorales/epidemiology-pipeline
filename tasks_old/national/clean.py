import pandas


def getFemalePopulationAnnuallyByAgeGroup(product):
    """Retorna la población anual de mujeres, por grupo etario.

    Args:
        product (_type_): Output

    Returns:
        <class 'pandas.core.frame.DataFrame'>
        Int64Index: 21 entries, 1 to 21
        Data columns (total 9 columns):
        #   Column     Non-Null Count  Dtype
        ---  ------     --------------  -----
        0   age_group  21 non-null     object
        1   2010       21 non-null     int64
        2   2011       21 non-null     int64
        3   2012       21 non-null     int64
        4   2013       21 non-null     int64
        5   2014       21 non-null     int64
        6   2015       21 non-null     int64
        7   2016       21 non-null     int64
        8   2017       21 non-null     int64
        dtypes: int64(8), object(1)
    """
    proyecciones_mujeres_df = pandas.read_csv(
        "/home/lmorales/resources/estimaciones-poblacion/tabula-proyeccionesyestimaciones_nac_2010_2040-mujeres.csv"
    )

    # el dataset de proyecciones tiene los valores para años hasta 2040, por lo que hay que quedarse con los años de interes
    proyecciones_mujeres_df = proyecciones_mujeres_df[:22]

    # renombrar columna
    proyecciones_mujeres_df = proyecciones_mujeres_df.rename(
        columns={"edad": "age_group"}
    )

    # eliminar renglón de totales
    proyecciones_mujeres_df = proyecciones_mujeres_df[
        proyecciones_mujeres_df["age_group"] != "Total"
    ]
    # crear la columna grupo etario
    proyecciones_mujeres_df["age_group"] = (
        proyecciones_mujeres_df["age_group"]
        .str.replace("- ", "-")
        .str.replace("-", " - ")
    )

    # convertir los valores de proyección en enteros
    for year_col in list(range(2010, 2018)):
        year_col = str(year_col)
        proyecciones_mujeres_df[year_col] = (
            proyecciones_mujeres_df[year_col]
            .str.replace(".", "", regex=False)
            .astype(int)
        )

    # guardar
    proyecciones_mujeres_df.to_parquet(str(product))


def getMalePopulationAnnuallyByAgeGroup(product):
    """Retorna los valores de las proyecciones anuales de población, por grupo etario, para varones, entre 2010 y 2017.

    Args:
        product (_type_): Output
    """
    proyecciones_varones_df = pandas.read_csv(
        "/home/lmorales/resources/estimaciones-poblacion/tabula-proyeccionesyestimaciones_nac_2010_2040-varones.csv"
    )

    proyecciones_varones_df = proyecciones_varones_df[:22]
    proyecciones_varones_df = proyecciones_varones_df.rename(
        columns={"edad": "age_group"}
    )
    proyecciones_varones_df = proyecciones_varones_df[
        proyecciones_varones_df["age_group"] != "Total"
    ]
    proyecciones_varones_df["age_group"] = (
        proyecciones_varones_df["age_group"]
        .str.replace("- ", "-")
        .str.replace("-", " - ")
    )

    for year_col in list(range(2010, 2018)):
        year_col = str(year_col)
        proyecciones_varones_df[year_col] = (
            proyecciones_varones_df[year_col]
            .str.replace(".", "", regex=False)
            .astype(int)
        )

    proyecciones_varones_df.to_parquet(str(product))
