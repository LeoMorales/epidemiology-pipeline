import pandas

SEX_CODE_MAPPING = {
    "1": "male",
    "2": "female",
    "9": "undetermined",
    "3": "undetermined",
    "99": "undetermined",
}


def get_sex_correspondence(sex_ids_list):
    """
    Args:
        sex_ids_list (array): arreglo de identificadores de sexo
    """
    # create column with sex description in text

    return pandas.Series(sex_ids_list).apply(
        lambda sex_id: SEX_CODE_MAPPING.get(str(sex_id).strip())
    )


def get_age_in_years(ageSerie, ageCategorySerie):
    result = ageSerie.fillna(999).astype(int)

    row_age_in_months = ageCategorySerie == 2
    result.loc[row_age_in_months] = 0

    row_age_in_days = ageCategorySerie == 3
    result[row_age_in_days] = 0

    row_age_in_horas = ageCategorySerie == 4
    result[row_age_in_horas] = 0

    # 5 MAS DE 100 ANIOS
    # 9 SE IGNORA
    row_age_in_centenarios = ageCategorySerie == 5
    result[row_age_in_centenarios] = 999

    row_age_sin_clasificar = ageCategorySerie == 9
    result[row_age_sin_clasificar] = 999

    return result


def rewrite_codes_according_to_grouping_deprecated(
    values: pandas.core.series.Series, codesGrouping: dict, defaultValue: str = "OTHERS"
) -> pandas.core.series.Series:
    """
    Funcion para obtener una serie con códigos agrupados en base a una serie de codigos
    y un diccionario con (key) grupos de codigos y (value) el listado de codigos al que representa.

    Args:
        values (pandas.Serie): Serie con los codigos presentes en las listas
            del diccionario codesGrouping.

        codesGrouping (dict):
            Example:
            {
                "G3XX": ["G309", "G301", "G300", "G308"],
                "3310": ["3310"],
                "2941": ["2941"],
            }

    Returns:
        response (pandas.Serie)
    """
    if not isinstance(values, pandas.core.series.Series):
        raise TypeError(
            f"values se espera de tipo pandas.core.series.Series, se encontró {type(values)}"
        )
    # valuesAndReplacements es una estructura para reescribir códigos
    # que tiene como valores los arreglos en donde buscar el cógido y
    # como clave la nueva etiqueta para el código
    response = values.astype(str).copy()

    # el valuesAndReplacements es mas cómodo pero lo reescribimos
    # para utilizar replace sobre los valores directamente
    flat_name_mapping = {}
    for key in codesGrouping:
        flat_name_mapping.update(
            {str(val): str(key) for val in codesGrouping[str(key)]}
        )

    # resultado de la reescritura:
    # {'P00': 'P00',
    #  'P01': 'P01',
    #  ...
    #  'Q01': 'Q00-Q99',
    #  'Q02': 'Q00-Q99',
    #  'Q03': 'Q00-Q99',
    #  ...}

    # etiquetar los 'Otros'
    other_code_record_mask = ~response.isin(flat_name_mapping.keys())
    response.loc[other_code_record_mask] = defaultValue

    # rescribir como codigo agrupado
    response = response.replace(flat_name_mapping)

    return response


def rewrite_codes_according_to_grouping(
    values: pandas.core.series.Series, codesGrouping: dict, defaultValue: str = "OTHERS"
) -> pandas.core.series.Series:
    """
    Funcion para obtener una serie con códigos agrupados en base a una serie de codigos
    y un diccionario con (key) grupos de codigos y (value) el listado de codigos al que representa.

    Args:
        values (pandas.Serie): Serie con los codigos presentes en las listas
            del diccionario codesGrouping.

        codesGrouping (dict):
            Example:
            {
                "G3XX": ["G309", "G301", "G300", "G308"],
                "3310": ["3310"],
                "2941": ["2941"],
            }

    Returns:
        response (pandas.Serie)
    """
    if not isinstance(values, pandas.core.series.Series):
        raise TypeError(
            f"values se espera de tipo pandas.core.series.Series, se encontró {type(values)}"
        )
    # valuesAndReplacements es una estructura para reescribir códigos
    # que tiene como valores los arreglos en donde buscar el cógido y
    # como clave la nueva etiqueta para el código
    response = values.astype(str).copy()

    # el valuesAndReplacements es mas cómodo pero lo reescribimos
    # para utilizar replace sobre los valores directamente
    flat_name_mapping = {}
    for key in codesGrouping:
        flat_name_mapping.update(
            {str(val): str(key) for val in codesGrouping[str(key)]}
        )

    # resultado de la reescritura:
    # {'P00': 'P00',
    #  'P01': 'P01',
    #  ...
    #  'Q01': 'Q00-Q99',
    #  'Q02': 'Q00-Q99',
    #  'Q03': 'Q00-Q99',
    #  ...}

    response = response.apply(lambda code: flat_name_mapping.get(code, "OTHERS"))

    return response
