import pandas


def get_sex_correspondence(sex_ids_list):
    """
    Args:
        sex_ids_list (array): arreglo de identificadores de sexo
    """
    # create column with sex description in text
    sex_mapping = {
        '1': 'varon',
        '2': 'mujer',
        '9': 'indeterminado',
        '3': 'indeterminado',
        '99': 'indeterminado'
    }

    return \
        pandas.Series(sex_ids_list)\
            .apply(
                lambda sex_id: sex_mapping.get(str(sex_id).strip())
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