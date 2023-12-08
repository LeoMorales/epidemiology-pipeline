import pandas
from epidemiology_package import cleanning


def get_all_deceases_1991_2017(upstream, product, ageGroupMapping):
    """
    Retorna el dataset principal de trabajo
    Interpreta (en años) la columna edad segun la columna unidad_edad.
    Utiliza el ageGroupMapping para asignar una etiqueta de grupo etario.
    """
    df = pandas.read_parquet(str(upstream["get-cleaned-data-1991-2017"]))

    df["age_in_years"] = cleanning.get_age_in_years(df["edad"], df["unidad_edad"])

    df["age_group"] = "NOT-ASSIGNED"

    for label, label_range in ageGroupMapping.items():
        range_a, range_b = label_range
        age_range = list(range(range_a, range_b + 1))

        range_mask = df["age_in_years"].isin(age_range)
        df.loc[range_mask, "age_group"] = label

    df.to_parquet(str(product))
