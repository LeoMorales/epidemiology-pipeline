import pandas
from epidemiology_package import utils


def __assign_name(department_code):
    return utils.DEPARTMENT_CODES_MAPPING.get(department_code, None)


def filter_departamental_records(upstream, product):
    df = pandas.read_parquet(str(upstream["get-cleaned-data-1991-2017"]))
    df["department_name"] = df["department_id"].apply(__assign_name)

    not_null_department_name = ~(df["department_name"].isna())
    not_unknown_department_name = ~(df["department_name"] == "DESCONOCIDO")
    departamental_df = df[not_null_department_name & not_unknown_department_name]

    departamental_df.to_parquet(str(product))


def get_deceases_from_specific_causes_for_1991_2017(upstream, product, causeCodes):
    df = pandas.read_parquet(
        str(upstream["get-deceases-with-age-group-label-1991-2017"])
    )

    # df[df['codigo_defuncion'].isin(causeCodes)].head()
    _causeCodes = [str(code) for code in causeCodes]

    df["codigo_defuncion"] = df["codigo_defuncion"].astype(str)
    df_causes_subset = df[df["codigo_defuncion"].isin(_causeCodes)]

    df_causes_subset = df_causes_subset.reset_index(drop=True)
    df_causes_subset.to_parquet(str(product))
