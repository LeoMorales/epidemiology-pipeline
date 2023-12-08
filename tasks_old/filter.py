import pandas
from epidemiology_package import utils


def get_cause_specific_deceases_1991_2017(upstream, product, causeCodes):
    df = pandas.read_parquet(str(upstream["get-deceases-1991-2017"]))

    # df[df['codigo_defuncion'].isin(causeCodes)].head()
    _causeCodes = [str(code) for code in causeCodes]

    df["codigo_defuncion"] = df["codigo_defuncion"].astype(str)
    df_causes_subset = df[df["codigo_defuncion"].isin(_causeCodes)]

    df_causes_subset = df_causes_subset.reset_index(drop=True)
    df_causes_subset.to_parquet(str(product))
