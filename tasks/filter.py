import pandas


def get_cause_specific_deceases_data(upstream, product, causeCodes):
    """Filtra los registros correspondientes a las causas especificas que se están analizando.

    Args:
        upstream (_type_): Input
        product (_type_): Output
        causeCodes (list): Lista de códigos CIE-10
    """
    df = pandas.read_parquet(str(upstream["get-clean-deceases-data"]))

    # df[df['codigo_defuncion'].isin(causeCodes)].head()
    _causeCodes = [str(code) for code in causeCodes]

    df["codigo_defuncion"] = df["codigo_defuncion"].astype(str)
    df_causes_subset = df[df["codigo_defuncion"].isin(_causeCodes)]

    df_causes_subset = df_causes_subset.reset_index(drop=True)
    df_causes_subset.to_parquet(str(product))
