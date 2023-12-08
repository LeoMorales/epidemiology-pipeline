import pandas
from epidemiology_package.constants import SEX_CODE_MAPPING
from surnames_package import utils


def get_annual_cause_specific_rate(upstream, product, age_category_order):
    """

    Args:
        upstream (_type_): Input
        product (_type_): Output
        age_category_order (list): Lista con el orden de las etiquetas de agrupamiento etario

    Returns:
        pandas.DataFrame: Datos agrupados.
    """
    df = pandas.read_parquet(str(upstream["get-cause-specific-deceases-data"]))

    df["region"] = df["provincia_id"].map(utils.REGION_BY_PROVINCE_CODE_DICT.get)
    workingColumns = ["codigo_defuncion", "sex", "year", "age_group", "region"]

    df = df[workingColumns].copy()
    df = df[df["sex"].isin(["male", "female"])]

    counts_df = (
        df.groupby(["region", "year", "sex", "age_group"])
        .count()
        .rename(columns=dict(codigo_defuncion="cause_specific_deceases"))
        .reset_index()
    )

    age_category_order_dict = {
        category: order
        for category, order in zip(age_category_order, range(len(age_category_order)))
    }

    output_df = (
        counts_df.assign(
            age_group_order=counts_df["age_group"].map(age_category_order_dict)
        )
        .sort_values(by=["region", "year", "sex", "age_group_order"])
        .drop(columns="age_group_order")
        .reset_index(drop=True)
    )

    output_df.to_parquet(str(product))
