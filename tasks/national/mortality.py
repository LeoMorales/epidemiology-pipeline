"""
1. Tasa bruta (cruda) de mortalidad
Relaciona todas las muertes acaecidas en la población de un área geográfica
dada durante un período de tiempo determinado con la población total de esa
área en el mismo período. Habitualmente el resultado de esta relación se
multiplica por 1000.

2. Tasa de mortalidad (según causa)
Relaciona las muertes acaecidas en la población por una causa específica
(o grupos de causas) con la población total del área.

3. Mortalidad proporcional (según causa)
Expresa el porcentaje de defunciones por una causa (o grupo de causas)
en relación al total de muertes. Este indicador no mide riesgo de muerte
sino la importancia relativa de cada causa (o grupo de causas) respecto
al total de muertes.
"""
import pandas
from epidemiology_package import utils
from epidemiology_package import cleanning
from surnames_package import utils as surnames_utils


def get_proportional_mortality(upstream, product):
    """
    Mortalidad proporcional para cada Argentina
    considerando todos los registros del período.
    """
    df_defunciones_totales = pandas.read_parquet(
        str(upstream["get-deceases-with-age-group-label-1991-2017"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["filter-deceases-for-subset-of-causes-1991-2017"])
    )

    total_deceases_arg = len(df_defunciones_totales)
    specific_deceases_arg = len(df_defunciones_especificas)

    df = pandas.DataFrame(
        columns=[
            "division",
            "total_deceases",
            "specific_deceases",
            "proportional_mortality",
        ],
        data=[
            [
                "Argentina",
                total_deceases_arg,
                specific_deceases_arg,
                ((specific_deceases_arg / total_deceases_arg) * 1_000),
            ]
        ],
    )

    df.to_parquet(str(product))


def get_proportional_mortality_per_period(upstream, product, groupingOfYears):
    """
    Mortalidad proporcional para la Argentina considerando todos los registros agrupados por periodos.

    Args:
        upstream (_type_): Destino
        product (_type_): Entradas
        groupingOfYears (dict): Agrupamiento de los registros. Por ejemplo
            {
                "1991": "1991-1993",
                "1992": "1991-1993",
                "1993": "1991-1993",
                "1994": "1994-1997",
                "1995": "1994-1997",
                ...
            }
    """
    df_defunciones_totales = pandas.read_parquet(
        str(upstream["get-deceases-with-age-group-label-1991-2017"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["filter-deceases-for-subset-of-causes-1991-2017"])
    )

    df_defunciones_totales["year_group"] = df_defunciones_totales["year"].replace(
        groupingOfYears
    )
    df_defunciones_especificas["year_group"] = df_defunciones_especificas[
        "year"
    ].replace(groupingOfYears)

    df_arg_anual = pandas.merge(
        df_defunciones_totales.groupby(["year"])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "total_deceases"}),
        df_defunciones_especificas.groupby(["year"])["provincia_id"]
        .count()
        .reset_index()
        .rename(columns={"provincia_id": "specific_deceases"}),
        on=["year"],
        how="left",
    )

    df_arg_anual["proportional_mortality"] = (
        df_arg_anual["specific_deceases"] / df_arg_anual["total_deceases"]
    ) * 1_000

    df_arg_anual.to_parquet(str(product))


def __filter_sex_province(df):
    df_filtered = df.copy()

    SEX_CODE_MAPPING = {
        "1": "varon",
        "2": "mujer",
        "9": "indeterminado",
        "3": "indeterminado",
        "99": "indeterminado",
    }

    df_filtered["sexo"] = df_filtered["sexo"].astype(str).apply(SEX_CODE_MAPPING.get)

    df_filtered = df_filtered[df_filtered["sexo"].isin(["mujer", "varon"])]

    # filter province
    province_ids = list(surnames_utils.PROVINCE_NAME_BY_ID.keys())

    df_filtered = df_filtered[df_filtered["provincia_id"].isin(province_ids)]

    return df_filtered


def get_annual_proportional_mortality_by_group(upstream, product):
    """
    Mortalidad proporcional para Argentina, por año y por sexo.
    """
    total_deceases_df = pandas.read_parquet(
        str(upstream["get-deceases-with-age-group-label-1991-2017"])
    )
    specific_deceases_df = pandas.read_parquet(
        str(upstream["filter-deceases-for-subset-of-causes-1991-2017"])
    )

    total_deceases_df = __filter_sex_province(total_deceases_df)
    specific_deceases_df = __filter_sex_province(specific_deceases_df)

    codes_for_each_category = utils.get_codes_categorization(
        specific_deceases_df["codigo_defuncion"]
    )

    # tenemos que etiquetar codigo->categoria
    specific_deceases_df["group"] = cleanning.rewrite_codes_according_to_grouping(
        specific_deceases_df["codigo_defuncion"], codes_for_each_category
    )

    specific_decease_by_group_by_year = (
        specific_deceases_df.groupby(["year", "group"])["department_id"]
        .count()
        .reset_index()
        .rename(columns={"department_id": "deceases_group"})
    )

    total_deceases_by_year_df = (
        total_deceases_df.groupby(["year"])["department_id"]
        .count()
        .reset_index()
        .rename(columns={"department_id": "deceases_year"})
    )

    mortality_by_group_by_year = pandas.merge(
        specific_decease_by_group_by_year, total_deceases_by_year_df, on="year"
    )

    mortality_by_group_by_year["proportional_mortality_1000"] = (
        mortality_by_group_by_year["deceases_group"]
        / mortality_by_group_by_year["deceases_year"]
        * 1_000
    )

    # save:
    mortality_by_group_by_year.to_parquet(str(product))


def get_annual_proportional_mortality_by_sex(upstream, product):
    """
    Mortalidad proporcional para Argentina, por año y por sexo.
    """
    total_deceases_df = pandas.read_parquet(
        str(upstream["get-deceases-with-age-group-label-1991-2017"])
    )
    specific_deceases_df = pandas.read_parquet(
        str(upstream["filter-deceases-for-subset-of-causes-1991-2017"])
    )

    total_deceases_df = __filter_sex_province(total_deceases_df)
    specific_deceases_df = __filter_sex_province(specific_deceases_df)

    annual_deceases_df = (
        total_deceases_df.groupby(["year", "sexo"])["department_id"]
        .count()
        .reset_index()
        .rename(columns=dict(department_id="total_deceases"))
    )

    annual_specific_deceases_df = (
        specific_deceases_df.groupby(["year", "sexo"])["department_id"]
        .count()
        .reset_index()
        .rename(columns=dict(department_id="specific_deceases"))
    )

    proportional_mortality_by_sex_df = pandas.merge(
        annual_specific_deceases_df, annual_deceases_df, on=["year", "sexo"]
    )

    proportional_mortality_by_sex_df["mortality_1000"] = (
        proportional_mortality_by_sex_df["specific_deceases"]
        / proportional_mortality_by_sex_df["total_deceases"]
        * 1_000
    )

    proportional_mortality_by_sex_df.to_parquet(str(product))


def get_annual_proportional_mortality_by_group_year(upstream, product):
    """
    Mortalidad proporcional para Argentina, por año y por grupo etario.
    """
    total_deceases_df = pandas.read_parquet(
        str(upstream["get-deceases-with-age-group-label-1991-2017"])
    )
    specific_deceases_df = pandas.read_parquet(
        str(upstream["filter-deceases-for-subset-of-causes-1991-2017"])
    )

    total_deceases_df = __filter_sex_province(total_deceases_df)
    specific_deceases_df = __filter_sex_province(specific_deceases_df)

    annual_deceases_by_age_group_df = (
        total_deceases_df.groupby(["year", "age_group"])["department_id"]
        .count()
        .reset_index()
        .rename(columns=dict(department_id="total_deceases"))
    )

    annual_specific_deceases_by_age_group_df = (
        specific_deceases_df.groupby(["year", "age_group"])["department_id"]
        .count()
        .reset_index()
        .rename(columns=dict(department_id="specific_deceases"))
    )

    proportional_mortality_by_age_group_df = pandas.merge(
        annual_specific_deceases_by_age_group_df,
        annual_deceases_by_age_group_df,
        on=["year", "age_group"],
    )

    proportional_mortality_by_age_group_df["mortality_1000"] = (
        proportional_mortality_by_age_group_df["specific_deceases"]
        / proportional_mortality_by_age_group_df["total_deceases"]
        * 1_000
    )

    # order records:
    AGE_GROUP_ORDER_DICT = {
        "0 - 5": 1,
        "6 - 15": 2,
        "16 - 35": 3,
        "36 - 45": 4,
        "46 - 55": 5,
        "56 - 65": 6,
        "66 - 75": 7,
        "76 - 85": 8,
        ">85": 9,
        "NOT-ASSIGNED": 10,
    }
    proportional_mortality_by_age_group_df = (
        proportional_mortality_by_age_group_df.assign(
            age_group_order=proportional_mortality_by_age_group_df["age_group"].apply(
                AGE_GROUP_ORDER_DICT.get
            )
        )
        .sort_values(by=["year", "age_group_order"])
        .drop(columns="age_group_order")
        .reset_index(drop=True)
    )

    # save:
    proportional_mortality_by_age_group_df.to_parquet(str(product))
