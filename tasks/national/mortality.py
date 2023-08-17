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
    """Mortalidad proporcional para la Argentina considerando todos los registros del período.

    Args:
        upstream (_type_): Input
        product (_type_): Output. Dataframe de un solo registro.
    """
    df_defunciones_totales = pandas.read_parquet(
        str(upstream["get-deceases-1991-2017"])
    )
    df_defunciones_especificas = pandas.read_parquet(
        str(upstream["filter-cause-specific-deceases-1991-2017"])
    )

    total_deceases_arg = len(df_defunciones_totales)
    specific_deceases_arg = len(df_defunciones_especificas)

    df = pandas.DataFrame(
        columns=[
            "division",
            "total_deceases",
            "specific_deceases",
            "csmr",
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


def __filter_sex_province(df):
    """Filtra registros de varones/mujeres y los que tienen un nombre de provincia válido.

    Args:
        df (pandas.Dataframe): Un dataframe con la columna numerica `sexo` y `provincia_id`

    Returns:
        pandas.Dataframe: un dataset filtrado.
    """
    df_filtered = df.copy()

    SEX_CODE_MAPPING = {
        "1": "varon",
        "2": "mujer",
        "9": "indeterminado",
        "3": "indeterminado",
        "99": "indeterminado",
    }

    # filter sex:
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
    total_deceases_df = pandas.read_parquet(str(upstream["get-deceases-1991-2017"]))
    cause_specific_deceases = pandas.read_parquet(
        str(upstream["filter-cause-specific-deceases-1991-2017"])
    )

    total_deceases_df = __filter_sex_province(total_deceases_df)
    cause_specific_deceases = __filter_sex_province(cause_specific_deceases)

    codes_for_each_category = utils.get_codes_categorization(
        cause_specific_deceases["codigo_defuncion"]
    )

    # tenemos que etiquetar codigo->categoria
    cause_specific_deceases["group"] = cleanning.rewrite_codes_according_to_grouping(
        cause_specific_deceases["codigo_defuncion"], codes_for_each_category
    )

    specific_decease_by_group_by_year = (
        cause_specific_deceases.groupby(["year", "group"])["department_id"]
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


def get_annual_proportional_mortality_by_group_year(upstream, product):
    """
    Mortalidad proporcional para Argentina, por año y por grupo etario.
    """
    total_deceases_df = pandas.read_parquet(str(upstream["get-deceases-1991-2017"]))
    cause_specific_deceases = pandas.read_parquet(
        str(upstream["filter-cause-specific-deceases-1991-2017"])
    )

    total_deceases_df = __filter_sex_province(total_deceases_df)
    cause_specific_deceases = __filter_sex_province(cause_specific_deceases)

    annual_deceases_by_age_group_df = (
        total_deceases_df.groupby(["year", "age_group"])["department_id"]
        .count()
        .reset_index()
        .rename(columns=dict(department_id="total_deceases"))
    )

    annual_specific_deceases_by_age_group_df = (
        cause_specific_deceases.groupby(["year", "age_group"])["department_id"]
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
