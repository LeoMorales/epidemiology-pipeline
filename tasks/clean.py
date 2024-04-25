import pandas
import logging
from surnames_package import cleaning
from epidemiology_package import cleanning as epi_cleanning
from surnames_package import utils


def get_clean_deceases_data(product, upstream, age_group_mapping, analysis_period):
    """Devuelve la base completa de fallecimientos.

    Args:
        product (_type_): Input
        upstream (_type_): Output
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)


    df_1991_2000 = pandas.read_parquet(
        str(upstream["get-raw-deceases-data"]["1991-2000"])
    )
    df_2001_2014 = pandas.read_parquet(
        str(upstream["get-raw-deceases-data"]["2001-2014"])
    )
    df_2015_2017 = pandas.read_parquet(
        str(upstream["get-raw-deceases-data"]["2015-2017"])
    )

    COLUMNS_MAPPING_2001_2014 = {
        "JURI": "jurisdiccion",
        "ANO": "ano",
        "FECINS": "fecha_inscripcion",
        "ATENMED": "tuvo_atencion_medica",
        "MEDSUS": "atendio_medico_que_suscribe",
        "CODMUER": "codigo_defuncion",
        "MUERVIO": "muerte_violenta",
        "EMBMUJER": "estuvo_embarazada",
        "FECEMB": "fecha_terminacion_embarazo",
        "FECDEF": "fecha_defuncion",
        "FECNAC": "fecha_nacimiento",
        "EDAD": "edad",
        "UNIEDA": "unidad_edad",
        "SEXO": "sexo",
        "OCLOC": "localidad_ocurrencia",
        "DEPOC": "departamento_ocurrencia",
        "PROVOC": "provincia_ocurrencia",
        "DEPRE": "departamento_residencia",
        "PROVRE": "provincia_residencia",
        "PAIRE": "pais_residencia",
        "ASOCIAD": "asociado_a",
        "FINSTRUC": "nivel_instruccion",
        "FSITLABO": "situacion_laboral",
        "INSTMAD": "nivel_instruccion_madre",
        "EDMAD": "edad_madre",
        "CONYMAD": "situacion_conyugal_madre",
        "INSTPAD": "nivel_instruccion_padre",
        "SITLABOR": "situacion_laboral_padre",
        "PESNAC": "peso_al_nacer",
        "PESMOR": "peso_al_morir",
        "PARTO": "tipo_parto",
        "CANTVIV": "nacidos_vivos_del_parto",
        "CANTMUE": "nacidos_muertos_del_parto",
        "TIEMGEST": "semanas_gestacion",
        "ULTMENS": "fecha_ultima_menstruacion",
        "TOTEMB": "total_embarazos_madre",
        "NACVIV": "hijos_nacidos_vivos_madre",
        "NACMUE": "defunciones_fetales_madre",
    }

    COLUMNS_MAPPING_1991_2000 = {
        "JURI": "jurisdiccion",
        "ANO": "ano",
        "MES": "mes_defuncion",
        "PROVOC": "jurisdiccion_ocurrencia",
        "DEPOC": "departamento_ocurrencia",
        "PAIRE": "pais_residencia",
        "PROVRE": "provincia_residencia",
        "DEPRE": "departamento_residencia",
        "FECDEF": "fecha_defuncion",
        "FECNAC": "fecha_nacimiento",
        "PAINAC": "pais_nacimiento",
        "PROVNAC": "provincia_nacimiento",
        "SEXO": "sexo",
        "EDAD": "edad",
        "UNIEDA": "unidad_edad",
        "SITCON": "situacion_conyugal",
        "INSTRU": "nivel_instruccion",
        "CONDACT": "condicion_actividad",
        "PESNAC": "peso_al_nacer",
        "PESMOR": "peso_al_morir",
        "PARTO": "tipo_parto",
        "CANTVIV": "nacidos_vivos_del_parto",
        "EDMAD": "edad_madre",
        "TOTEMB": "total_embarazos_madre",
        "NACVIV": "hijos_nacidos_vivos_madre",
        "NACMUE": "defunciones_fetales_madre",
        "CONYMAD": "situacion_conyugal_madre",
        "INSTMAD": "nivel_instruccion_padre",
        "ACTMAD": "condicion_actividad_madre",
        "CODMUER": "codigo_defuncion",
    }

    COLUMNS_MAPPING_2015_2017 = {
        "JURI": "jurisdiccion",
        "ANO": "ano",
        "MESDEF": "mes_ano_defuncion",
        "PROVRES": "provincia_residencia",
        "DEPRES": "departamento_residencia",
        "CODMUER": "codigo_defuncion",
        "SEXO": "sexo",
        "EDAD": "edad",
        "UNIEDAD": "unidad_edad",
    }

    # column renaming:
    df_1991_2000 = df_1991_2000.rename(columns=COLUMNS_MAPPING_1991_2000)
    df_2001_2014 = df_2001_2014.rename(columns=COLUMNS_MAPPING_2001_2014)
    df_2015_2017 = df_2015_2017.rename(columns=COLUMNS_MAPPING_2015_2017)

    # get province and department IDs:
    df_1991_2000["provincia_id"] = cleaning.rewrite_province_codes(
        df_1991_2000["provincia_residencia"].astype(int)
    )

    df_1991_2000["department_id"] = cleaning.rewrite_department_codes(
        df_1991_2000["departamento_residencia"].astype(int),
        df_1991_2000["provincia_id"],
    )

    df_2001_2014["provincia_residencia_clean"] = df_2001_2014[
        "provincia_residencia"
    ].str.replace(".0", "", regex=False)
    df_2001_2014["departamento_residencia_clean"] = df_2001_2014[
        "departamento_residencia"
    ].str.replace(".0", "", regex=False)

    df_2001_2014["provincia_id"] = cleaning.rewrite_province_codes(
        df_2001_2014["provincia_residencia_clean"]
    )

    df_2001_2014["department_id"] = cleaning.rewrite_department_codes(
        df_2001_2014["departamento_residencia_clean"], df_2001_2014["provincia_id"]
    )

    df_2015_2017["provincia_residencia_clean"] = df_2015_2017[
        "provincia_residencia"
    ].str.replace(".0", "", regex=False)
    df_2015_2017["departamento_residencia_clean"] = df_2015_2017[
        "departamento_residencia"
    ].str.replace(".0", "", regex=False)

    df_2015_2017["provincia_id"] = cleaning.rewrite_province_codes(
        df_2015_2017["provincia_residencia_clean"]
    )

    df_2015_2017["department_id"] = cleaning.rewrite_department_codes(
        df_2015_2017["departamento_residencia_clean"], df_2015_2017["provincia_id"]
    )

    # months cleanning:

    df_2015_2017["mes_defuncion"] = df_2015_2017["mes_ano_defuncion"].str.slice(0, 2)
    df_2001_2014["mes_defuncion"] = (
        df_2001_2014["fecha_defuncion"].str.split("/").str[1]
    )
    df_1991_2000["mes_defuncion"] = df_1991_2000["mes_defuncion"].str.strip()

    # caso 1: hay fechas con 3 digitos que nos dan mal en el campo MES original
    # asi que detectamos esos casos y nos quedamos con el primero de los digitos
    # del campo fecha_defuncion (fecha defuncion viene orignal, sin limpiar)
    dates_with_3_digits = df_1991_2000["fecha_defuncion"].str.len() == 3
    df_1991_2000.loc[dates_with_3_digits, "mes_defuncion"] = "0" + df_1991_2000.loc[
        dates_with_3_digits, "fecha_defuncion"
    ].str.slice(0, 1)

    # caso 2: mes tiene un solo digito.
    # si ademas la fecha tiene 4 digitos, usamos los dos primeros
    # para corregir el mes de defuncion
    meses_que_han_quedado_de_un_digito = df_1991_2000["mes_defuncion"].str.len() == 1

    fecha_tiene_4_digitos = df_1991_2000["fecha_defuncion"].str.len() == 4

    condition = meses_que_han_quedado_de_un_digito & fecha_tiene_4_digitos

    df_1991_2000.loc[condition, "mes_defuncion"] = df_1991_2000.loc[
        condition, "fecha_defuncion"
    ].str.slice(0, 2)

    # caso 3: mes tiene un solo digito.
    # despues de limpiar los caso 2, rellenar los que quedaron con len = 1
    # con ceros en la izquierda.
    mes_con_un_solo_digito = df_1991_2000["mes_defuncion"].str.len() == 1
    df_1991_2000.loc[mes_con_un_solo_digito, "mes_defuncion"] = df_1991_2000.loc[
        mes_con_un_solo_digito, "mes_defuncion"
    ].str.zfill(2)

    # caso aislado
    # df_1991_2000.loc[df_1991_2000['mes_defuncion'] == '49', 'mes_defuncion'] =\
    #    df_1991_2000.loc[df_1991_2000['mes_defuncion'] == '49', 'fecha_defuncion'].str.slice(0, 2)

    # caso 4: Si no tienen un mes válido y tienen una fecha de 4 digitos,
    # tomamos los dos primeros dígitos de esa fecha (último recurso)

    # [f'0{digit}' for digit in range(1, 10)]
    valid_months = [
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
    ]

    doesnt_have_a_valid_month = ~df_1991_2000["mes_defuncion"].isin(valid_months)
    date_with_four_digits = df_1991_2000["fecha_defuncion"].str.len() == 4

    condition = doesnt_have_a_valid_month & date_with_four_digits

    df_1991_2000.loc[condition, "mes_defuncion"] = df_1991_2000.loc[
        condition, "fecha_defuncion"
    ].str.slice(0, 2)

    # limpieza meses del 2001 al 2014:

    df_2001_2014["mes_defuncion"] = df_2001_2014["mes_defuncion"].str.strip()
    df_2001_2014["mes_defuncion"] = df_2001_2014["mes_defuncion"].str.zfill(2)

    # casos 20010927:
    no_bar_and_len_8 = (~df_2001_2014["fecha_defuncion"].str.contains("/")) & (
        df_2001_2014["fecha_defuncion"].str.len() == 8
    )

    df_2001_2014.loc[no_bar_and_len_8, "mes_defuncion"] = df_2001_2014.loc[
        no_bar_and_len_8, "fecha_defuncion"
    ].str.slice(4, 6)

    # casos 01/22/01:
    has_bar_and_not_valid_month = (
        df_2001_2014["fecha_defuncion"].str.contains("/")
    ) & (~df_2001_2014["mes_defuncion"].isin(valid_months))

    df_2001_2014.loc[has_bar_and_not_valid_month, "mes_defuncion"] = (
        df_2001_2014.loc[has_bar_and_not_valid_month, "fecha_defuncion"]
        .str.split("/")
        .str[0]
    )

    df_2001_2014["mes_defuncion"] = df_2001_2014["mes_defuncion"].str.strip()
    df_2001_2014["mes_defuncion"] = df_2001_2014["mes_defuncion"].str.zfill(2)

    # limpieza meses del 2015 al 2017:
    month_number_mapping = {
        "ene": "01",
        "feb": "02",
        "mar": "03",
        "abr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "ago": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dic": "12",
    }

    contains_hyphen = df_2015_2017["mes_ano_defuncion"].str.contains("-")

    df_2015_2017.loc[contains_hyphen, "mes_defuncion"] = (
        df_2015_2017.loc[contains_hyphen, "mes_ano_defuncion"]
        .str.split("-")
        .str[0]
        .apply(lambda m: month_number_mapping.get(m, "NOT-FOUND"))
    )

    common_columns = [
        "provincia_id",
        "department_id",
        "codigo_defuncion",
        "sexo",
        "edad",
        "unidad_edad",
        "year",
        "mes_defuncion",
    ]

    df = pandas.concat(
        [
            df_1991_2000[common_columns],
            df_2001_2014[common_columns],
            df_2015_2017[common_columns],
        ]
    )
    
    # 06217 es Chascomus antes del 2011, así que homogeneizamos reescribiendo a 06218 (Chascomus actual, y el usado en la capa geográfica)
    df["department_id"] = df["department_id"].replace("06217", "06218")

    integer_cols = ["sexo", "unidad_edad", "year"]
    for int_col in integer_cols:
        df[int_col] = pandas.to_numeric(df[int_col], errors="coerce")
        df[int_col] = df[int_col].fillna(99)
        df[int_col] = df[int_col].astype(int)

    starting_year = analysis_period[0]
    finish_year = analysis_period[1]
    
    logger.info(f'Filtrando años {starting_year} - {finish_year}')

    df = df[df['year'] >= starting_year].copy()
    df = df[df['year'] <= finish_year].copy()
    
    logger.info(f'Filtrado finalizado')

    df["age_in_years"] = epi_cleanning.get_age_in_years(df["edad"], df["unidad_edad"])

    # reescribir variable sexo
    SEX_CODE_MAPPING = {
        "1": "male",
        "2": "female",
    }

    df["sex"] = (
        df["sexo"]
        .astype(str)
        .map(lambda sex_code: SEX_CODE_MAPPING.get(sex_code, "undetermined"))
    )

    codigos_provincia_validos = list(utils.PROVINCE_NAME_BY_ID.keys())
    df = df[df["provincia_id"].isin(codigos_provincia_validos)]

    # asignar etiqueta de grupo etario
    df["age_group"] = "NOT-ASSIGNED"

    # por cada etiqueta y rango:
    for label, label_range in age_group_mapping.items():
        range_a, range_b = label_range
        age_range = list(range(range_a, range_b + 1))

        range_mask = df["age_in_years"].isin(age_range)
        df.loc[range_mask, "age_group"] = label

    selected_cols = [
        "provincia_id",
        "department_id",
        "codigo_defuncion",
        "sex",
        "year",
        "age_in_years",
        "age_group",
    ]
    df = df[selected_cols]

    df = df.reset_index(drop=True)
    df.to_parquet(str(product))


# def profile_cleaned_deceases_data(upstream, product):
#    df = pandas.read_parquet(upstream["get-clean-deceases-data"])
#    profile = ProfileReport(df, title="Clean Data Profiling Report")
#    profile.to_file(str(product))
