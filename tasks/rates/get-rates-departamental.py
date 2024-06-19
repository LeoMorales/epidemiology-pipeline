# ---
# jupyter:
#   jupytext:
#     formats: py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# + papermill={"duration": 0.0061, "end_time": "2024-05-24T21:03:31.634773", "exception": false, "start_time": "2024-05-24T21:03:31.628673", "status": "completed"}
# Add description here
#
# *Note:* You can open this file as a notebook (JupyterLab: right-click on it in the side bar -> Open With -> Notebook)


# + papermill={"duration": 0.005062, "end_time": "2024-05-24T21:03:31.641456", "exception": false, "start_time": "2024-05-24T21:03:31.636394", "status": "completed"}
# Uncomment the next two lines to enable auto reloading for imported modules
# # %load_ext autoreload
# # %autoreload 2
# For more info, see:
# https://docs.ploomber.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# + papermill={"duration": 0.007835, "end_time": "2024-05-24T21:03:31.650679", "exception": false, "start_time": "2024-05-24T21:03:31.642844", "status": "completed"} tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = ["get-clean-deceases-data"]

# This is a placeholder, leave it as None
product = None


# + papermill={"duration": 0.005444, "end_time": "2024-05-24T21:03:31.657626", "exception": false, "start_time": "2024-05-24T21:03:31.652182", "status": "completed"} tags=["injected-parameters"]
# Parameters
upstream = {"get-clean-deceases-data": "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/clean/cleaned-deceases-data.parquet"}
product = {"nb": "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates/get-rates.ipynb", "data": "/home/lmorales/work/pipelines/epidemiology/epidemiology_pipeline/_products/rates/rates.csv"}


# + papermill={"duration": 41.485231, "end_time": "2024-05-24T21:04:13.144287", "exception": false, "start_time": "2024-05-24T21:03:31.659056", "status": "completed"}
import pandas
from surnames_package import utils
from epidemiologic import rates
# -

# # Lectura y ajuste de los datos

# +
data_path = str(upstream['get-clean-deceases-data'])
#"../../../epidemiology_pipeline/_products/clean/cleaned-deceases-data.parquet"

clean_df = pandas.read_parquet(data_path)

year_min = clean_df[clean_df['is_specific']]['year'].min()
year_max = clean_df[clean_df['is_specific']]['year'].max()


all_deceases_df = clean_df[
    ( clean_df['year'] >= year_min ) &
    ( clean_df['year'] <= year_max)
].copy()

WORKING_COLUMNS = ['codigo_defuncion', 'sex', 'age_in_years', 'department_id', 'provincia_id', 'year']
# -

clean_df.head()

# + papermill={"duration": 41.485231, "end_time": "2024-05-24T21:04:13.144287", "exception": false, "start_time": "2024-05-24T21:03:31.659056", "status": "completed"}
############################################################################
# totales
############################################################################
all_deceases_df = all_deceases_df[WORKING_COLUMNS].copy()

all_deceases_df['region_id'] = all_deceases_df['provincia_id'].apply(
    utils.REGION_BY_PROVINCE_CODE_DICT.get)
all_deceases_df = all_deceases_df.sort_values(
    by=["department_id", "codigo_defuncion", "sex"])
all_deceases_df = all_deceases_df.reset_index(drop=True)

all_deceases_df = all_deceases_df[WORKING_COLUMNS + ['region_id']]

############################################################################
# específicos
############################################################################
specific_deceases_df = clean_df[clean_df['is_specific']].copy()
specific_deceases_df = specific_deceases_df[WORKING_COLUMNS].copy()

specific_deceases_df['region_id'] = specific_deceases_df['provincia_id'].apply(
    utils.REGION_BY_PROVINCE_CODE_DICT.get)

specific_deceases_df = specific_deceases_df.sort_values(
    by=["department_id", "codigo_defuncion", "sex"])
specific_deceases_df = specific_deceases_df.reset_index(drop=True)

specific_deceases_df = specific_deceases_df[WORKING_COLUMNS + ['region_id']]
# -

all_deceases_df.head(3)

specific_deceases_df.head(3)

# # Tasas

# Se obtienen las tasas por sexo en columnas:

# +
df = rates.get_divisional_csmr(
    all_deceases_df, specific_deceases_df, "department_id"
)

df.head()
# -

# Se busca ordenar el dataset con columnas estandarizadas y columnas discriminantes de categoría

print(" || ".join(df.columns))

# +
cols_totals = [
    "total_deceases",
    "total_deceases_female",
    "total_deceases_male",
    "total_deceases_undetermined"
]

all_deceases_melted_df = (
    pandas.melt(
        df, id_vars=['department_id'], value_vars=cols_totals,
        var_name='sex', value_name='deceases')
    .assign(sex=lambda row: row['sex'].str.replace("total_deceases_", ""))
    .assign(sex=lambda row: row['sex'].str.replace("total_deceases", "all"))
)

cols_specific = [
    'specific_deceases',
    'specific_deceases_female',
    'specific_deceases_male',
    'specific_deceases_undetermined',
]

specific_deceases_melted_df = (
    pandas.melt(
        df, id_vars=['department_id'], value_vars=cols_specific,
        var_name='sex', value_name='specific')
    .assign(sex=lambda row: row['sex'].str.replace("specific_deceases_", ""))
    .assign(sex=lambda row: row['sex'].str.replace("specific_deceases", "all"))
)

cols_rates = [
    'csmr',
    'csmr_female',
    'csmr_male',
    'csmr_undetermined',
]

rates_melted_df = (
    pandas.melt(
        df, id_vars=['department_id'], value_vars=cols_rates,
        var_name='sex', value_name='rate')
    .assign(sex=lambda row: row['sex'].str.replace("csmr_", ""))
    .assign(sex=lambda row: row['sex'].str.replace("csmr", "all"))
)


all_deceases_melted_df.head(3)
# -


tidy_df = pandas.merge(
    pandas.merge(
        all_deceases_melted_df,
        specific_deceases_melted_df,
        on=["department_id", "sex"]),
    rates_melted_df,
    on=["department_id", "sex"],
)

tidy_df.head(3)

# Comprobar si la reorganización de columnas salió bien --> en este caso, comprobamos las tasas:

# +
tidy_df['new_rate'] = tidy_df['specific'] / tidy_df['deceases'] * 1_000
tidy_df.fillna(0, inplace=True)

# !!!
assert len(tidy_df[tidy_df['rate'] != tidy_df['new_rate']]) == 0

tidy_df = tidy_df.drop(columns='new_rate')

for deceases_col_i in ["deceases", "specific"]:
    tidy_df[deceases_col_i] = tidy_df[deceases_col_i].fillna(0).astype(int)
    

tidy_df.head()


# -

# Se define una función que nos transforma un dataset con tasas desempaquetadas en uno con las tasas con columnas categorizadas

def melt_data(df):   
    cols_totals = [
        "total_deceases",
        "total_deceases_female",
        "total_deceases_male",
    ]
    
    if "total_deceases_undetermined" in df.columns:
        cols_totals.append("total_deceases_undetermined")

    all_deceases_melted_df = (
        pandas.melt(
            df, id_vars=['department_id'], value_vars=cols_totals,
            var_name='sex', value_name='deceases')
        .assign(sex=lambda row: row['sex'].str.replace("total_deceases_", ""))
        .assign(sex=lambda row: row['sex'].str.replace("total_deceases", "all"))
    )

    cols_specific = [
        'specific_deceases',
        'specific_deceases_female',
        'specific_deceases_male',
    ]

    if "specific_deceases_undetermined" in df.columns:
        cols_specific.append("specific_deceases_undetermined")

    specific_deceases_melted_df = (
        pandas.melt(
            df, id_vars=['department_id'], value_vars=cols_specific,
            var_name='sex', value_name='specific')
        .assign(sex=lambda row: row['sex'].str.replace("specific_deceases_", ""))
        .assign(sex=lambda row: row['sex'].str.replace("specific_deceases", "all"))
    )

    cols_rates = [
        'csmr',
        'csmr_female',
        'csmr_male',
    ]

    if "csmr_undetermined" in df.columns:
        cols_rates.append("csmr_undetermined")

    rates_melted_df = (
        pandas.melt(
            df, id_vars=['department_id'], value_vars=cols_rates,
            var_name='sex', value_name='rate')
        .assign(sex=lambda row: row['sex'].str.replace("csmr_", ""))
        .assign(sex=lambda row: row['sex'].str.replace("csmr", "all"))
    )

    # assert len(all_deceases_melted_df) == len(specific_deceases_melted_df)

    tidy_df = pandas.merge(
        pandas.merge(
            all_deceases_melted_df,
            specific_deceases_melted_df,
            on=["department_id", "sex"]),
        rates_melted_df,
        on=["department_id", "sex"],
    )

    tidy_df['new_rate'] = tidy_df['specific'] / tidy_df['deceases'] * 1_000
    tidy_df.fillna(0, inplace=True)

    # !!!
    assert len(tidy_df[tidy_df['rate'] != tidy_df['new_rate']]) == 0

    tidy_df = tidy_df.drop(columns='new_rate')

    for deceases_col_i in ["deceases", "specific"]:
        tidy_df[deceases_col_i] = tidy_df[deceases_col_i].fillna(0).astype(int)

    return tidy_df


melted_df = melt_data(df)
melted_df.head()

# # Procesamiento

# + papermill={"duration": 41.485231, "end_time": "2024-05-24T21:04:13.144287", "exception": false, "start_time": "2024-05-24T21:03:31.659056", "status": "completed"}
# una tupla por cada rango etario a analizar
rangos_etarios = [(0, 150), (0, 64), (65, 150)]

# una tupla por cada periodo a analizar
periodos_de_un_anio = [(year_a, year_a) for year_a in range(1997, 2018)]
periodos_analisis = [(1997, 2017)] + periodos_de_un_anio
periodos_analisis = [(1997, 2017), (1997, 2007), (2007, 2017)]

datasets = []

############################################################################
# loop: periodos (años)
############################################################################

for year_a, year_b in periodos_analisis:

    periodo_i = f"{year_a}-{year_b}"
    print(f"Procesando período {periodo_i}", end=" ")

    ############################################################################
    # loop: rangos etarios
    ############################################################################


    for edad_base, edad_tope in rangos_etarios:
        grupo_etario_i = f"{edad_base}-{edad_tope}"
        print(grupo_etario_i, end=" ")

        all_df = all_deceases_df[
            (all_deceases_df['year'] >= year_a)
            & (all_deceases_df['year'] <= year_b)
            & (all_deceases_df['age_in_years'] >= edad_base)
            & (all_deceases_df['age_in_years'] <= edad_tope)
            
        ].copy()

        specific_df = specific_deceases_df[
            (specific_deceases_df['year'] >= year_a)
            & (specific_deceases_df['year'] <= year_b)
            &(specific_deceases_df['age_in_years'] >= edad_base)
            & (specific_deceases_df['age_in_years'] <= edad_tope)    
        ].copy()

        
        local_df = rates.get_divisional_csmr(
            all_df, specific_df, "department_id"
        )
        
        flat_df = melt_data(local_df)
        
        flat_df['period'] = periodo_i
        flat_df['age_group'] = grupo_etario_i
        
        datasets.append(flat_df)
    
    print()
    
print("Finalizado.")
    
output_df = pandas.concat(datasets)
# -
print(output_df.head())

print(len(output_df))

print(output_df.dtypes)

output_df.to_csv(str(product['data']), index=False)

# + active=""
#
#
#
#
#
#
#
#
#
#
# -

columns_renaming = {
    'department_id': "DEPARTAMENTO",
    'sex': "SEXO",
    'deceases': "FALLECIMIENTOS TOTALES",
    'specific': "FALLECIMIENTOS ESPECÍFICOS",
    'rate': "TASAS POR 1000",
    'period': "PERIODO",
    'age_group': "RANGO EDADES",    
}

output_df = output_df.rename(columns=columns_renaming)

output_df.head(3)

# +
mayores_de_64 = output_df["RANGO EDADES"] == "65-150"
solo_mujeres = output_df["SEXO"] == "female"
desde_2007_al_2017 = output_df["PERIODO"] == "1997-2017"

output_df[mayores_de_64 & solo_mujeres & desde_2007_al_2017]
