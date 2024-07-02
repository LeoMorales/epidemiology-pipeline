# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# Add description here
#
# *Note:* You can open this file as a notebook (JupyterLab: right-click on it in the side bar -> Open With -> Notebook)


# %%
# Uncomment the next two lines to enable auto reloading for imported modules
# # %load_ext autoreload
# # %autoreload 2
# For more info, see:
# https://docs.ploomber.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# %% tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = None

# This is a placeholder, leave it as None
product = None


# %%
# your code here...

# %%
import pysal
import pandas
import glob
import geopandas

# %%
import sys
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# to log a message, call logger.info
logger.info('Logging up')


# %%
def dbf2DF(dbfile, upper=True):
    # Reads in DBF files and returns Pandas DF
    gdf = geopandas.read_file(dbfile)
    return pandas.DataFrame(gdf).drop(columns=["geometry"])


# %%
# Realiza la lectura de los archivos crudos por periodos de años.
# A partir de estos archivos genera un nuevo archivo para el período 1991-2017,
# sin procesar aún, pero con una estructura comun definida.

paths_regex = "/home/lmorales/resources/mortalidad/*.dbf"
paths_regex_2 = "/home/lmorales/resources/mortalidad/*.DBF"

paths = sorted([*glob.glob(paths_regex), *glob.glob(paths_regex_2)])
dataframes = {}
for path in paths:
    filename = path.split("/")[-1].upper()
    year = filename.replace("DEFT", "").replace(".DBF", "")

    df_i = dbf2DF(path)
    df_i["year"] = year
    dataframes[year] = df_i
    logger.info(f"{year} readed!")

df_1991_2000 = pandas.concat([dataframes[str(key)] for key in range(1991, 2001)])

dataframes["2014"] = dataframes["2014"].rename(
    columns={
        "PAISRE": "PAIRE",
        "NACMUER": "NACMUE",
        "CANTMUER": "CANTMUE",
        "PROVRES": "PROVRE",
    }
)

df_2001_2014 = pandas.concat([dataframes[str(key)] for key in range(2001, 2015)])

path = "/home/lmorales/resources/mortalidad/DEFT2015.DBF"
df_2015 = dbf2DF(path)
df_2015["year"] = "2015"

df_2016_2017 = pandas.read_csv(
    "/home/lmorales/resources/mortalidad/def 2016_2017.csv",
    sep="\t",
    dtype={"DEPRES": "object"},
)
df_2016_2017["year"] = df_2016_2017["ANO"].copy()

df_2015_2017 = pandas.concat([df_2015, df_2016_2017])
# basic cleaning:
str_columns = [
    "PROVOC",
    "PAIRE",
    "PROVRE",
    "DEPRE",
]
for str_column in str_columns:
    df_1991_2000[str_column] = df_1991_2000[str_column].astype(str)

float_columns = [
    "PESNAC",
    "PESMOR",
    "EDMAD",
]
for float_col in float_columns:
    df_1991_2000[float_col] = pandas.to_numeric(
        df_1991_2000[float_col], errors="coerce"
    )
    df_1991_2000[float_col] = df_1991_2000[float_col].fillna(0.0)

int_columns = [
    "TOTEMB",
    "NACVIV",
    "NACMUE",
]
for int_col in int_columns:
    df_1991_2000[int_col] = pandas.to_numeric(
        df_1991_2000[int_col], errors="coerce"
    )
    df_1991_2000[int_col] = df_1991_2000[int_col].fillna(99)
    df_1991_2000[int_col] = df_1991_2000[int_col].astype(int)

#  2001-2014
df_2001_2014["JURI"] = df_2001_2014["JURI"].astype(str)
df_2001_2014["ANO"] = df_2001_2014["ANO"].astype(str)
df_2001_2014["year"] = df_2001_2014["year"].astype(str)

integer_columns = [
    "ATENMED",
    "MEDSUS",
    "MUERVIO",
    "EMBMUJER",
    "UNIEDA",
    "SEXO",
    "OCLOC",
    "ASOCIAD",
    "FINSTRUC",
    "FSITLABO",
    "INSTMAD",
    "CONYMAD",
    "INSTPAD",
    "SITLABOR",
    "PARTO",
    "PAIRE",
]
for column in integer_columns:
    df_2001_2014[column] = pandas.to_numeric(df_2001_2014[column], errors="coerce")
    df_2001_2014[column] = df_2001_2014[column].fillna(99)
    df_2001_2014[column] = df_2001_2014[column].astype(int)

df_2001_2014["DEPOC"] = df_2001_2014["DEPOC"].astype(str)
df_2001_2014["PROVOC"] = df_2001_2014["PROVOC"].astype(str)
df_2001_2014["DEPRE"] = df_2001_2014["DEPRE"].astype(str)
df_2001_2014["PROVRE"] = df_2001_2014["PROVRE"].astype(str)
df_2001_2014["year"] = df_2001_2014["year"].astype(str)

# 2015 - 2017:
df_2015_2017["JURI"] = df_2015_2017["JURI"].astype(str)
df_2015_2017["PROVRES"] = df_2015_2017["PROVRES"].astype(str)
df_2015_2017["DEPRES"] = df_2015_2017["DEPRES"].astype(str)
df_2015_2017["SEXO"] = pandas.to_numeric(df_2015_2017["SEXO"], errors="coerce")
df_2015_2017["SEXO"] = df_2015_2017["SEXO"].fillna(99)
df_2015_2017["SEXO"] = df_2015_2017["SEXO"].astype(int)
df_2015_2017["year"] = df_2015_2017["year"].astype(str)

df_1991_2000.to_parquet(str(product["data_1991_2000"]))
df_2001_2014.to_parquet(str(product["data_2001_2014"]))
df_2015_2017.to_parquet(str(product["data_2015_2017"]))

logger.info('Clean task finish')

# %%
