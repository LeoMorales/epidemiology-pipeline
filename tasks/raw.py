import pysal
import pandas
import glob

def dbf2DF(dbfile, upper=True): #Reads in DBF files and returns Pandas DF
    '''
    This block of code copied from https://gist.github.com/ryan-hill/f90b1c68f60d12baea81 
    Arguments
    ---------
    dbfile  : DBF file - Input to be imported
    upper   : Condition - If true, make column heads upper case
    '''
    db = pysal.lib.io.open.open(dbfile) #Pysal to open DBF
    d = {col: db.by_col(col) for col in db.header} #Convert dbf to dictionary
    #pandasDF = pd.DataFrame(db[:]) #Convert to Pandas DF
    pandasDF = pandas.DataFrame(d) #Convert to Pandas DF
    if upper == True: #Make columns uppercase if wanted 
        pandasDF.columns = map(str.upper, db.header) 
    db.close() 
    return pandasDF

def get_raw_dataframes(product):
    """
    Realiza la lectura de los archivos crudos por periodos de años.
    A partir de estos archivos genera un nuevo archivo para el período 1991-2017,
    sin procesar aún, pero con una estructura comun definida.
    """
    paths_regex = "/home/lmorales/resources/mortalidad/*.dbf"
    paths_regex_2 = "/home/lmorales/resources/mortalidad/*.DBF"

    paths = sorted([*glob.glob(paths_regex), *glob.glob(paths_regex_2)])
    dataframes = {}
    for path in paths:
        filename = path.split('/')[-1].upper()
        year = filename.replace("DEFT", "").replace(".DBF", "")
        
        df_i = dbf2DF(path)
        df_i['year'] = year
        dataframes[year] = df_i
        print(f"{year} readed!")

    
    df_1991_2000 = pandas.concat(
        [dataframes[str(key)] for key in range(1991, 2001)])
    
    dataframes["2014"] = dataframes["2014"].rename(columns={
            'PAISRE': 'PAIRE',
            'NACMUER': 'NACMUE',
            'CANTMUER': 'CANTMUE',
            'PROVRES': 'PROVRE'
        }
    )
    
    df_2001_2014 = pandas.concat(
        [dataframes[str(key)] for key in range(2001, 2015)])

    path = "/home/lmorales/resources/mortalidad/DEFT2015.DBF"
    df_2015 = dbf2DF(path)
    df_2015['year'] = "2015"
    
    df_2016_2017 = pandas.read_csv(
        "/home/lmorales/resources/mortalidad/def 2016_2017.csv",
        sep='\t',
        dtype={'DEPRES':'object'}
    )
    df_2016_2017['year'] = df_2016_2017['ANO'].copy()
    
    df_2015_2017 = pandas.concat(
        [df_2015, df_2016_2017]
    )
    # basic cleaning:
    str_columns = [
        'PROVOC',
        'PAIRE',
        'PROVRE',
        'DEPRE',
    ]
    for str_column in str_columns:
        df_1991_2000[str_column] = df_1991_2000[str_column].astype(str)

    float_columns = [
        'PESNAC',
        'PESMOR',
        'EDMAD',
    ]
    for float_col in float_columns:
        df_1991_2000[float_col] = pandas.to_numeric(df_1991_2000[float_col], errors='coerce')
        df_1991_2000[float_col] = df_1991_2000[float_col].fillna(0.0)

    int_columns = [
        'TOTEMB',
        'NACVIV',
        'NACMUE',
    ]
    for int_col in int_columns:
        df_1991_2000[int_col] = pandas.to_numeric(df_1991_2000[int_col], errors='coerce')
        df_1991_2000[int_col] = df_1991_2000[int_col].fillna(99)
        df_1991_2000[int_col] = df_1991_2000[int_col].astype(int)

    #  2001-2014   
    df_2001_2014['JURI'] = df_2001_2014['JURI'].astype(str)
    df_2001_2014['ANO'] = df_2001_2014['ANO'].astype(str)
    df_2001_2014['year'] = df_2001_2014['year'].astype(str)

    integer_columns = [
        'ATENMED',
        'MEDSUS',
        'MUERVIO',
        'EMBMUJER',
        'UNIEDA',
        'SEXO',
        'OCLOC',
        'ASOCIAD',
        'FINSTRUC',
        'FSITLABO',
        'INSTMAD',
        'CONYMAD',
        'INSTPAD',
        'SITLABOR',
        'PARTO',
        'PAIRE',
    ]
    for column in integer_columns:
        df_2001_2014[column] = pandas.to_numeric(df_2001_2014[column], errors='coerce')
        df_2001_2014[column] = df_2001_2014[column].fillna(99)
        df_2001_2014[column] = df_2001_2014[column].astype(int)


    df_2001_2014['DEPOC'] = df_2001_2014['DEPOC'].astype(str)
    df_2001_2014['PROVOC'] = df_2001_2014['PROVOC'].astype(str)
    df_2001_2014['DEPRE'] = df_2001_2014['DEPRE'].astype(str)
    df_2001_2014['PROVRE'] = df_2001_2014['PROVRE'].astype(str)
    df_2001_2014['year'] = df_2001_2014['year'].astype(str)


    # 2015 - 2017:    
    df_2015_2017['JURI'] = df_2015_2017['JURI'].astype(str)
    df_2015_2017['PROVRES'] = df_2015_2017['PROVRES'].astype(str)
    df_2015_2017['DEPRES'] = df_2015_2017['DEPRES'].astype(str)
    df_2015_2017['SEXO'] = pandas.to_numeric(df_2015_2017['SEXO'], errors='coerce')
    df_2015_2017['SEXO'] = df_2015_2017['SEXO'].fillna(99)
    df_2015_2017['SEXO'] = df_2015_2017['SEXO'].astype(int)
    df_2015_2017['year'] = df_2015_2017['year'].astype(str)

    df_1991_2000.to_parquet(str(product['1991-2000']))
    df_2001_2014.to_parquet(str(product['2001-2014']))
    df_2015_2017.to_parquet(str(product['2015-2017']))