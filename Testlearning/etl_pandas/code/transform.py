from pandas import DataFrame
import pandas as pd
# Rename all the columns
'''
:param df: input dataframe
:param mapping_dict: dict of columns names
:return: ouput dataframe
'''
def rename_cols(df: DataFrame, mapping_dict: dict) ->DataFrame:
    
    
    df.rename(columns=mapping_dict,inplace=True)
    print(df.dtypes)
    print("--------------")
    return df


# get specific cols df
'''
:param df: input dataframe
:param specific_cols: list of columns names
:return: ouput dataframe
'''
def specific_cols(df: DataFrame, specific_cols: list):
    return df[specific_cols]


# Join two dataframes
'''
:param left_df: input dataframe
:param right_df: input dataframe
:param ON_COLUMNS: list of columns to perform join
:param JOIN_TYPE: Join type 
:return: ouput dataframe
'''
def join_df(left_df: DataFrame, right_df: DataFrame, ON_COLUMNS:list, JOIN_TYPE: str)->DataFrame:
    
    output_df=left_df.merge(right_df, on=ON_COLUMNS, how=JOIN_TYPE)
    
    return output_df

def create_ANIO_column(df: DataFrame, year: str)->DataFrame:
    
    df["ANIO"]=pd.Series([2017 for x in range(len(df.index))])
    return df

def join_df_final(left_df: DataFrame, right_df: DataFrame)->DataFrame:
    
    output_df=pd.concat([left_df, right_df],axis=0,ignore_index=True)
    
    return output_df

def correct_cols(df: DataFrame)->DataFrame:
    
    incorrect_values = df['CuantosCuartosDuermen'] > 8
    output_df_filtered = df[~incorrect_values]
    output_df = output_df_filtered

    return output_df

def correct_NOMBRE_LOCALIDAD(df: DataFrame)->DataFrame:
    
    df["NOMBRE_LOCALIDAD"] = df["NOMBRE_LOCALIDAD"].str.strip().str.lower()


    # Corregir valores inconsistentes
    df.loc[df['NOMBRE_LOCALIDAD'] == 'antonio nariño', 'NOMBRE_LOCALIDAD'] = 'antonio narino'
    df.loc[df['NOMBRE_LOCALIDAD'] == 'antonio narião', 'NOMBRE_LOCALIDAD'] = 'antonio narino'
    df.loc[df['NOMBRE_LOCALIDAD'] == 'candelaria', 'NOMBRE_LOCALIDAD'] = 'la candelaria'
    df.loc[df['NOMBRE_LOCALIDAD'] == 'santa fe', 'NOMBRE_LOCALIDAD'] = 'santa fe , Bogotá'
    df.loc[df['NOMBRE_LOCALIDAD'] == 'bosa', 'NOMBRE_LOCALIDAD'] = 'bosa , Bogotá'
    df.loc[df['NOMBRE_LOCALIDAD'] == 'san cristobal', 'NOMBRE_LOCALIDAD'] = 'san cristobal , Bogotá'
    df.loc[df['NOMBRE_LOCALIDAD'] == 'ciudad bolivar', 'NOMBRE_LOCALIDAD'] = 'ciudad bolivar , Bogotá'

    df["NOMBRE_LOCALIDAD"] = df["NOMBRE_LOCALIDAD"].str.capitalize()
    output_df = df.copy()
    return output_df

def recategorizer(df: DataFrame)->DataFrame:
    df.loc[df['CercaBasureros'] == 2, 'CercaBasureros'] = 0
    df.loc[df['CercaFabricas'] == 2, 'CercaFabricas'] = 0
    df.loc[df['PresentaHumedad'] == 2, 'PresentaHumedad'] = 0
    df.loc[df['CercaCañosResiduales'] == 2, 'CercaCañosResiduales'] = 0
    df.loc[df['PresentaGoteras'] == 2, 'PresentaGoteras'] = 0
    df.loc[df['PresentaGrietas'] == 2, 'PresentaGrietas'] = 0
    df.loc[df['PresentaFallasCañerias'] == 2, 'PresentaFallasCañerias'] = 0
    df.loc[df['PresentaEscasaVentilacion'] == 2, 'PresentaEscasaVentilacion'] = 0
    df.loc[df['ServicioBasuras'] == 2, 'ServicioBasuras'] = 0
    df.loc[df['ContaminacionAire'] == 2, 'ContaminacionAire'] = 0
    df.loc[df['MalosOlores'] == 2, 'MalosOlores'] = 0
    df.loc[df['TieneNevera'] == 2, 'TieneNevera'] = 0
    df.loc[df['DiagnosticoProblemasRespiratorios'] == 2, 'DiagnosticoProblemasRespiratorios'] = 0
    output_df = df.copy()
    return output_df
