import pandas as pd
# extracting data from filesystem
# IMported required libraries and module
import uuid

from constant import DATOS_2017_2021_COL_DICT, connection, CITY_COL_DICT, COUNTRY_LANGUAGE_COL_DICT, COUNTRY_COL_DICT, \
    JOIN_TYPE, JOIN_ON_COLUMNS, SPEC_COLS





    
from extract import extract
from transform import correct_NOMBRE_LOCALIDAD, correct_cols, create_dimension, create_fact_table, recategorizer, rename_cols, join_df, specific_cols, create_ANIO_column, join_df_final
from load import load



#### ----- Extract ----- ####

# Extracting CITY and COUNTRY data from MYSQL
#city_df = extract("db","city")
#country_df = extract("db","country")

# Extracting COUNTRYLANGUAGE data from FileSystem
#country_language_df = extract("csv","etl_pandas\data\countrylanguage.csv")

df2017 = extract("csv","../data/Datos_proyecto_II_BI_2017.csv")
df2021 = extract("csv","../data/Datos_proyecto_II_BI_2021.csv")


#print(city_df.dtypes)
#print(country_df.dtypes)
#print(country_language_df.dtypes)
print(df2017.dtypes)
print(df2021.dtypes)


#### ----- Transformation ----- ####

# 1. Selecting Columns from df2017

df_salud_2017 =df2017[["LOCALIDAD_TEX","NPCEP5","CODLOCALIDAD","COD_UPZ","NVCBP8A","NVCBP8B","NVCBP8C","NVCBP8D","NVCBP8G","NVCBP10","NVCBP11AA","NVCBP11D","NVCBP12","NVCBP13","NVCBP14A","NVCBP14B","NVCBP14I","NVCBP15D","NVCBP15E","NHCCPCTRL2","NHCCP20","NHCCP23","NHCCP26","NHCCP27","NHCCP31","NHCCP32","NHCCP36A","NHCCP36C","NHCCP37","NHCCP40B","NHCCP40N","NHCCP40P","NPCFP14I","NPCFP14F"]]
create_ANIO_column(df_salud_2017, 2017)
df_salud_2017 = rename_cols(df_salud_2017, {'LOCALIDAD_TEX':'NOMBRE_LOCALIDAD','COD_UPZ':'COD_UPZ_GRUPO','CODLOCALIDAD':'COD_LOCALIDAD'})
df_salud_2017 = df_salud_2017.dropna()
# 2. Selecting Columns from df2021

df_salud_2021 = df2021[["NOMBRE_LOCALIDAD","NPCEP5","COD_LOCALIDAD","COD_UPZ_GRUPO","NVCBP8A","NVCBP8B","NVCBP8C","NVCBP8D","NVCBP8G","NVCBP10","NVCBP11AA","NVCBP11D","NVCBP12","NVCBP13","NVCBP14A","NVCBP14B","NVCBP14I","NVCBP15D","NVCBP15E","NHCCPCTRL2","NHCCP20","NHCCP23","NHCCP26","NHCCP27","NHCCP31","NHCCP32","NHCCP36A","NHCCP36C","NHCCP37","NHCCP40B","NHCCP40N","NHCCP40P","NPCFP14I","NPCFP14F"]]
create_ANIO_column(df_salud_2021, 2021)
df_salud_2021 = df_salud_2021.dropna()

df_salud_final = join_df_final(df_salud_2017, df_salud_2021)

df_salud_final['fact_id'] = df_salud_final.apply(lambda _: uuid.uuid4(), axis=1)

df_salud_final = rename_cols(df_salud_final, DATOS_2017_2021_COL_DICT)

df_salud_final = correct_cols(df_salud_final)

df_salud_final = correct_NOMBRE_LOCALIDAD(df_salud_final)

df_salud_final = recategorizer(df_salud_final)

#Dimension 1

dimension_ubicacion = create_dimension(df_salud_final,["Estrato","NOMBRE_LOCALIDAD"], "ubicacion")

#Dimension 2

dimension_vivienda = create_dimension(df_salud_final,["MaterialParedes","MaterialPisos","TipoVivienda","ServicioBasuras","PresentaHumedad","PresentaGoteras","PresentaGrietas","PresentaFallasCañerias","PresentaEscasaVentilacion","ContaminacionAire","CercaFabricas","CercaBasureros","CercaCañosResiduales"], "vivienda")

#Dimension 3

dimension_persona = create_dimension(df_salud_final,["DiagnosticoProblemasRespiratorios","SEXO"], "persona")

#Dimension 4

dimension_fecha = create_dimension(df_salud_final,["ANIO"], "fecha")


tabla_hechos = create_fact_table(dimension_ubicacion["ubicacion_id"],dimension_vivienda["vivienda_id"],dimension_persona["persona_id"],dimension_fecha["fecha_id"])
# 1. Rename Columns


#load

constraint_primary_key = 'ALTER TABLE public."{0}" ADD CONSTRAINT "{1}" PRIMARY KEY ("{2}");'
constraint_foreing_key = 'ALTER TABLE public."{0}" ADD CONSTRAINT "{1}" FOREIGN KEY ("{2}") REFERENCES public."{3}"("{4}");'
constraint_unique = 'ALTER TABLE public."{0}" ADD CONSTRAINT "{1}" UNIQUE ("{2}");'

load("db",dimension_ubicacion, "Ubicacion",[
    constraint_unique.format("Ubicacion","ubicacion_id_U","ubicacion_id"),
    constraint_primary_key.format("Ubicacion","ubicacionPK","ubicacion_id")
    ])
load("db",dimension_vivienda, "Vivienda",[
    constraint_unique.format("Vivienda","vivienda_id_U","vivienda_id"),
    constraint_primary_key.format("Vivienda","viviendaPK","vivienda_id")
    ])
load("db",dimension_persona, "Persona",[
    constraint_unique.format("Persona","persona_id_U","persona_id"),
    constraint_primary_key.format("Persona","personaPK","persona_id")
    ])
load("db",dimension_fecha, "Fecha",[
    constraint_unique.format("Fecha","fecha_id_U","fecha_id"),
    constraint_primary_key.format("Fecha","fechaPK","fecha_id")
    ])
load("db",tabla_hechos, "TablaHechos",[
    constraint_primary_key.format("TablaHechos","tabla_hechosPK","fact_id"),
    constraint_foreing_key.format("TablaHechos","ubicacionFK","ubicacion_id","Ubicacion","ubicacion_id"),
    constraint_foreing_key.format("TablaHechos","viviendaFK","vivienda_id","Vivienda","vivienda_id"),
    constraint_foreing_key.format("TablaHechos","personaFK","persona_id","Persona","persona_id"),
    constraint_foreing_key.format("TablaHechos","fechaFK","fecha_id","Fecha","fecha_id")
    ])

"""
city_df = rename_cols(city_df, constant.CITY_COL_DICT)
country_df = rename_cols(country_df, constant.COUNTRY_COL_DICT)
country_language_df = rename_cols(country_language_df, constant.COUNTRY_LANGUAGE_COL_DICT)

# 2. Join DF with common column "country_code"
country_city_df=join_df(country_df, city_df, JOIN_ON_COLUMNS, JOIN_TYPE)
country_city_language_df= join_df(country_city_df, country_language_df, JOIN_ON_COLUMNS, JOIN_TYPE)

# 3. Get specific cols
country_city_language_df = specific_cols(country_city_language_df, SPEC_COLS)

"""

#### ----- Load Data ----- ####

# MySQL 
#load("db",country_city_language_df, "countrycitylanguage")
#load("db",df_salud_final, "EM_salud2021-2017")
# FileSystem
#load("csv",country_city_language_df, "etl_pandas/output/countrycitylanguage.csv")
load("csv",df_salud_final, "etl_pandas/output/EM_salud2021-2017.csv")


    
    
