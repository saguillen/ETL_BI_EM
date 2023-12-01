
from sqlalchemy import create_engine

# Database connection details --Note: Dont do like this in production code
def connection():
     
     mydb = create_engine('postgresql://saguillen:Lbp0qYHxr5Mj@ep-noisy-term-13291448.us-east-2.aws.neon.tech/proyecto2db?sslmode=require')
     return mydb

# As all df have 'country_code' column we use that columns to join df
JOIN_ON_COLUMNS=['country_code']
JOIN_TYPE="left"

SPEC_COLS=[
    "country_code",
    "country_name",
    "region",
    "surface_area",
    "independence_year",
    "country_population",
    "life_expectancy",
    "local_name",
    "head_of_state",
    "capital",
    "country_code_2",
    "city_id",
    "city_name",
    "city_district",
    "city_population",
    "language",
    "is_official_language",
    "language_percentage"  
]



CITY_COL_DICT={
          "ID": "city_id",
          "Name": "city_name",
          "CountryCode": "country_code",
          "District": "city_district",
          "Population": "city_population"
     }


COUNTRY_COL_DICT={
          "Code": "country_code",
          "Name": "country_name",
          "Continent": "continent",
          "Region": "region",
          "SurfaceArea": "surface_area",
          "IndepYear": "independence_year",
          "Population": "country_population",
          "LifeExpectancy": "life_expectancy",
          "GNP": "gross_national_product",
          "GNPOld": "old_gross_national_product",
          "LocalName": "local_name",
          "GovernmentForm": "government_form",
          "HeadOfState": "head_of_state",
          "Capital": "capital",
          "Code2": "country_code_2"
     }    


COUNTRY_LANGUAGE_COL_DICT={
          "CountryCode": "country_code",
          "Language": "language",
          "IsOfficial": "is_official_language",
          "Percentage": "language_percentage"
     }

DATOS_2017_2021_COL_DICT={
    "NVCBP8A":"PresentaHumedad",
    "NVCBP8B":"PresentaGoteras",
    "NVCBP8C":"PresentaGrietas",
    "NVCBP8D":"PresentaFallasCañerias",
    "NVCBP8G":"PresentaEscasaVentilacion",
    "NVCBP10":"TipoVivienda",
    "NVCBP11AA":"Estrato",
    "NVCBP11D":"ServicioBasuras",
    "NVCBP12":"MaterialParedes",
    "NVCBP13":"MaterialPisos",
    "NVCBP14A":"CercaFabricas",
    "NVCBP14B":"CercaBasureros",
    "NVCBP14I":"CercaCañosResiduales",
    "NVCBP15D":"ContaminacionAire",
    "NVCBP15E":"MalosOlores",
    "NHCCPCTRL2":"CantMiembrosHogar",
    "NHCCP20":"CuantosCuartosDuermen",
    "NHCCP23":"DondePreparanAlimentos",
    "NHCCP26":"TipoEnergiaCocinar",
    "NHCCP27":"FuenteAguaAlimentos",
    "NHCCP31":"TipoServicioSanitario",
    "NHCCP32":"CantidadSanitarios",
    "NHCCP36A":"TieneLavamanos",
    "NHCCP36C":"TieneTanqueAgua",
    "NHCCP37":"TipoTratamientoBasuras",
    "NHCCP40B":"TieneNevera",
    "NHCCP40N":"TieneAnimalesCria",
    "NHCCP40P":"VehiculosNoCarro",
    "NPCFP14I":"DiagnosticoProblemasBucales",
    "NPCFP14F":"DiagnosticoProblemasRespiratorios"
}
