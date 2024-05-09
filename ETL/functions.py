# Importar librerias necesarias
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import warnings
warnings.filterwarnings("ignore")

def get_neighborhood_names(url):
    # Envia una peticion GET al sitio web
    response = requests.get(url)
    
    # Analiza el contenido del HTML
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Busca el menu de navegacion que contiene los nombres
    nav_menu = soup.find("nav", class_="folder-nav")
    
    # Crea una lista para almacenar los barrios
    neighborhoods = []
    
    # Extrae el nombre de cada barrio del menu de navegacion
    if nav_menu:
        # Encuentra todos los elementos tipo lista del menu de navegacion
        list_items = nav_menu.find_all("li", class_="page-collection")
        for li in list_items:
            # Extrae el texto del link
            neighborhood = li.get_text(strip=True)
            neighborhoods.append(neighborhood)
    
    return neighborhoods

# Lista de URLs
urls = {
    'Queens': "https://www.cityneighborhoods.nyc/queens-neighborhoods",
    'Bronx': "https://www.cityneighborhoods.nyc/the-bronx-neighborhoods",
    'Manhattan': "https://www.cityneighborhoods.nyc/manhattan-neighborhoods",
    'Staten Island': "https://www.cityneighborhoods.nyc/staten-island-neighborhoods",
    'Brooklyn': "https://www.cityneighborhoods.nyc/brooklyn-neighborhoods"
}

# Diccionario para almacenar los resultados
neighborhoods_dict = {}

# Iterar sobre las URLs y obtener los barrios para cada distrito
for district, url in urls.items():
    neighborhoods = get_neighborhood_names(url)
    neighborhoods_dict[district] = neighborhoods


def get_borough(location):
    # Itera sobre cada par clave-valor en el diccionario neighborhoods_dict
    for borough, neighborhoods in neighborhoods_dict.items():
        # Verifica si algún barrio en la lista de barrios del distrito está presente en la ubicación
        if any(neighborhood in location for neighborhood in neighborhoods):
            # Si encuentra un barrio en la ubicación, devuelve el nombre del distrito (borough)
            return borough
    
    # Si no se encuentra ningún distrito correspondiente, devuelve "Unknown"
    return "Unknown"


#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#


def data_characteristics(df):
    list = []
    for columna in df.columns:
        cant_nulos = df[columna].isnull().sum()
        tipo_dato = df[columna].dtypes
        porcen_null = round((cant_nulos / len(df))* 100)

        list.append({'Columna':columna,
                     'Tipo de dato':tipo_dato,
                     'Cantidad nulos':cant_nulos,
                     'Porcentaje':f'{porcen_null}%'})

    return pd.DataFrame(list)


#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#


# Define una función para extraer el año de la columna 'Time Period'
def extract_year(time_period):
    # Busca el año utilizando una expresión regular
    match = re.search(r'\d{4}', time_period)
    if match:
        # Si se encuentra un año, devuelve el año como un entero
        return int(match.group())
    else:
        # Si no se encuentra un año, devuelve None
        return None
    

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#


def fuel(fuel):
    if fuel == 'X' or fuel == 'Z':
        return 'Gasoline'
    elif fuel == 'D':
        return 'Diesel'
    elif fuel == 'E':
        return 'E85'
    elif fuel == 'N':
        return 'Natural gas'
    elif fuel == 'B':
        return 'Electric'
    else:
        return 'Unknown'