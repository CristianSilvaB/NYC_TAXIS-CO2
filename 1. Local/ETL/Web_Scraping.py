import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
import shutil

#Esta funcion descarga el driver de chrome automaticamente
def config_browser():
   # Configuración
   opts = Options()
   opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

   # Instala el web driver automáticamente
   driver = webdriver.Chrome(
      service=Service(ChromeDriverManager().install()),
      options=opts )  
   
   return driver


driver = config_browser() 
wait = WebDriverWait(driver, 10) 

#IMPORTANTE!!!!
# Si lo vas a ejecutar pone la ruta donde queres que se carguen los archivos.
carpeta_destino = "..\Data\TLC_scraping"

# Crea la carpeta si no existe 
if not os.path.exists(carpeta_destino):
    os.makedirs(carpeta_destino)


url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
# Obtener la página
driver.get(url)

#Estructura de la tabla donde hay que scrapear:
# TR = Fila de la tabla. En este caso 1 fila
# TD = Celda de la tabla. En este caso 2 celdas 
# cada celda contiene varias listas(ul) y cada lista, 4 items


tabla_celdas = driver.find_elements(By.XPATH,"//div[@id='faq2023']//table//tr//td")

def obtenerLinks(celdas):
    for td in celdas:
        # Busca todas las listas(ul) dentro de la celda (td)
        listas = td.find_elements(By.TAG_NAME, "ul")   
           # Itera sobre c/u de las listas      
        for elemento_lista in listas:
           # Guarda en links_columns los enlaces
          links_columns = elemento_lista.find_elements(By.TAG_NAME, "a")
          
          for link in links_columns: #itera sobre la lista de enlces
             titulo = link.get_attribute("title")#Obtiene el titulo del enlace
             
             if (titulo == "Yellow Taxi Trip Records" or titulo == "Green Taxi Trip Records"):
                 enlace = link.get_attribute("href")
                 # Obtener el nombre del archivo a partir de la URL
                 nombre_archivo = enlace.split("/")[-1]
                 # Descarga el archivo
                 response = requests.get(enlace, stream=True)
                 # Guarda el archivo en la carpeta destino
                 ruta_archivo = os.path.join(carpeta_destino, nombre_archivo)
                 with open(ruta_archivo, 'wb') as archivo:
                     shutil.copyfileobj(response.raw, archivo)
                 print(f"Archivo '{nombre_archivo}' guardado en '{carpeta_destino}'")
                        
obtenerLinks(tabla_celdas)
driver.quit()