from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def run_etl():
    # Inicializa sesión de Spark
    spark = SparkSession.builder \
        .appName("ETL_Automatizado_Electric_Fuel_Stations") \
        .getOrCreate()

    # Carga los datos desde el archivo Parquet
    try:
        electric_fuel_stations = spark.read.parquet("s3://origennyt/Normalized/electric_alternative_fuel_stations.parquet")
    except AnalysisException as e:
        print("No se pudo cargar el archivo Parquet:", e)
        return

    # # Selecciona las columnas requeridas
    columnas_requeridas = ['Fuel Type Code', 'Station Name', 'Street Address', 'City', 'State', 'Latitude', 'Longitude']
    try:
        electric_fuel_stations = electric_fuel_stations.select(*columnas_requeridas)
    except AnalysisException as e:
        print("Una o más columnas requeridas no existen en el DataFrame. Continuando sin filtrar columnas:", e)

    # Filtra por estado de Nueva York si la columna 'State' existe
    if 'State' in electric_fuel_stations.columns:
        electric_fuel_stations = electric_fuel_stations.filter(electric_fuel_stations['State'] == 'NY')

    # Filtra por los 5 distritos principales de la ciudad de Nueva York si la columna 'City' existe
    if 'City' in electric_fuel_stations.columns:
        electric_fuel_stations = electric_fuel_stations.filter(electric_fuel_stations['City'].isin(['Brooklyn', 'Manhattan', 'Queens', 'Bronx', 'Staten Island']))

    # cambia el nombre de la columna 'City' a 'Borough' si existe
    if 'City' in electric_fuel_stations.columns:
        electric_fuel_stations = electric_fuel_stations.withColumnRenamed('City', 'Borough')

    # Elimina la columna 'State' si existe
    if 'State' in electric_fuel_stations.columns:
        electric_fuel_stations = electric_fuel_stations.drop('State')

    
    if electric_fuel_stations.count() > 0:
        electric_fuel_stations.show()

    # Guarda el DataFrame formato Parquet en el bucket de salida
    output_path = "s3://targetnyt/s3://targetnyt/Electric_and_Alternative_Fuel_Charging_Stations_cleaned.parquet/"
    electric_fuel_stations.write.mode("overwrite").parquet(output_path)

    # Cierra la sesión de Spark
    spark.stop()

# Ejecutar el ETL
run_etl()

