from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_etl():
    # Inicializa una sesión de Spark
    spark = SparkSession.builder \
        .appName("ETL_Annotations") \
        .getOrCreate()

    # Carga los datos 
    df_annotations = spark.read.parquet("s3://origennyt/Normalized/")


    # Selecciona las columnas si existen
    columnas_seleccionadas = ['borough', 'latitude', 'longitude', 'year', 'day', 'hour', '1_engine_presence']
    df_sound = df_annotations.select([col for col in columnas_seleccionadas if col in df_annotations.columns])

    # cambia el nombre de la columna '1_engine_presence' a 'engine_sound' si existe
    try:
        df_sound = df_sound.withColumnRenamed('1_engine_presence', 'engine_sound')
    except Exception as e:
        print(f"No se pudo renombrar la columna '1_engine_presence': {e}")

    # Mapea los valores de 'engine_sound' a categorías
    mapping = {-1: 'Low', 0: 'Medium', 1: 'High'}
    if 'engine_sound' in df_sound.columns:
        df_sound = df_sound.replace(mapping, subset=['engine_sound'])

    # # cambia el nombre de la columna de las columnas
    renombrar_columnas = {'borough': 'Borough', 'latitude': 'Latitude', 'longitude': 'Longitude', 'year': 'Year', 'day': 'Day', 'hour': 'Hour', 'engine_sound': 'Engine Sound'}
    for old_name, new_name in renombrar_columnas.items():
        try:
            df_sound = df_sound.withColumnRenamed(old_name, new_name)
        except Exception as e:
            print(f"No se pudo renombrar la columna '{old_name}': {e}")

 

    # Guarda el DataFrame en formato Parquet en S3
    df_sound.write.mode('overwrite').parquet("s3://origennyt/Processed/annotations_parquet")
    
    # Cierra la sesión de Spark
    spark.stop()

# Ejecutar el ETL
run_etl()
