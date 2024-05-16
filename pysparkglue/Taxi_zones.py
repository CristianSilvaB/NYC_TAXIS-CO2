from pyspark.sql import SparkSession


def run_etl():
    # Inicializa una sesión de Spark
    spark = SparkSession.builder \
        .appName("ETL_Automatizado") \
        .getOrCreate()

    # Carga los datos desde el archivo DBF
    taxi_zones = DBF('s3://origennyt/taxi_zones.dbf')

    # Convierte los datos a un DataFrame de PySpark
    df = spark.createDataFrame(taxi_zones)

    # Elimina las columnas innecesarias si existen
    columnas_innecesarias = ['Shape_Area', 'Shape_Leng']
    columnas_existentes = [col for col in columnas_innecesarias if col in df.columns]
    if columnas_existentes:
        df = df.drop(*columnas_existentes)

    # Cambia el nombre de las columnas
    df = df.withColumnRenamed('zone', 'Zone') \
           .withColumnRenamed('OBJECTID', 'ObjectID') \
           .withColumnRenamed('borough', 'Borough')

    # Filtra filas por nombres de zona específicos
    df = df.filter((df['Zone'] == "Governor's Island/Ellis Island/Liberty Island") | (df['Zone'] == "Corona"))

    # Elimina la columna ObjectID si existe
    if 'ObjectID' in df.columns:
        df = df.drop('ObjectID')

    # Elimina duplicados
    df = df.dropDuplicates()


    # Guarda el DataFrame formato Parquet en el bucket de salida
    output_path = "s3://targetnyt/taxi_zones_cleaned.parquet"
    df.write.mode("overwrite").parquet(output_path)

    # Cierra la sesión de Spark
    spark.stop()

# Ejecutar el ETL
run_etl()