from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

def run_etl():
    # Inicializa una sesión de Spark
    spark = SparkSession.builder \
        .appName("ETL_Automatizado") \
        .getOrCreate()

    # Define la ruta de salida
    output_path = "s3://targetnyt/vehicle_fuel_economy_data.parquet"

    try:
        # Carga el archivo 
        vfed = spark.read.format("parquet").load("s3://origennyt/Normalized/vehicle_fuel_economy_data.parquet")
        
        # Selecciona las columnas requeridas
        vfed = vfed.select('Model', 'Year', 'Manufacturer', 'VClass', 'fuelType1', 'fuelType2', 'city08', 'co2')
    except AnalysisException as e:
        print("Error durante la carga del archivo Parquet:", e)
        return

    # Elimina columnas innecesarias
    columnas_innecesarias = ['fuelType1', 'fuelType2']
    for col_name in columnas_innecesarias:
        if col_name in vfed.columns:
            vfed = vfed.drop(col_name)

    # Cambia el nombre de las columnas
    renombrar_columnas = {'VClass': 'Category', 'city08': 'Consumption (mpg)', 'co2': 'CO2 (g/mile)'}
    for old_name, new_name in renombrar_columnas.items():
        if old_name in vfed.columns:
            vfed = vfed.withColumnRenamed(old_name, new_name)

    # Filtra datos por año (mayor o igual a 2019) si la columna 'Year' existe
    if 'Year' in vfed.columns:
        vfed = vfed.filter(col('Year') >= 2019)

    # Elimina  duplicados si existen
    if vfed.count() > 0:  # Verificar si hay datos para evitar errores al usar dropDuplicates()
        vfed = vfed.dropDuplicates()

    # Deja solo categorías específicas si la columna 'Category' existe
    if 'Category' in vfed.columns:
        vfed = vfed.filter(col('Category').isin(['Small Sport Utility Vehicle 4WD', 'Small Sport Utility Vehicle 2WD', 
                                                'Compact Cars', 'Midsize Cars', 'Large Cars', 'Standard Sport Utility Vehicle 4WD',
                                                'Standard Sport Utility Vehicle 2WD', 'Minivan - 2WD', 'Vans', 'Minivan - 4WD']))

    # Guarda el DataFrame en el bucket de salida
    try:
        if spark._jsparkSession.sparkContext._jobj.hadoopConfiguration().get("fs.s3a.path.style.access") == "true":
            if spark._jsparkSession._jsc.hadoopConfiguration().get("fs.s3a.impl") == "org.apache.hadoop.fs.s3a.S3AFileSystem":
                import os
                hadoop_conf = spark._jsparkSession._jsc.hadoopConfiguration()
                hadoop_conf.set("fs.s3a.path.style.access", "false")
                os.environ['AWS_ACCESS_KEY_ID']= hadoop_conf.get("fs.s3a.access.key")
                os.environ['AWS_SECRET_ACCESS_KEY'] = hadoop_conf.get("fs.s3a.secret.key")
    except Exception as e:
        print("Error al configurar el acceso a S3:", e)

    try:
        if vfed.count() > 0 and not spark._jsparkSession.sparkContext._jobj.hadoopConfiguration().get("fs.s3a.path.style.access") == "true":
            if not spark._jsparkSession._jsc.hadoopConfiguration().get("fs.s3a.impl") == "org.apache.hadoop.fs.s3a.S3AFileSystem":
                vfed.write.mode("append").parquet(output_path)
            else:
                vfed.write.mode("overwrite").parquet(output_path)
        else:
            vfed.write.mode("overwrite").parquet(output_path)
    except AnalysisException as e:
        print("Error durante el proceso de escritura:", e)

    # Cierra la sesión de Spark
    spark.stop()

# Ejecutar el ETL
run_etl()