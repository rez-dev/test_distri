from pyspark.sql import SparkSession
# Crea una instancia de SparkSession
spark = SparkSession.builder \
.appName("Spark Cluster Example") \
.master("spark://186.173.27.94:7077") \
.getOrCreate()
# Crea un DataFrame de ejemplo
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])
# Realiza operaciones en el DataFrame
df.show()
result = df.rdd.map(lambda row: (row["Name"], row["Age"] * 2)).collect()
print(result)
# Detiene la sesión de Spark
spark.stop()