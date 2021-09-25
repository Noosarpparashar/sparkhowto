from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("castColumnToDifferentDataType").getOrCreate()
sc = spark.sparkContext
df = sc.parallelize([
  (0,"cat26",30.9), (0,"cat13",22.1), (0,"cat95",19.6), (0,"cat105",1.3),
  (1,"cat67",28.5), (1,"cat4",26.8), (1,"cat13",12.6), (1,"cat23",5.3),
  (2,"cat56",39.6), (2,"cat40",29.7), (2,"cat187",27.9), (2,"cat68",9.8),
  (3,"cat8",35.6)]).toDF(["Hour", "Category", "TotalValue"])

df.show()
df.printSchema()
df = df.withColumn("Hour",df["Hour"].cast(IntegerType()))
df.show()
df.printSchema()