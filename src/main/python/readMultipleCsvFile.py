from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,StructType,StructField,IntegerType,DoubleType

spark = SparkSession.builder.appName("readMultipleCsvFiles").master("local[*]").getOrCreate()

df =spark.read.format("csv").option("header",True).load("datasets/archive/iris*.csv")
df.orderBy(["SepalLengthCm"],ascending=[0]).show(100,False)
