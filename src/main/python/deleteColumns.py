from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,DoubleType

spark = SparkSession.builder.master("local[*]").appName("deleteColumns").getOrCreate()

df = spark.read.format("csv").option("header",True).load("datasets/archive/iris2.csv")
df = df.drop(df["SepalWidthCm"])
#cols_to_be_Deleted = ["SepalWidthCm","PetalLengthCm"]
#df = df.drop(*cols_to_be_Deleted)
#df = df.drop(*["SepalWidthCm","PetalLengthCm"])
df.show()