from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType,DoubleType


spark = SparkSession.builder.appName("union").master("local[*]").getOrCreate()

df = spark.read.format("csv").option("header",True).load("datasets/archive/iris2.csv")
df = df.withColumn("SepalWidthCm",df["SepalWidthCm"].cast(DoubleType()))
df.show()
df.printSchema()
df = df.na.drop(subset=["SepalWidthCm"])
rdd = df.rdd.map(lambda x: (x.SepalLengthCm, x.PetalLengthCm, x.SepalWidthCm*10))
for i in rdd.collect():
    print(i)
