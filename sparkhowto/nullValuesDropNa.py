from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,StructType,StructField,IntegerType,TimestampType

spark = SparkSession.builder.master("local[*]").appName("filterNullValues").getOrCreate()
sc = spark.sparkContext
df = spark.read.format("csv").option("header",True).load("datasets/archive/iris2.csv")
df.show()

df.where(col("SepalWidthCm").isNull()).show()
df.where(col("SepalWidthCm").isNotNull()).show()
df.na.drop(subset=df.columns).show()
df.na.drop(subset=["SepalWidthCm","PetalLengthCm"]).show()

a="yes" if len(df.head(1))>=1 else "no"
print(a)
#Get null values
df.filter("id is null").show
