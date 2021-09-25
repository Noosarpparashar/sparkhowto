from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,FloatType,IntegerType
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("groupBySortBy").getOrCreate()
df = spark.read.format("csv").option("header",True).load("datasets/archive/iris2.csv")
#df.count().filter("`count` >= 10").orderBy('count', ascending=False).show()
df.groupby("SepalLengthCm").count().filter(col("count")>1).orderBy(["count"],ascending=[0]).show()
df.groupby("SepalLengthCm").agg({'SepalLengthCm':'count','petalLengthCm':'sum'}).show()
df.groupby("SepalLengthCm").agg(count('SepalLengthCm').alias("CountSepalLengthCm")).orderBy(["CountSepalLengthCm"],ascending=[0]).show()
#df.orderBy(["age", "name"], ascending=[0, 1]).collect()
#[Row(age=5, name='Bob'), Row(age=2, name='Alice')]