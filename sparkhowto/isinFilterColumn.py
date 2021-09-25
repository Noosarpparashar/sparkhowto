from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType


spark = SparkSession.builder.appName("explode").master("local[*]").getOrCreate()

listt = [(1,200,42),(2,2556,55),(1,262,5),(3,54656,1),(2,5696,55665)]
df = spark.createDataFrame(listt,["col1","col2","col3"])
checkList = [1,2]
df.filter(col("col1").isin(checkList)).show()
# if it contains this word
df.filter(df.location.contains('google.com'))