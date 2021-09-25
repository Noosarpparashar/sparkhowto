from pyspark.sql import  SparkSession
from pyspark.sql.types import StringType,IntegerType,DoubleType,StructType,StructField
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("createEmptyDF").master("local[*]").getOrCreate()
sc = spark.sparkContext
schema = StructType([
    StructField("Id",IntegerType(),True),
    StructField("SepalLengthCm",DoubleType(),True),
    StructField("SepalWidthCm",DoubleType(),True),
    StructField("PetalLengthCm",DoubleType(),True),
    StructField("PetalWidthCm", DoubleType(), True),
    StructField("Species", StringType(), True),
                     ])
df= spark.createDataFrame([],schema=schema)
df.show()