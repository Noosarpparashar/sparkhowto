from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,StructType,StructField,DoubleType,IntegerType

spark = SparkSession.builder.master("local[*]").appName("changeDataType").getOrCreate()
schema = StructType([
    StructField("Id",IntegerType(),True),
    StructField("SepalLengthCm",DoubleType(),True),
    StructField("SepalWidthCm",StringType(),True),
    StructField("PetalLengthCm",DoubleType(),True),
    StructField("PetalWidthCm",DoubleType(),True),
    StructField("Species", StringType(), True),

])

df = spark.read.format("csv").option("header",True).schema(schema).load("datasets/archive/iris2.csv")
df.printSchema()
df=df.withColumn("SepalWidthCm",df["SepalWidthCm"].cast(DoubleType()))
df.printSchema()
#df.show()

