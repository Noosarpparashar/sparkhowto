from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,StructType,IntegerType,TimestampType,StructField
spark = SparkSession.builder.master("local[*]").appName("howTo").getOrCreate()

df = spark.read.format("csv").option("header",True).load("datasets/archive/iris.csv")
df.show()
# df =df.selectExpr("SepalLengthCm as sepallength", "Species as type")
# df.show()
newCols = ["id","sapealLength","sepalWidth","petalLength","petalWidth","type"]
from functools import reduce
oldColumns = df.schema.names
print(oldColumns)
column = df.columns
print(column)
newdf = reduce(lambda dat, idx: dat.withColumnRenamed(oldColumns[idx], newCols[idx]), range(len(oldColumns)), df)

newdf.printSchema()
newdf.show()
#newColumns= ["id","sapealLength","sepalWidth","petalLength","petalWidth","types"]
newColumns = list(map(lambda x: x.replace("a","z"),df.columns))
df.toDF(*newColumns).show()

newdf.withColumnRenamed("sapealLength","sepalLength").withColumnRenamed("type","typeOfSpecies").show()
