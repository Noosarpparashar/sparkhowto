from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType

spark =SparkSession.builder.appName("joinDataframes").master("local[*]").getOrCreate()
valuesA = [('Pirate', 1), ('Monkey', 2), ('Ninja', 3), ('Spaghetti', 4)]
dfA = spark.createDataFrame(valuesA, ['name', 'id'])

valuesB = [('Rutabaga', 1), ('Pirate', 2), ('Ninja', 3), ('Darth Vader', 4)]
dfB = spark.createDataFrame(valuesB, ['name', 'id'])

dfA.show()
dfB.show()

joinedDf = dfA.join(dfB, on =dfA.name==dfB.name, how= "inner")
joinedDf.show()
joinedDf = dfA.join(dfB, on =dfA.name==dfB.name, how= "left")
joinedDf.show()
joinedDf = dfA.join(dfB, on =dfA.name==dfB.name, how= "right")
joinedDf.show()
joinedDf = dfA.join(dfB, on =dfA.name==dfB.name, how= "full")
joinedDf.show()