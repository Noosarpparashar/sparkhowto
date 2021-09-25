from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,StructField,IntegerType,DoubleType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("uniqueDistinct").master("local[*]").getOrCreate()
sc = spark.sparkContext
df = spark.read.format("csv").option("header",True).load("datasets/archive/iris2.csv")
df.select("SepalLengthCm").distinct().orderBy(["SepalLengthCm"],ascending=[0]).show()

#DropDuplicates based on some column
data = sc.parallelize([('Foo',41,'US',3),
                       ('Foo',39,'UK',1),
                       ('Bar',57,'CA',2),
                       ('Bar',72,'CA',2),
                       ('Baz',22,'US',6),
                       ('Baz',36,'US',6)]).toDF(["item","price","country","score"])
data.show()
"""
+----+-----+-------+-----+
|item|price|country|score|
+----+-----+-------+-----+
| Foo|   41|     US|    3|
| Foo|   39|     UK|    1|
| Bar|   57|     CA|    2|
| Bar|   72|     CA|    2|
| Baz|   22|     US|    6|
| Baz|   36|     US|    6|
+----+-----+-------+-----+
"""
data.dropDuplicates(["item","country","score"]).show()

"""
+----+-----+-------+-----+
|item|price|country|score|
+----+-----+-------+-----+
| Bar|   57|     CA|    2|
| Foo|   39|     UK|    1|
| Baz|   22|     US|    6|
| Foo|   41|     US|    3|
+----+-----+-------+-----+
"""
data.dropDuplicates().show()
"""
|item|price|country|score|
+----+-----+-------+-----+
| Baz|   22|     US|    6|
| Foo|   39|     UK|    1|
| Bar|   72|     CA|    2|
| Bar|   57|     CA|    2|
| Foo|   41|     US|    3|
| Baz|   36|     US|    6|
+----+-----+-------+-----+
"""





