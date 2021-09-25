from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,StructType,StructField,IntegerType,TimestampType

spark = SparkSession.builder.master("local[*]").appName("filterNullValues").getOrCreate()
sc = spark.sparkContext

listt = [
    ("a158",["666b"],[12]),
    ("7g21",["c0b5"],[45]),
    ("7g21",["c0b4"],[87]),
    ("a158",["666b","777c"],[]),
]
df = spark.createDataFrame(listt,["col1","col2","col3"])

df.select(explode("col2")).show()
df.groupby("col1").agg(collect_list(col("col2"))).show(10,False)

"""
+----+----------------------+
|col1|collect_list(col2)    |
+----+----------------------+
|a158|[[666b], [666b, 777c]]|
|7g21|[[c0b5], [c0b4]]      |
+----+----------------------+
"""

