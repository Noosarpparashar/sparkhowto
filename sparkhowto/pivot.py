from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,StructField,IntegerType,DoubleType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("uniqueDistinct").master("local[*]").getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(
    [
        (0, "A", 223,"201603", "PORT"),
        (0, "A", 22,"201602", "PORT"),
        (0, "A", 422,"201601", "DOCK"),
        (1,"B", 3213,"201602", "DOCK"),
        (1,"B", 3213,"201601", "PORT"),
        (2,"C", 2321,"201601", "DOCK")
    ]
)
df_data = spark.createDataFrame(rdd, ["id","type", "cost", "date", "ship"])
df_data.groupby(df_data.id, df_data.type).pivot("date").avg("cost").show()
"""
+---+----+------+------+------+
| id|type|201601|201602|201603|
+---+----+------+------+------+
|  1|   B|3213.0|3213.0|  null|
|  2|   C|2321.0|  null|  null|
|  0|   A| 422.0|  22.0| 223.0|
+---+----+------+------+------+
"""
df_data.groupby(df_data.id, df_data.type).pivot("date").agg(first("ship")).show()
"""
+---+----+------+------+------+
| id|type|201601|201602|201603|
+---+----+------+------+------+
|  1|   B|  PORT|  DOCK|  null|
|  2|   C|  DOCK|  null|  null|
|  0|   A|  DOCK|  PORT|  PORT|
+---+----+------+------+------+
"""

rdd = sc.parallelize(
    [
        (0, "A", 223,"201603", "PORT"),
        (0, "A", 22,"201602", "PORT"),
        (0, "A", 422,"201601", "DOCK"),
        (1,"B", 3213,"201602", "DOCK"),
        (1,"B", 3213,"201601", "PORT"),
        (2,"C", 2321,"201601", "DOCK"),
        (2,"C", 2321,"201601", "DOCK"),
        (2,"C", 2321,"201601", "PORT")
    ]
)
df_data = spark.createDataFrame(rdd, ["id","type", "cost", "date", "ship"])
print("printing count")
df_data.show()

"""
+---+----+----+------+----+
| id|type|cost|  date|ship|
+---+----+----+------+----+
|  0|   A| 223|201603|PORT|
|  0|   A|  22|201602|PORT|
|  0|   A| 422|201601|DOCK|
|  1|   B|3213|201602|DOCK|
|  1|   B|3213|201601|PORT|
|  2|   C|2321|201601|DOCK|
|  2|   C|2321|201601|DOCK|
|  2|   C|2321|201601|PORT|
+---+----+----+------+----+
"""
df_data.groupby("id","type","cost","date").agg(count("ship").alias("countship")).show()
"""
+---+----+----+------+---------+
| id|type|cost|  date|countship|
+---+----+----+------+---------+
|  1|   B|3213|201601|        1|
|  0|   A| 223|201603|        1|
|  0|   A| 422|201601|        1|
|  2|   C|2321|201601|        3|
|  1|   B|3213|201602|        1|
|  0|   A|  22|201602|        1|
+---+----+----+------+---------+
"""
df_data.groupby("id","type","cost","date").agg(count("ship").alias("countship")).groupby("id","type").pivot("date").agg({'countship':'max'}).show()
"""
+---+----+------+------+------+
| id|type|201601|201602|201603|
+---+----+------+------+------+
|  1|   B|     1|     1|  null|
|  2|   C|     2|  null|  null|
|  0|   A|     1|     1|     1|
"""
df_data.show()
df_data.groupby("id","type","cost","ship","date").count().groupby("id","type").pivot("date").agg(max(struct("count", "ship"))).show()

