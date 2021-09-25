from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType,ArrayType# from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("groupBy Agrregate").master("local[*]").getOrCreate()

listt=[(1,2,3),(1,5,6),(7,8,9),(7,11,12),(1,14,15)]
df = spark.createDataFrame(listt,["col1","col2","col3"])
df.select(sum("col1")).show()
df.show()
print("*******************************************************************************")
from pyspark import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode

listt = [(1,[1,2,3],[7,8,9],"foo")]
df = spark.createDataFrame(listt,["col1","col2","col3","col4"])
df.show()
"""
+----+---------+---------+----+
|col1|     col2|     col3|col4|
+----+---------+---------+----+
|   1|[1, 2, 3]|[7, 8, 9]| foo|
+----+---------+---------+----+
"""
df.select("col1",explode("col2"),"col4").show()
#df.withColumn("tmp", arrays_zip("col2", "col3")).show(20,False)
"""
+----+---------+---------+----+------------------------+
|col1|col2     |col3     |col4|tmp                     |
+----+---------+---------+----+------------------------+
|1   |[1, 2, 3]|[7, 8, 9]|foo |[[1, 7], [2, 8], [3, 9]]|
+----+---------+---------+----+------------------------+

"""
df = df.withColumn("tmp", arrays_zip("col2", "col3")).withColumn("tmp",explode("tmp"))
df.show()
"""
+----+---------+---------+----+------+
|col1|     col2|     col3|col4|   tmp|
+----+---------+---------+----+------+
|   1|[1, 2, 3]|[7, 8, 9]| foo|[1, 7]|
|   1|[1, 2, 3]|[7, 8, 9]| foo|[2, 8]|
|   1|[1, 2, 3]|[7, 8, 9]| foo|[3, 9]|
+----+---------+---------+----+------+
"""

#df=df.withColumn("SepalWidthCm",df["SepalWidthCm"].cast(DoubleType()))
df.printSchema()
df.select("col1",col("tmp").col2, col("tmp").col3).show()
"""
+----+--------+--------+
|col1|tmp.col2|tmp.col3|
+----+--------+--------+
|   1|       1|       7|
|   1|       2|       8|
|   1|       3|       9|
+----+--------+--------+
"""
print(lit(None).alias("hello"))

data = spark.createDataFrame([(1,2), (3,4)], ['x1', 'x2'])
mapping = dict(zip(['x1', 'x2'], ['x3', 'x4']))
print(mapping)
data.select([col(c).alias(mapping[c]) for c in data.columns]).show()
data.select([col(c).alias(mapping.get(c, c)) for c in data.columns]).show()
