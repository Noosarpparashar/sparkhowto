from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType


spark = SparkSession.builder.appName("explode").master("local[*]").getOrCreate()
listt = [(18,"856-yygrm"),(20,"777-psgdg")]
df = spark.createDataFrame(listt,["num","strValue"])
splitCol = split(df["strValue"],"-")
df = df.withColumn("first",splitCol.getItem(0))
df = df.withColumn("second",splitCol.getItem(1))
df.show()

listt = [(1, 'A, B, C, D'),(2, 'E, F, G'), (3, 'H,I'),(4,'J')]
df = spark.createDataFrame(listt,["numm","value"])
df = df.withColumn("letters",split(col("value"),","))
df.show()#


df = df.select("numm","value","letters",posexplode("letters").alias("position","column"))
df = df.withColumn("position",concat(lit("letters"),col("position")))
df.show()
#posexplode will give both position and will explode
"""
+----+----------+---------------+--------+------+
|numm|     value|        letters|position|column|
+----+----------+---------------+--------+------+
|   1|A, B, C, D|[A,  B,  C,  D]|letters0|     A|
|   1|A, B, C, D|[A,  B,  C,  D]|letters1|     B|
|   1|A, B, C, D|[A,  B,  C,  D]|letters2|     C|
|   1|A, B, C, D|[A,  B,  C,  D]|letters3|     D|
|   2|   E, F, G|    [E,  F,  G]|letters0|     E|
|   2|   E, F, G|    [E,  F,  G]|letters1|     F|
|   2|   E, F, G|    [E,  F,  G]|letters2|     G|
|   3|       H,I|         [H, I]|letters0|     H|
|   3|       H,I|         [H, I]|letters1|     I|
|   4|         J|            [J]|letters0|     J|
+----+----------+---------------+--------+------+
"""
print("sajdlkasjd")
df.select("numm",explode("letters")).show()
df = spark.createDataFrame(listt,["numm","value"])
df = df.withColumn("letters",split(col("value"),","))
#df=df.select("letters")
df.select("numm",explode("letters")).show()
"""
+----+---+
|numm|col|
+----+---+
|   1|  A|
|   1|  B|
|   1|  C|
|   1|  D|
|   2|  E|
|   2|  F|
|   2|  G|
|   3|  H|
|   3|  I|
|   4|  J|
+----+---+
"""
df.show()

df.select("col1",explode("col2"),"col4").show()
#df.withColumn("tmp", arrays_zip("col2", "col3")).show(20,False)
"""
+----+---------+---------+----+------------------------+
|col1|col2     |col3     |col4|tmp                     |
+----+---------+---------+----+------------------------+
|1   |[1, 2, 3]|[7, 8, 9]|foo |[[1, 7], [2, 8], [3, 9]]|
+----+---------+---------+----+------------------------+


I have a dataframe which has one row, and several columns. Some of the columns are single values, and others are lists. All list columns are the same length. I want to split each list column into a separate row, while keeping any non-list column as is.

Sample DF:

from pyspark import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode

sqlc = SQLContext(sc)

df = sqlc.createDataFrame([Row(a=1, b=[1,2,3],c=[7,8,9], d='foo')])
# +---+---------+---------+---+
# |  a|        b|        c|  d|
# +---+---------+---------+---+
# |  1|[1, 2, 3]|[7, 8, 9]|foo|
# +---+---------+---------+---+

What I want:

+---+---+----+------+
|  a|  b|  c |    d |
+---+---+----+------+
|  1|  1|  7 |  foo |
|  1|  2|  8 |  foo |
|  1|  3|  9 |  foo |
+---+---+----+------+

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
