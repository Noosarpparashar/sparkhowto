from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType,DoubleType

spark = SparkSession.builder.master("local[*]").appName("partitionWrite").getOrCreate()

sc = spark.sparkContext
df = sc.parallelize([("foo", 1.0), ("bar", 2.0), ("foo", 1.5), ("bar", 2.6)]
).toDF(["k", "v"])

df.write.partitionBy("k").json("/tmp/foo")
------------------------------------------------------------------------------------------------------------------------------

from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("FirstElementOfEachGroup").getOrCreate()
sc = spark.sparkContext
df = sc.parallelize([
  (0,"cat26",30.9), (0,"cat13",22.1), (0,"cat95",19.6), (0,"cat105",1.3),
  (1,"cat67",28.5), (1,"cat4",26.8), (1,"cat13",12.6), (1,"cat23",5.3),
  (2,"cat56",39.6), (2,"cat40",29.7), (2,"cat187",27.9), (2,"cat68",9.8),
  (3,"cat8",35.6)]).toDF(["Hour", "Category", "TotalValue"])

df.show()
w = Window.partitionBy(col("Hour")).orderBy(asc("TotalValue"))
refined_df = df.withColumn("rn", row_number().over(w)).where(col("rn") == 1)
refined_df.show()
df.createOrReplaceTempView("table")
spark.sql("select Hour, Category, TotalValue from (select *, row_number() OVER (PARTITION BY Hour ORDER BY TotalValue DESC) as rn  FROM table) tmp where rn = 1").show()

"""
user_id object_id score
user_1  object_1  3
user_1  object_1  1
user_1  object_2  2
user_2  object_1  5
user_2  object_2  2
user_2  object_2  6

What I expect is returning 2 records in each group with the same user_id, which need to have the highest score. Consequently, the result should look as the following:

user_id object_id score
user_1  object_1  3
user_1  object_2  2
user_2  object_2  6
user_2  object_1  5

"""
rdd = sc.parallelize([("user_1",  "object_1",  3),
                      ("user_1",  "object_2",  2),
                      ("user_1",  "object_1"   ,1),

                      ("user_2",  "object_1",  5),
                      ("user_2",  "object_2",  2),
                      ("user_2",  "object_2",  6)])
df = spark.createDataFrame(rdd, ["user_id", "object_id", "score"])
df.show()

w = Window.partitionBy(col("user_id")).orderBy(desc("score"))
refined_df = df.withColumn("rank", rank().over(w)).where(col("rank") > 0)
refined_df.show()