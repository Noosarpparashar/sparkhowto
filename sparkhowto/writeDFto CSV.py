from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType

spark = SparkSession.builder.appName("writeDF").master("local[*]").getOrCreate()

df = spark.read.format("csv").load("datasets/archive/iris2.csv")
df = df.na.drop(subset=["SepalLengthCm","SepalWidthCm","PetalLengthCm","PetalWidthCm"])
df.show()
df.write.csv("datasets/archive/output")

# outputdf = spark.read.format("csv").option("header",True).load("datasets/archive/output")
# outputdf.show()
#To have only one partition
#df.repartition(1).write.csv("cc_out.csv", sep='|')
#Overwrite single partition only
df.write.mode(SaveMode.Overwrite).save("/root/path/to/data/partition_col=value")
# or prefer this approach
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
data.write.mode("overwrite").insertInto("partitioned_table")