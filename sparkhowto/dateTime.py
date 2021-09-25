from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,IntegerType,TimestampType,DoubleType
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("dateTime").getOrCreate()

sc = spark.sparkContext
df=spark.createDataFrame([["02-03-2013"],["05-06-2023"]],["input"])
df = df.select(col("input"),to_date(col("input"),"MM-dd-yyyy").alias("date"))
#This function takes the first argument as a date string and the second argument takes the pattern the date is in the first argument.
df.printSchema()

"""
+----------+----------+
|     input|      date|
+----------+----------+
|02-03-2013|2013-02-03|
|05-06-2023|2023-05-06|
+----------+----------+

"""
input = sc.parallelize([("a",1497348453),("b",1497345453),("c",1497341453),("d",1497340453)]).toDF(["name", "timestamp"])
input.printSchema()
input1=input.withColumn("timestamp",to_date(from_unixtime(col("timestamp")/1000)))
input1.printSchema()
"""
+----+----------+
|name| timestamp|
+----+----------+
|   a|1970-01-18|
|   b|1970-01-18|
|   c|1970-01-18|
|   d|1970-01-18|
+----+----------+
"""
input = input.withColumn("timestamp",from_unixtime(col("timestamp")/1000))
"""
+----+-------------------+
|name|          timestamp|
+----+-------------------+
|   a|1970-01-18 13:25:48|
|   b|1970-01-18 13:25:45|
|   c|1970-01-18 13:25:41|
|   d|1970-01-18 13:25:40|
+----+-------------------+
"""

year =input.withColumn("year",year(col("timestamp")))
"""
Similarly replace year with
month
hour
minute
"""
year.printSchema()
year.show()
#input.show()
