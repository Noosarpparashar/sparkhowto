from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType

spark = SparkSession.builder.appName("groupBy Agrregate").master("local[*]").getOrCreate()

listt=[(1,2,3),(1,5,6),(7,8,9),(7,11,12),(1,14,15)]
df = spark.createDataFrame(listt,["col1","col2","col3"])
df.show()
# df.groupby("col1").sum("col2").show()
# df.groupby("col1").sum("col2","col3").show()
# df.groupby("col1").agg({'col2':'sum','col3':'avg'}).show()
# #df.groupby("col1").agg({'col2':'sum','col3':'avg'}).groupby("col1").show()
#
# df.groupby("col1").agg(
#     sum("col2").alias("sumCol2"),
#     avg("col2").alias("avgCol2"),
#     sum("col3").alias("sumCol2"),
#     mean("col3").alias("meancol3")
# ).show()
#
    """
    val result = logs.select("page","visitor")
            .groupBy('page)
            .agg('page, countDistinct('visitor))
            
            
     df2.groupBy($"page").agg(count($"visitor").as("count"))
     milestones.groupBy(col("termsheet_id")).agg(countDistinct(col("opportunity_id"))
    """

df.groupby("col1").agg(
    mean("col2").alias("mean_col2")
).show()