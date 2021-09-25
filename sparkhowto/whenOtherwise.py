from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType,StringType,StructType,StructField,IntegerType,DoubleType,FloatType

spark = SparkSession.builder.master("local[*]").appName("whenOtherwise").getOrCreate()

sc = spark.sparkContext
df = sc.parallelize([
    ("orange", "apple"), ("kiwi", None), (None, "banana"),
    ("mango", "mango"), (None, None)
]).toDF(["fruit1", "fruit2"])

df.withColumn("vowelFinder", when(col("fruit1").startswith("o"),"yes").otherwise("No")).show()
"""
new_column_2 = when(
    col("fruit1").isNull() | col("fruit2").isNull(), 3
).when(col("fruit1") == col("fruit2"), 1).otherwise(0)
"""
df.show()