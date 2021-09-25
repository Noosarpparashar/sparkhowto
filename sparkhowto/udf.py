from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("testUdf").master("local[*]").getOrCreate()

sc = spark.sparkContext
# schema = StructType([
#     StructField("foo", FloatType(), False),
#     StructField("bar", FloatType(), False)
# ])


def udf_test1(n):
    if n>1:
        return float(n)
    else:
        return float(80)
test_udf1 = udf(udf_test1)
df = sc.parallelize([(1, 2.0), (2, 3.0)]).toDF(["x", "y"])
foobars = df.select(test_udf1("x").alias("foobar"))
foobars.printSchema()
foobars.show()

# udf returns aray instead of single value
# schema = StructType([
#     StructField("foo", FloatType(), False),
#     StructField("bar", FloatType(), False)
# ])
#
# def udf_test(n):
#     return (n / 2, n % 2) if n and n != 0.0 else (float('nan'), float('nan'))
#
# test_udf = udf(udf_test, schema)
# df = sc.parallelize([(1, 2.0), (2, 3.0)]).toDF(["x", "y"])
#
# foobars = df.select(test_udf("y").alias("foobar"))
# foobars.printSchema()
# ## root
# ##  |-- foobar: struct (nullable = true)
# ##  |    |-- foo: float (nullable = false)
# ##  |    |-- bar: float (nullable = fa

