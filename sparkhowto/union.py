


from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import lit
from pyspark.sql import Row

spark = SparkSession.builder.appName("union").master("local[*]").getOrCreate()

def customUnion(df1, df2):
    cols1 = df1.columns
    cols2 = df2.columns
    total_cols = sorted(cols1 + list(set(cols2) - set(cols1)))
    def expr(mycols, allcols):
        def processCols(colname):
            if colname in mycols:
                return colname
            else:
                return lit(None).alias(colname)
        cols = map(processCols, allcols)
        return list(cols)
    appended = df1.select(expr(cols1, total_cols)).union(df2.select(expr(cols2, total_cols)))
    return appended



data = [
    Row(zip_code=58542, dma='MIN'),
    Row(zip_code=58701, dma='MIN'),
    Row(zip_code=57632, dma='MIN'),
    Row(zip_code=58734, dma='MIN')
]

firstDF = spark.createDataFrame(data)

data = [
    Row(zip_code='534', name='MIN'),
    Row(zip_code='353', name='MIN'),
    Row(zip_code='134', name='MIN'),
    Row(zip_code='245', name='MIN')
]

secondDF = spark.createDataFrame(data)

customUnion(firstDF,secondDF).show()

