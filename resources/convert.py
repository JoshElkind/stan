from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import Window
from pyspark.sql.functions import lag, lead, avg, max, coalesce, lit, sum, array, col


spark = SparkSession.builder.appName("convert-right-format").master("local[*]").getOrCreate()

filename = "result1.csv"

df = spark.read.csv(filename, header=True, inferSchema=True)

df = df.withColumn("price", (functions.col("open") + functions.col("close")) / 2)
df = df.drop("open", "high", "low", "close", "volume")

df.coalesce(1).write.option("header", "true").csv("result1_right.csv")
