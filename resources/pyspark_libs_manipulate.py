from pyspark.sql import SparkSession


### NOTE: this is a tutorial on how to use PySpark

# this essentialy initiates the core engine for all our operations
spark = SparkSession.builder.appName("PySpark Tutorial").getOrCreate()

### BASIC RDD (resilient distributed dataset)

data = [1,2,3,4,5,6]
letter_pairs = [("c", 4), ("a", 1), ("c", 3)]

rdd = spark.sparkContext.parallelize(data)  # converts basic arrays/tuples to RDD
letter_pairs_rdd = spark.sparkContext.parallelize(letter_pairs)   
# RDDs are the common data structure in pyspark
# parallelize converts the array into a RDD

# print(rdd.collect()) # groups RDD data back into array form...

# mapping functions are used to apply something on each element of the RDD
squared = rdd.map(lambda x: x * x) # lambda function needed, just like Racket
# print(squared.collect())

# filter functions are used to keep items that yield true, discard those that are false
even = rdd.filter(lambda y: (y % 2) == 0)
# print(even.collect())

# can also do the similar pandas group by aggregate operations
# below basically 
reduced = letter_pairs_rdd.reduceByKey(lambda x,y: x+y )
# print(reduced.collect())

# counts number of elements
count_elements = rdd.count()
# print(count_elements)

# fetches first element
first = rdd.first()
# print(first)

# returns in an array first n elements: rdd.take(n), ex:
first_three_elements = rdd.take(3)


### DATAFRAMES OPERATIONS

# to create a PySpark df using 2d array:
data1 = [("Alice", 34), ("Bob", 36), ("Cathy", 29)] # number of outer elements equals number of rows...
df1 = spark.createDataFrame(data1, ["Name", "Age"]) # second arg are column names
# df1.show()

# to create a PySpark df using csv:
# df2 = spark.read.csv("HON.csv", header=True, inferSchema=True)
# df2.show()

# Common DF operations:

# df2.select("Open").show() # select certain columns...
# df2.filter(df.Volume > 7).show() # filter out only certain rows we want...

# SQL Queries in PySpark:
df2.createOrReplaceTempView("people") # need to initiate a certain name in engine to be accessed in query (people in this case)...
result = spark.sql("SELECT * FROM people WHERE Volume > 30") # can literaly type in SQL code!
result.show()