from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import Window
from pyspark.sql.functions import lag, lead, avg, max, coalesce, lit, sum, array, col


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
# df2 = spark.read.csv("HON.csv", header=True, inferSchema=True)
# df2.createOrReplaceTempView("people") # need to initiate a certain name in engine to be accessed in query (people in this case)...
# result = spark.sql("SELECT * FROM people WHERE Volume > 30") # can literaly type in SQL code!
# result.show()

### How PySpark works:
# builds up a queue of operations / mutations to be done to the df
# but only computes the results when a certain "eager operation" wants to access something

# EX: elect, filter, groupBy, withColumn transformations do not immediately compute mutations when
# called on df, but then when we call show, count, collect, write, the PySpark engine calls the tranformations in
# an efficient way that way its optimizwed

### Starting a SparkSession (entry point to PySpark functionality)
spark = SparkSession.builder.appName("PySparkTutorial").master("local[*]").getOrCreate()
# the .master("local[*]") basically telsl the session that we want to use all local CPUs cores
# we need to change this in CLOUD or actual production when using Kubernetes etc...

### Selecting Columns and Rows
df2 = spark.read.csv("HON.csv", header=True, inferSchema=True) # first read csv into df

# can now select certain columns using the .select("colname1", "colname2") method
short_df2 = df2.select("date", "volume")

# however it is better practise that if we use dynamic column name variables we use .col(var_colname)
# MUST import functions from pyspark.sql:

var_col_name1 = "date"
var_col_name2 = "high"
# short_df2 = df2.select(functions.col(var_col_name1).alias("new_date"), functions.col(var_col_name2))
# NOTE: .alias() basically renames the column in the new df


### FILTERING ROWS
# can use .filter() or .where() methods with a condition

# can make the condition using PySpark logic:
# df2_expensive = df2.filter(functions.col("high") > 144.60)
# df2_expensive = df2.where(functions.col("high") > 144.60)

# or can use SQL logic (as a string), obviously must know SQL well for this...
# df2_expensive = df2.filter("high > 144.60 AND volume > 80000")
# df2_expensive = df2.where("high > 144.60 AND volume > 80000")


### ADDING/TRANSFORMING COLUMNS

# can create new column by doing .withColumn("newcol_name", expression)
# or can just edit an exisiting column by using same name instead of "newcol_name"
# NOTE: the expression must ONLY involve columns, can't just put 5, or "hello" to make every
# value in the column that...

### EX:
# df2_add_avgprice = df2.withColumn("avg_price", (functions.col("high") + functions.col("low")) / 2)
# df2_twice_volume = df2.withColumn("volume", functions.col("volume") * 2)

# USING LITERALS:
# df2_with_ticker = df2.withColumn("ticker", "APPL") # CANT DO THIS...
# df2_with_ticker = df2.withColumn("ticker", functions.lit("APPL")) # MUST DO THIS INSTEAD

### TO GET # OF ROWS:
# total_rows = df2.count()

### BASIC AGGREGATIONS AND GROUPING

# avg_volume_df2 = df2.select(functions.avg("volume").alias("volume_avg"))
# avg_volume_df2.show()

### can group by unique values in a specified column

# below, we group by repeated volume values, our aggregating functions
# put entries of how many rows share the volume value (count), also we find sum
# of all the rows' high values, and the average of all the rows' low values under same grouping
'''
summary_df2 = df2.groupBy("volume").agg(
    functions.count("*").alias("count"),
    functions.sum("high").alias("total_high"),
    functions.avg("low").alias("avg_low")
)
summary_df2.show()
'''

# SOME OTHER COMMON AGGREGATING FUNCTIONS ARE:

# .countDistinct(col1, col2, ...) - can supply one col to find number of distinct 
# row values in that col, or can supply multiple to find # of unique combinations of those supplied rows...

# .sumDistinct(colname) - sums all distinct values in that col

# .product(colname) - multiplies all values in that column belongin to same grouping

# .min(colname) - returns the minimum value

# .max(colname) - returns the maximum value

# .collect_list(colname) - makes a list of all values (puts it into that spot)

# .collect_set(colname) - makes a set of all values

# .first(colname) - takes the first value appearing in col of that grouping

# .last(colname) - takes the last value appearing in col of that grouping

### CONDITIONAL AGGREGATIONS:
# how do conidtionals work in PySpark: .when(expression, value_if_expression_true).otherwise(value_if_expression_false)
# EX:
'''
df2_cond_group_mult_volume = df2.groupBy("close").agg(
    functions.product(functions.when(df2.volume > 0, df2.volume).otherwise(1)).alias("volume_mult")
)
df2_cond_group_mult_volume.show()
'''

### NOTE: we can use multiple  .when().otherwise(), below we put a cap on how large the multiplied volume can be
'''
df2_cond_group_mult_volume_cap = df2.groupBy("close").agg(
    functions.when(functions.product(functions.when(df2.volume > 0, df2.volume).otherwise(1)) < 1000, functions.product(functions.when(df2.volume > 0, df2.volume).otherwise(1))).otherwise(1000).alias("volume_mult")
)
df2_cond_group_mult_volume_cap.show()
'''

### ARRAYS AND POSITIONS

# array(col1_val, col2_val, ...) - will create array of column objects we can work with

# array_posititon(array, value_to_search_for) - will return the position (not index) of search val in specified array

### WRITING TO CSV:
# df2.write.option("header", "true").csv("out1.csv") # we add extra option to show header with col names


### SORTING AND ORDERING DATA
# to oder in ascending order by columns name: .orderBy(col, ascending=Bool)
# can order by multiple cols (earlier col takes priority in our listing): .orderBy([col1, col2], ascending=[Bool1, Bool2])
# EX:
# latest_to_oldest_data_df = df2.orderBy("date", ascending=False)
# latest_to_oldest_data_df.show()

### NOTE: we can only use date ordering when the values are in datetime form,
# in this case we are good, but if the dates weren't in that form we would have
# to convert it with: df2 = df2.withColumn("date", col("date").cast("timestamp"))


### PARTIONING - repartition() AND coalesce()

# large operations on dataframes usually benefit from parralelism since we are
# able to use all cores in the cluster and none are left not working
# thus when we repartition(num) to a larger num, we end up using more cores and it 
# speeds up our operations
# NOTE: the repartition splits up our dataframe into multiple smaller dataframes (rows get split up),
# thus if we are doing any operations that require cross-analysis between rows, partitioning
# is not recommended (not possible...)

# we can use coalesce(num), where num is smaller than the current # of partions used,
# we do this when we are done our major operations and wish to write to a single file
# NOTE: if we have more than one partition active and write to file then each partition will
# do so causing multiple file writing and files to be created
# NOTE: coalesce() is very usefull when for example filtering reduces the size of each partition
# thus further tasks won't need as many cores dedicated to each leaving many unused,
# so usefull to decrease number of partitions after the power of computation requirement decreases

# We usually want to use 2 - 4 times our amount of cpu cores of partitions (use 2 times to be safe)
# over use leads to a queue build up of cores running small tasks
# under use leads to cores not being used to their full potential/capability
# NOTE: to check number of cores on your local pc, run:
# num_cores = spark.sparkContext.defaultParallelism
# print(f"Number of CPU cores available: {num_cores}")
### IN OUR CASE: we have 8 cpu cores available thus when doing large procceses we should partition 16 times

### REMOVING COLUMNS
# to effeciently remove a column from a dataframe use: .drop(col1, col2, ...)

### RELATIONSHIPS BETWEEN CLOSE ROWS (Windows)
# windows allow us to view data surrounding given rows based on a certain ordering operation
# can also be partitioned for improved efficiency

# we can use functions such as .lead() (to look foward), and .lag() (to look back) within the same partition
# EX:

window = Window.orderBy("date")
df2_augmented = df2.withColumn("3prev_open", lag("open", 3, 0).over(window)).withColumn("1foward_close", lead("close", 1, 0).over(window))
# as you see, the lag(), and lead() takes the column its looking at, the offset, and the value in case none/NULL/eof

# WE CAN ALSO GIVE CONTEXT TO AGGREGATION FUNCTIONS USING WINDOWS: 
# EX:

window_10_before = Window.orderBy("date").rowsBetween(-10,0) # Fixed-size rolling window (rowsBetween)
# now if we want to add an extra column with sum of previous 10 day volumes:
df2_with_volume_prev_10 = df2.withColumn("10_Prev_Volume_Added", sum("volume").over(window_10_before))
# could also do .rowsBetween(0, 17) to specify context to next 17 rows...

windows_deviate_10 = Window.orderBy("volume").rangeBetween(-5,5) # must match and make sense, we couldn't do range between on date cause how would that work
df2_within_5 = df2.withColumn("within5_volumes", avg("volume").over(windows_deviate_10))
# this includes new column that takes the avg of all volumes within 10 deviation of 
# the current volume on each row (includes the value on that row as well)

# SOME EXAMPLE USAGES:

# lets add an extra column shows wether or not at least one row in next three has volume > 0
w = Window.orderBy("date").rowsBetween(1,3)
df_ex1 = df2.withColumn("bool", functions.when(max("volume").over(w) > 0, 1).otherwise(0))

# find which row ahead (1,2,or 3) has volume > 0, if none put 0 in col
'''
w = Window.orderBy("date")
df2_next_three_volumes = df2.withColumn("one_ahead_volume", lead("volume", 1).over(w)).withColumn("two_ahead_volume", lead("volume", 2).over(w)).withColumn("third_ahead_volume", lead("volume", 3).over(w))
df2_next_three_volumes = df2_next_three_volumes.withColumn("next_zero_position", coalesce(
    functions.when(functions.col("one_ahead_volume") > 0, functions.lit(1)).otherwise(lit(0)),
    functions.when(functions.col("two_ahead_volume") > 0, functions.lit(2)).otherwise(lit(0)),
    functions.when(functions.col("third_ahead_volume") > 0, functions.lit(3)).otherwise(lit(0)),
    functions.lit(0)  # Default value if no zero found
))
# above we essentialy added a three cols, 1 ahead vol, 2 ahead, and 3 ahead, then we make fourth 
# column that finds which one of those columns with priority to left is greater than 0
# then, below we drop all the three leading volume columns
df2_next_three_volumes.drop("one_ahead_volume", "two_ahead_volume", "third_ahead_volume")
'''

# more efficient way of finding which of three next rows have volume > 0 (else 0)
### NOTE: below if just a showing of how we use the array_position to find first
# occurence of zero volume value, below it is the actual code we want
'''
df_test1 = df2.withColumn(
    "next_zero_position",
    functions.when( # below we specify what each value of column to be
        functions.array_position(functions.array(
            lead("volume", 1).over(w),
            lead("volume", 2).over(w),
            lead("volume", 3).over(w)
        ), 0) > 0,
        functions.array_position(functions.array(
            lead("volume", 1).over(w),
            lead("volume", 2).over(w),
            lead("volume", 3).over(w)
        ), 0)  # Return the position of the first 0
    ).otherwise(0)  # Default to 0 if no zero is found
)
'''

# WANT:
'''
next_three_volumes_efficient = df2.withColumn(
    "next_zero_position",
    functions.when( # below we specify what each value of column to be
        functions.array_position(functions.array(
            functions.when(lead("volume", 1).over(w) > 0, lit(1)).otherwise(lit(0)),
            functions.when(lead("volume", 2).over(w) > 0, lit(1)).otherwise(lit(0)),
            functions.when(lead("volume", 3).over(w) > 0, lit(1)).otherwise(lit(0))
        ), 1) > 0,
        functions.array_position(functions.array(
            functions.when(lead("volume", 1).over(w) > 0, lit(1)).otherwise(lit(0)),
            functions.when(lead("volume", 2).over(w) > 0, lit(1)).otherwise(lit(0)),
            functions.when(lead("volume", 3).over(w) > 0, lit(1)).otherwise(lit(0))
        ), 1)  # Return the position of the first 0
    ).otherwise(lit(0))  # Default to 0 if no zero is found
)
# Show the result
# next_three_volumes_efficient.show()
'''

# Let's change it up a bit, lets add col where we say which of next three of open
# price increase more than 10% (otherwise 0)
'''
ten_percent_increase_open = df2.withColumn(
    "has_increase_when",
    functions.when(
        functions.array_position(functions.array(
            functions.when((lead("open", 1).over(w) - functions.col("open")) / functions.col("open") > 0.1, lit(1)).otherwise(lit(0)),
            functions.when((lead("open", 2).over(w) - functions.col("open")) / functions.col("open") > 0.1, lit(1)).otherwise(lit(0)),
            functions.when((lead("open", 3).over(w) - functions.col("open")) / functions.col("open") > 0.1, lit(1)).otherwise(lit(0))
        ), 1) > 0,
        functions.array_position(functions.array(
            functions.when((lead("open", 1).over(w) - functions.col("open")) / functions.col("open") > 0.1, lit(1)).otherwise(lit(0)),
            functions.when((lead("open", 2).over(w) - functions.col("open")) / functions.col("open") > 0.1, lit(1)).otherwise(lit(0)),
            functions.when((lead("open", 3).over(w) - functions.col("open")) / functions.col("open") > 0.1, lit(1)).otherwise(lit(0))
        ), 1)
    ).otherwise(0)
) 
'''

### OR EVEN QUICKER:
'''
ten_percent_increase_open = df2.withColumn(
    "has_increase_when",
    functions.array_position(functions.array(
            functions.when((lead("open", 1).over(w) - functions.col("open")) / functions.col("open") > 0.1, lit(1)).otherwise(lit(0)),
            functions.when((lead("open", 2).over(w) - functions.col("open")) / functions.col("open") > 0.1, lit(1)).otherwise(lit(0)),
            functions.when((lead("open", 3).over(w) - functions.col("open")) / functions.col("open") > 0.1, lit(1)).otherwise(lit(0))
        ), 1)
) 
'''


### LETS DO THE SAME AS ABOVE BUT LOOKING AT DYNAMIC VAR ROWS AHEAD (instead of just 3):
print("GOT IT!")
df3 = spark.read.csv("result1.csv", header=True, inferSchema=True) # first read csv into df



rows_to_look_ahead = 9
w = Window.orderBy("date")
array_upcoming_open = array(*[
    functions.when((lead("open", i).over(w) - col("open")) / col("open") > 0.001, lit(1)).otherwise(lit(0)) 
    for i in range(1, rows_to_look_ahead + 1)
])
ten_percent_increase_open_dynamic_look_ahead = df3.withColumn("has_increase_when", functions.array_position(array_upcoming_open, 1))
# NOTE: don't put COLs (literal, with lit()) in the array_position search, it takes values NOT column objects...

ten_percent_increase_open_dynamic_look_ahead.show()