import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg, sum as spark_sum
from pyspark.sql.window import Window


### Test 1
# Simply start position on a "Buy", then sell at next "Sell"
# If multiple of same type of "Action" appear we implement stack 
# where we push Action and the Price of Buy/Sell
def test1(df, pl, gp, lp): # dataframe, position length (ticks), gain percentage, loss percentage
    total_percentage_change = 0
    buy_wins = 0
    buy_losses = 0
    sell_wins = 0
    sell_losses = 0
    stack = []
    length = len(df)
    for i in range(length):
        action = df["Action"].iloc[i]
        price = (df["open"].iloc[i] + df["close"].iloc[i]) / 2
        if (not stack):
            stack.append([action, price])
        elif (stack[-1][0] == "Buy" and action == "Sell"):
            change = price - stack[-1][1]
            p_change = change / stack[-1][1]
            total_percentage_change += float(p_change)
            if (p_change > 0):
                buy_wins += 1
            elif (p_change < 0):
                buy_losses += 1
            stack.pop()
        elif (stack[-1][0] == "Sell" and action == "Buy"):
            change = stack[-1][1] - price
            p_change = change / stack[-1][1]
            total_percentage_change += float(p_change)
            if (p_change > 0):
                sell_wins += 1
            elif (p_change < 0):
               sell_losses += 1
            stack.pop()
        else:
            stack.append([action, price])
    return [total_percentage_change, buy_wins, buy_losses, sell_wins, sell_losses] # returns the gain/loss percentage over all trades, number of positions won, number of positions lost


### Test 2
# Here we use the gp, lp to evaluate the algorithm better
def test2(df, pl, gp, lp):
    win_loss_setup_ratio = 0
    buy_wins = 0
    buy_losses = 0
    sell_wins = 0
    sell_losses = 0
    length = len(df)
    for i in range(length):
        action = df["Action"].iloc[i]
        price = (df["open"].iloc[i] + df["close"].iloc[i]) / 2
        if (action != "Hold"):
            start = i + 1
            while (start <= i + pl - 1 and start < length):
                start_price = (df["open"].iloc[start] + df["close"].iloc[start]) / 2
                # print(0)
                if (action == "Buy" and (start_price - price)/price >= gp):
                    win_loss_setup_ratio += gp
                    buy_wins += 1
                    # print(1)
                    break
                elif (action == "Buy" and (start_price - price) < 0 and abs((start_price - price)/price) >= lp):
                    win_loss_setup_ratio -= lp
                    buy_losses += 1
                    # print(2)
                    break
                elif (action == "Sell" and (price - start_price)/price >= gp):
                    win_loss_setup_ratio += gp
                    sell_wins += 1
                    # print(3)
                    break
                elif (action == "Sell" and (price - start_price) < 0 and abs(price - start_price)/price >= lp):
                    win_loss_setup_ratio -= lp
                    sell_losses += 1
                    # print(4)
                    break
                start += 1
    return [win_loss_setup_ratio/(buy_wins + buy_losses + sell_wins + sell_losses), buy_wins, buy_losses, sell_wins, sell_losses] # percent gain/loss total, number of won positions, and number of lost positions

### Test 3 (optimized with PySparl)
def test3(df, pl, gp, lp):
    spark = SparkSession.builder.appName("PySpark Tutorial").getOrCreate()
    df = spark.createDataFrame(df)
    # Calculate the average price
    df = df.withColumn("price", (col("open") + col("close")) / 2)

    # Calculate the next prices within the prediction length (pl) using a window
    windowed_df = df.withColumn("next_price", col("price").over(Window.rowsBetween(1, pl - 1)))

    # Calculate the price differences and percentage changes
    df = windowed_df.withColumn("price_diff", col("next_price") - col("price")) \
                    .withColumn("perc_change", col("price_diff") / col("price"))

    # Conditions for Buy and Sell wins and losses
    df = df.withColumn("win_loss",
                       when((col("Action") == "Buy") & (col("perc_change") >= gp), gp)
                       .when((col("Action") == "Buy") & (col("price_diff") < 0) & (abs(col("perc_change")) >= lp), -lp)
                       .when((col("Action") == "Sell") & (col("perc_change") <= -gp), gp)
                       .when((col("Action") == "Sell") & (col("price_diff") > 0) & (abs(col("perc_change")) >= lp), -lp)
                       .otherwise(0))

    # Counting wins and losses
    df = df.withColumn("buy_wins", when((col("Action") == "Buy") & (col("win_loss") == gp), 1).otherwise(0)) \
           .withColumn("buy_losses", when((col("Action") == "Buy") & (col("win_loss") == -lp), 1).otherwise(0)) \
           .withColumn("sell_wins", when((col("Action") == "Sell") & (col("win_loss") == gp), 1).otherwise(0)) \
           .withColumn("sell_losses", when((col("Action") == "Sell") & (col("win_loss") == -lp), 1).otherwise(0))

    # Aggregating the results
    result = df.agg(
        (spark_sum("win_loss") / (spark_sum("buy_wins") + spark_sum("buy_losses") + spark_sum("sell_wins") + spark_sum("sell_losses"))).alias("win_loss_ratio"),
        spark_sum("buy_wins").alias("buy_wins"),
        spark_sum("buy_losses").alias("buy_losses"),
        spark_sum("sell_wins").alias("sell_wins"),
        spark_sum("sell_losses").alias("sell_losses")
    )

    df = df.toPandas()
    return df



### NOTE: MUST CHANGE COLUMN NAMES NOT CAPITAL FIRST LETTER !!!
# dataframe = pd.read_csv("algorithms/results1.csv")
# print(test1(dataframe, 50, 0.012, 0.005)) 

### PAST TESTS:
### NOTE: test1 doesn't use pl, gp, lp in its evaluation!
# print(test1(dataframe, 50, 0.012, 0.005)): ([0.7464119780703932, 33, 2])
# print(test2(dataframe, 40, 0.1, 0.03)): ([0.5320000000000003, 46, 4])
