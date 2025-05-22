import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import Window
from pyspark.sql.functions import lag, lead, avg, max, coalesce, lit, sum, array, col, abs, when



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


def test3(df, pl, gp, lp):
    df = df.coalesce(1)
    # assume only has column date, column price, and column action (price = open + close / 2)
    w = Window.orderBy("date")
    # for Buy:
    buy_win_upcoming = array(*[
    functions.when((lead("price", i).over(w) - col("price")) / col("price") >= gp, lit(1)).otherwise(lit(0)) 
    for i in range(1, pl + 1)
    ])
    buy_loss_upcoming = array(*[
    functions.when((lead("price", i).over(w) - col("price") < 0) & (abs((lead("price", i).over(w) - col("price")) / col("price")) >= lp), lit(1)).otherwise(lit(0))
    for i in range(1, pl + 1)
    ])
    # for Sell:
    sell_win_upcoming = array(*[
    functions.when((col("price") - lead("price", i).over(w)) / col("price") >= gp, lit(1)).otherwise(lit(0)) 
    for i in range(1, pl + 1)
    ])
    sell_loss_upcoming = array(*[
    functions.when((col("price") - lead("price", i).over(w) < 0) & (abs((col("price") - lead("price", i).over(w)) / col("price")) >= lp), lit(1)).otherwise(lit(0))
    for i in range(1, pl + 1)
    ])

    df_win_loss_buy = df.withColumn(
        "win-loss-arr",
        functions.when(col("Action") == "Buy", lit(array(lit(functions.array_position(buy_win_upcoming, 1)), lit(functions.array_position(buy_loss_upcoming, 1))))). otherwise(lit(array(lit(0),lit(0))))
    )
    df_win_loss_buy = df_win_loss_buy.cache()
    df_win_loss_buy = df_win_loss_buy.repartition(16) # may change on AWS...
    df_win_loss_buy = df_win_loss_buy.drop("date", "price")
    df_win_loss_buy = df_win_loss_buy.filter(functions.col("Action") == "Buy")
    df_win_loss_buy = df_win_loss_buy.drop("Action")
    df_win_loss_buy = df_win_loss_buy.withColumn(
        "win-loss-arr", # 0 nothing, 1 loss, 2 win
        when((col("win-loss-arr")[0] == 0) & (col("win-loss-arr")[1] == 0), lit(0))
        .when(col("win-loss-arr")[0] == 0, lit(1))
        .when(col("win-loss-arr")[1] == 0, lit(2))
        .when(col("win-loss-arr")[0] < col("win-loss-arr")[1], lit(2))
        .otherwise(lit(1))
    )
    df_win_loss_buy = df_win_loss_buy.groupBy("win-loss-arr").agg(
        functions.count("*").alias("count")
    )
    df_win_loss_buy = df_win_loss_buy.coalesce(1)


    df_win_loss_sell = df.withColumn(
        "win-loss-arr",
        functions.when(col("Action") == "Sell", lit(array(lit(functions.array_position(sell_win_upcoming, 1)), lit(functions.array_position(sell_loss_upcoming, 1))))). otherwise(lit(array(lit(0),lit(0))))
    )
    df_win_loss_sell = df_win_loss_sell.cache()
    df_win_loss_sell = df_win_loss_sell.repartition(16)
    df_win_loss_sell= df_win_loss_sell.drop("date", "price")
    df_win_loss_sell = df_win_loss_sell.filter(functions.col("Action") == "Sell")
    df_win_loss_sell= df_win_loss_sell.drop("Action")
    df_win_loss_sell = df_win_loss_sell.withColumn(
        "win-loss-arr", # 0 nothing, 1 loss, 2 win
        when((col("win-loss-arr")[0] == 0) & (col("win-loss-arr")[1] == 0), lit(0))
        .when(col("win-loss-arr")[0] == 0, lit(1))
        .when(col("win-loss-arr")[1] == 0, lit(2))
        .when(col("win-loss-arr")[0] < col("win-loss-arr")[1], lit(2))
        .otherwise(lit(1))
    )
    df_win_loss_sell = df_win_loss_sell.groupBy("win-loss-arr").agg(
        functions.count("*").alias("count")
    )
    df_win_loss_sell = df_win_loss_sell.coalesce(1)

    win_buy = df_win_loss_buy.filter(col("win-loss-arr") == 2).select("count").collect()[0][0]
    loss_buy = df_win_loss_buy.filter(col("win-loss-arr") == 1).select("count").collect()[0][0]
    total_buy = df_win_loss_buy.filter(col("win-loss-arr") == 0).select("count").collect()[0][0] + win_buy + loss_buy
    win_sell = df_win_loss_sell.filter(col("win-loss-arr") == 2).select("count").collect()[0][0]
    loss_sell = df_win_loss_sell.filter(col("win-loss-arr") == 1).select("count").collect()[0][0]
    total_sell = df_win_loss_sell.filter(col("win-loss-arr") == 0).select("count").collect()[0][0] + win_sell + loss_sell
    arr_final = ([win_buy, loss_buy, win_sell, loss_sell, total_buy, total_sell])
    arr_final.insert(0, (((win_buy + win_sell) * (gp / lp) - (loss_buy + loss_sell) * (lp)) / (win_buy + win_sell + loss_buy + loss_sell)))
    return arr_final

spark = SparkSession.builder.appName("TradeSignalEvaluation_before").getOrCreate()

dataframe = pd.read_csv("result1.csv")

# spark:
# dataframe = spark.read.csv("result1.csv", header=True)
# print("START") 
# print(test2(dataframe, 2, 0.012, 0.005))

# pandas:
# dataframe = pd.read_csv("result1.csv")
