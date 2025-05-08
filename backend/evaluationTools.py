import pandas as pd

### Test 1
# Simply start position on a "Buy", then sell at next "Sell"
# If multiple of same type of "Action" appear we implement stack 
# where we push Action and the Price of Buy/Sell
def test1(df, pl, gp, lp): # dataframe, position length (ticks), gain percentage, loss percentage
    total_percentage_change = 0
    stack = []
    length = len(df)
    for i in range(length):
        action = df["Action"].iloc[i]
        price = (df["Open"].iloc[i] + df["Close"].iloc[i]) / 2
        if (not stack):
            stack.append([action, price])
        elif (stack[-1][0] == "Buy" and action == "Sell"):
            change = price - stack[-1][1]
            p_change = change / stack[-1][1]
            total_percentage_change += p_change
        elif (stack[-1][0] == "Sell" and action == "Buy"):
            change = stack[-1][1] - price
            p_change = change / stack[-1][1]
            total_percentage_change += p_change
        else:
            stack.append([action, price])
    return total_percentage_change # returns the gain/loss percentage over all trades...


### Test 2
# Here we use the gp, lp to evaluate the algorithm better
def test2(df, pl, gp, lp):
    win_loss_setup_ratio = 0
    length = len(df)
    for i in range(length):
        action = df["Action"].iloc[i]
        price = (df["Open"].iloc[i] + df["Close"].iloc[i]) / 2
        if (action != "Hold"):
            start = i + 1
            while (start <= i + pl and start < length):
                start_price = (df["Open"].iloc[start] + df["Close"].iloc[start]) / 2
                if (action == "Buy" and (start_price - price)/price >= gp):
                    total_percentage_change += 1
                    break
                elif (action == "Buy" and (start_price - price)/price <= lp):
                    total_percentage_change -= 1
                    break
                elif (action == "Sell" and (price - start_price)/price >= gp):
                    total_percentage_change += 1
                    break
                elif (action == "Sell" and (price - start_price)/price <= lp):
                    total_percentage_change -= 1
                    break
                start += 1
    return win_loss_setup_ratio # returns whole number, # of wins minus # of loses in positions...