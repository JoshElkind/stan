def moving_average_crossover(df):
    import pandas as pd
    import numpy as np
    short_window = 20  
    long_window = 50   
   
    short_ma = df['close'].rolling(window=short_window, min_periods=1).mean()
    long_ma = df['close'].rolling(window=long_window, min_periods=1).mean()
    signals = []
    for i in range(len(df)):
        if i == 0:
            signals.append("Hold")
        else:
            # bullish crossover: short MA crosses above long MA
            if short_ma[i] > long_ma[i] and short_ma[i-1] <= long_ma[i-1]:
                signals.append("Buy")
            # bearish crossover: short MA crosses below long MA
            elif short_ma[i] < long_ma[i] and short_ma[i-1] >= long_ma[i-1]:
                signals.append("Sell")
            else:
                signals.append("Hold")
    return signals
