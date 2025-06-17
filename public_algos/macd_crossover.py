def macd_crossover(df):
    import pandas as pd
    import numpy as np
   
    fast_span = 12
    slow_span = 26
    signal_span = 9
  
    ema_fast = df['close'].ewm(span=fast_span, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow_span, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal_span, adjust=False).mean()
    signals = []
    for i in range(len(df)):
        if i == 0:
            signals.append("Hold")
        else:
            # bullish MACD crossover
            if macd_line.iloc[i] > signal_line.iloc[i] and macd_line.iloc[i-1] <= signal_line.iloc[i-1]:
                signals.append("Buy")
            # bbearish MACD crossover
            elif macd_line.iloc[i] < signal_line.iloc[i] and macd_line.iloc[i-1] >= signal_line.iloc[i-1]:
                signals.append("Sell")
            else:
                signals.append("Hold")
    return signals
