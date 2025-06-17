def rsi_mean_reversion(df):
    import pandas as pd
    import numpy as np
    from ta.momentum import RSIIndicator
    # 14-period RSI
    rsi_indicator = RSIIndicator(close=df['close'], window=14)
    rsi_series = rsi_indicator.rsi()
    signals = []
    for i in range(len(df)):
        if i == 0:
            signals.append("Hold")
        else: # RSI exits...
           
            if pd.isna(rsi_series.iloc[i]) or pd.isna(rsi_series.iloc[i-1]):
                signals.append("Hold")

            elif rsi_series.iloc[i] > 30 and rsi_series.iloc[i-1] <= 30:
                signals.append("Buy")
         
            elif rsi_series.iloc[i] < 70 and rsi_series.iloc[i-1] >= 70:
                signals.append("Sell")
            else:
                signals.append("Hold")
    return signals
