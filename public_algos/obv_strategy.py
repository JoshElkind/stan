def obv_strategy(df):
    import pandas as pd
    import numpy as np
  
    price_change = df['close'].diff().fillna(0)
    direction = np.sign(price_change)  
    obv = (direction * df['volume']).cumsum().fillna(0)
 
    ma_window = 10
    obv_ma = obv.rolling(window=ma_window, min_periods=ma_window).mean()
    signals = []
    for i in range(len(df)):
        if i < ma_window:
            signals.append("Hold")
        else:
            
            if obv.iloc[i] > obv_ma.iloc[i] and obv.iloc[i-1] <= obv_ma.iloc[i-1]:
                signals.append("Buy")
       
            elif obv.iloc[i] < obv_ma.iloc[i] and obv.iloc[i-1] >= obv_ma.iloc[i-1]:
                signals.append("Sell")
            else:
                signals.append("Hold")
    return signals
