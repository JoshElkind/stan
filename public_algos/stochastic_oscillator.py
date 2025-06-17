def stochastic_oscillator(df):
    import pandas as pd
    import numpy as np
    window = 14   # look-back period for %K
    smooth = 3    # smoothing period for %D
    
    highest_high = df['high'].rolling(window=window, min_periods=window).max()
    lowest_low = df['low'].rolling(window=window, min_periods=window).min()
   
    percent_k = 100 * (df['close'] - lowest_low) / (highest_high - lowest_low)
    
    percent_d = percent_k.rolling(window=smooth, min_periods=smooth).mean()
    signals = []
    for i in range(len(df)):
        if i < window or i < smooth:
            signals.append("Hold")
        else:
           
            if pd.isna(percent_d.iloc[i]) or pd.isna(percent_d.iloc[i-1]):
                signals.append("Hold")
           
            elif percent_k.iloc[i] > percent_d.iloc[i] and percent_k.iloc[i-1] <= percent_d.iloc[i-1] and percent_k.iloc[i-1] < 20:
                signals.append("Buy")
           
            elif percent_k.iloc[i] < percent_d.iloc[i] and percent_k.iloc[i-1] >= percent_d.iloc[i-1] and percent_k.iloc[i-1] > 80:
                signals.append("Sell")
            else:
                signals.append("Hold")
    return signals
