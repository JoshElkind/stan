def bollinger_bands(df):
    import pandas as pd
    import numpy as np
    window = 20  # period for moving average and bands
    
    rolling_mean = df['close'].rolling(window=window, min_periods=window).mean()
    rolling_std = df['close'].rolling(window=window, min_periods=window).std()
    upper_band = rolling_mean + 2 * rolling_std
    lower_band = rolling_mean - 2 * rolling_std
    signals = []
    for i in range(len(df)):
        if i == 0:
            signals.append("Hold")
        else:
           
            if pd.isna(upper_band.iloc[i]) or pd.isna(lower_band.iloc[i]) or pd.isna(upper_band.iloc[i-1]) or pd.isna(lower_band.iloc[i-1]):
                signals.append("Hold")
          
            elif df['close'].iloc[i] >= lower_band.iloc[i] and df['close'].iloc[i-1] < lower_band.iloc[i-1]:
                signals.append("Buy")
       
            elif df['close'].iloc[i] <= upper_band.iloc[i] and df['close'].iloc[i-1] > upper_band.iloc[i-1]:
                signals.append("Sell")
            else:
                signals.append("Hold")
    return signals
