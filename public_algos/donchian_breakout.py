def donchian_breakout(df):
    import pandas as pd
    import numpy as np
    N = 20 
    signals = []
    for i in range(len(df)):
        if i < N:
            # if not enough data for a full window
            signals.append("Hold")
        else:
        
            past_high = df['high'].iloc[i-N:i].max()
            past_low = df['low'].iloc[i-N:i].min()
            if df['close'].iloc[i] > past_high:
                signals.append("Buy")
            elif df['close'].iloc[i] < past_low:
                signals.append("Sell")
            else:
                signals.append("Hold")
    return signals
