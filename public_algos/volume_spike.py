def volume_spike(df):
    import pandas as pd
    import numpy as np
    window = 20     
    factor = 2.0    
    signals = []
    for i in range(len(df)):
        if i == 0:
            signals.append("Hold")
        else:
          
            if i < window:
                avg_vol = df['volume'].iloc[:i].mean()
            else:
                avg_vol = df['volume'].iloc[i-window:i].mean()
          
            if avg_vol > 0 and df['volume'].iloc[i] > factor * avg_vol:
               
                if df['close'].iloc[i] > df['close'].iloc[i-1]:
                    signals.append("Buy")
                
                elif df['close'].iloc[i] < df['close'].iloc[i-1]:
                    signals.append("Sell")
                else:
                    signals.append("Hold")
            else:
                signals.append("Hold")
    return signals
