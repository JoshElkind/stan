def momentum_strategy(df):
    import pandas as pd
    import numpy as np
    window = 10           
    threshold = 0.01     
    signals = []
    for i in range(len(df)):
        if i < window:
            # not enough look-back data for momentum calculation
            signals.append("Hold")
        else:
          
            past_price = df['close'].iloc[i - window]
            current_price = df['close'].iloc[i]
            if past_price == 0:
                signals.append("Hold")
                continue
            change_pct = (current_price - past_price) / past_price
            if change_pct > threshold:
                signals.append("Buy")
            elif change_pct < -threshold:
                signals.append("Sell")
            else:
                signals.append("Hold")
    return signals
