def adx_trend(df):
    import pandas as pd
    import numpy as np
    from ta.trend import ADXIndicator
    
    adx_indicator = ADXIndicator(high=df['high'], low=df['low'], close=df['close'], window=14)
    adx_series = adx_indicator.adx()
    pos_di = adx_indicator.adx_pos()  
    neg_di = adx_indicator.adx_neg()  
    signals = []
    for i in range(len(df)):
        if i == 0:
            signals.append("Hold")
        else:
          
            if pd.isna(adx_series.iloc[i]) or pd.isna(pos_di.iloc[i]) or pd.isna(neg_di.iloc[i]):
                signals.append("Hold")
            elif adx_series.iloc[i] > 25:
             
                if pos_di.iloc[i] > neg_di.iloc[i] and pos_di.iloc[i-1] <= neg_di.iloc[i-1]:
                    signals.append("Buy")
              
                elif neg_di.iloc[i] > pos_di.iloc[i] and neg_di.iloc[i-1] <= pos_di.iloc[i-1]:
                    signals.append("Sell")
                else:
                    signals.append("Hold")
            else:
                
                signals.append("Hold")
    return signals
