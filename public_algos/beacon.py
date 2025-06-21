import numpy as np
import pandas as pd

def custom_upload_strategy_1(df, ema_period=20, atr_period=10, multiplier=2.0):
    # Calculate EMA of close prices
    ema = df['close'].ewm(span=ema_period).mean()
    
    # Calculate True Range and ATR
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    true_range = np.maximum(high_low, np.maximum(high_close, low_close))
    atr = true_range.rolling(atr_period).mean()
    
    # Calculate Keltner Channel bands
    upper_band = ema + (multiplier * atr)
    lower_band = ema - (multiplier * atr)
    
    signals = ['Hold'] * len(df)
    
    for i in range(1, len(df)):
        # Buy when price breaks above upper band (breakout)
        if df['close'].iloc[i-1] <= upper_band.iloc[i-1] and df['close'].iloc[i] > upper_band.iloc[i]:
            signals[i] = 'Buy'
        # Sell when price breaks below lower band (breakdown)
        elif df['close'].iloc[i-1] >= lower_band.iloc[i-1] and df['close'].iloc[i] < lower_band.iloc[i]:
            signals[i] = 'Sell'
    
    return signals