import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, DateTime 
from sqlalchemy.orm import declarative_base, sessionmaker, relationship 
from datetime import datetime
from sqlalchemy import asc, desc, text

def pivot_atr_algorithm(df):
    """
    Process stock data to detect pivots and calculate ATR.

    Parameters:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to save the output CSV file.
        atr_length (int): ATR lookback period. Default is 14.
        pivot_len (int): Pivot detection length (bars to the left and right). Default is 5.
        
    """

    atr_length = 14
    pivot_len = 5
    # Ensure Date column is datetime type and sort by Date
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # 2. Calculate True Range (TR) for each row
    high = df['high']
    low = df['low']
    close = df['close']
    prev_close = close.shift(1)

    # True Range calculation
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # 3. Calculate ATR using Wilder's smoothing
    atr = [None] * len(df)
    if len(df) >= atr_length:
        initial_atr = df['TR'].iloc[0:atr_length].mean()
        atr[atr_length - 1] = initial_atr
        for i in range(atr_length, len(df)):
            prev_atr = atr[i - 1]
            current_tr = df.at[i, 'TR']
            atr_val = (prev_atr * (atr_length - 1) + current_tr) / atr_length
            atr[i] = atr_val

    # Add ATR to DataFrame
    df['ATR'] = atr

    # 4. Identify pivot highs and pivot lows
    actions = ['Hold'] * len(df)
    for i in range(len(df)):
        # Skip bars that cannot be confirmed as pivots
        if i < pivot_len or i > len(df) - pivot_len - 1:
            continue

        # Determine if bar i is a pivot high
        current_high = df.at[i, 'high']
        left_window_highs = df['high'].iloc[i - pivot_len: i]
        right_window_highs = df['high'].iloc[i + 1: i + 1 + pivot_len]
        if current_high > left_window_highs.max() and current_high >= right_window_highs.max():
            actions[i] = 'Sell'

        # Determine if bar i is a pivot low
        current_low = df.at[i, 'low']
        left_window_lows = df['low'].iloc[i - pivot_len: i]
        right_window_lows = df['low'].iloc[i + 1: i + 1 + pivot_len]
        if current_low < left_window_lows.min() and current_low <= right_window_lows.min():
            actions[i] = 'Buy'

    # 5. Assign actions to a new column in DataFrame
    df['Action'] = actions

    # 6. Save the DataFrame to the output file
    return df

import pandas as pd

def bollinger_bands_minute_strategy(df):
    """
    Apply Bollinger Bands strategy for 1-minute interval stock data.

    Parameters:
        csv_path (str): Path to the input CSV file.
        period (int): Number of minutes for the moving average and standard deviation. Default is 20.
        num_std_dev (int): Number of standard deviations for the Bollinger Bands. Default is 2.

    Returns:
        pd.DataFrame: DataFrame with an additional 'Action' column.
    """
    period=150
    num_std_dev=2

    # Ensure the Date column is datetime type and sort by date
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Calculate the Moving Average (MA) and Standard Deviation (SD) over the specified period
    df['MA'] = df['close'].rolling(window=period).mean()
    df['SD'] = df['close'].rolling(window=period).std()

    # Calculate the Upper and Lower Bollinger Bands
    df['Upper Band'] = df['MA'] + (df['SD'] * num_std_dev)
    df['Lower Band'] = df['MA'] - (df['SD'] * num_std_dev)

    # Initialize the Action column with 'Hold'
    df['Action'] = 'Hold'

    # Determine Buy, Sell, or Hold based on Bollinger Bands
    for i in range(len(df)):
        if i < period:
            continue  # Skip rows without sufficient data

        close = df.at[i, 'close']
        upper_band = df.at[i, 'Upper Band']
        lower_band = df.at[i, 'Lower Band']

        if close < lower_band:
            df.at[i, 'Action'] = 'Buy'
        elif close > upper_band:
            df.at[i, 'Action'] = 'Sell'
        else:
            df.at[i, 'Action'] = 'Hold'

    # Drop intermediate calculation columns to keep the output clean
    df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'Action']]

    return df


import pandas as pd
import numpy as np

def macd_vwap_strategy(df):
    """
    Combines MACD Scalping and VWAP Reversion strategies to determine Buy, Sell, Hold signals.

    Parameters:
        csv_path (str): Path to the input CSV file.

    Returns:
        pd.DataFrame: DataFrame with an additional 'Action' column.
    """
    # MACD and VWAP Parameters
    # Optimized MACD and VWAP Parameters for 15-minute trading
    short_window = 9    # Short-term EMA period for MACD
    long_window = 21    # Long-term EMA period for MACD
    signal_window = 6   # Signal line period for MACD
    vwap_period = 15    # VWAP calculation period (15 minutes)
    vwap_threshold = 0.2  # VWAP deviation threshold (20%) # Threshold for deviation from VWAP

    # Load the CSV file

    # Ensure Date column is datetime type and sort by date
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # 1. Calculate MACD and Signal Line
    df['EMA12'] = df['close'].ewm(span=short_window, adjust=False).mean()
    df['EMA26'] = df['close'].ewm(span=long_window, adjust=False).mean()
    df['MACD'] = df['EMA12'] - df['EMA26']
    df['Signal Line'] = df['MACD'].ewm(span=signal_window, adjust=False).mean()

    # 2. Calculate VWAP
    df['Typical Price'] = (df['high'] + df['low'] + df['close']) / 3
    df['TPxVolume'] = df['Typical Price'] * df['volume']
    df['Cumulative TPxVolume'] = df['TPxVolume'].cumsum()
    df['Cumulative Volume'] = df['volume'].cumsum()
    df['VWAP'] = df['Cumulative TPxVolume'] / df['Cumulative Volume']

    # Initialize the Action column with 'Hold'
    df['Action'] = 'Hold'

    # 3. Combined Signal Logic
    for i in range(len(df)):
        if i < max(long_window, signal_window):
            continue  # Skip rows without sufficient data

        macd = df.at[i, 'MACD']
        signal = df.at[i, 'Signal Line']
        close_price = df.at[i, 'close']
        vwap = df.at[i, 'VWAP']

        # MACD Signal Conditions
        macd_buy = macd > signal and macd > 0
        macd_sell = macd < signal and macd < 0

        # VWAP Signal Conditions
        vwap_buy = close_price < vwap * (1 - vwap_threshold)
        vwap_sell = close_price > vwap * (1 + vwap_threshold)

        # Combined Buy Signal
        if macd_buy and vwap_buy:
            df.at[i, 'Action'] = 'Buy'
        # Combined Sell Signal
        elif macd_sell and vwap_sell:
            df.at[i, 'Action'] = 'Sell'
        # Hold if no clear signal
        else:
            df.at[i, 'Action'] = 'Hold'

    # Clean up intermediate columns
    df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'VWAP', 'MACD', 'Signal Line', 'Action']]
    
    return df


def vwap_pullback_strategy(df):
    """
    VWAP Pullback Strategy to determine Buy, Sell, Hold signals.

    Parameters:
        csv_path (str): Path to the input CSV file.
        vwap_period (int): Period for VWAP calculation (default is 20).
        vwap_threshold (float): Deviation threshold from VWAP (default is 0.01 or 1%).

    Returns:
        pd.DataFrame: DataFrame with an additional 'Action' column.
    """

    vwap_period=20
    vwap_threshold=0.01

    # Load the CSV file

    # Ensure Date column is datetime type and sort by date
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Calculate the Typical Price for VWAP calculation
    df['Typical Price'] = (df['high'] + df['low'] + df['close']) / 3

    # Calculate Cumulative TP * Volume and Cumulative Volume
    df['TPxVolume'] = df['Typical Price'] * df['volume']
    df['Cumulative TPxVolume'] = df['TPxVolume'].cumsum()
    df['Cumulative Volume'] = df['volume'].cumsum()

    # Calculate VWAP
    df['VWAP'] = df['Cumulative TPxVolume'] / df['Cumulative Volume']

    # Initialize the Action column with 'Hold'
    df['Action'] = 'Hold'

    # Generate Buy, Sell, Hold signals based on VWAP deviation
    for i in range(len(df)):
        if i < vwap_period:
            continue  # Skip rows without sufficient data
        
        close_price = df.at[i, 'close']
        vwap = df.at[i, 'VWAP']

        # Calculate deviation from VWAP
        deviation = (close_price - vwap) / vwap

        # Buy signal: Price significantly below VWAP
        if deviation <= -vwap_threshold:
            df.at[i, 'Action'] = 'Buy'
        # Sell signal: Price significantly above VWAP
        elif deviation >= vwap_threshold:
            df.at[i, 'Action'] = 'Sell'
        # Hold if within the threshold
        else:
            df.at[i, 'Action'] = 'Hold'

    # Drop intermediate columns for clean output
    df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'VWAP', 'Action']]

    return df

def momentum_strategy(df):
    df = df.copy()  # work on a copy
    # Compute short-term and long-term moving averages (e.g. 5-period and 20-period)
    short_window = 5
    long_window = 20
    df['short_ma'] = df['close'].rolling(window=short_window).mean()
    df['long_ma'] = df['close'].rolling(window=long_window).mean()
    # Initialize Action as "Hold"
    df['Action'] = 'Hold'
    # Generate Buy signal when short MA crosses above long MA, Sell when it crosses below
    buy_signal = (df['short_ma'] > df['long_ma']) & (df['short_ma'].shift(1) <= df['long_ma'].shift(1))
    sell_signal = (df['short_ma'] < df['long_ma']) & (df['short_ma'].shift(1) >= df['long_ma'].shift(1))
    df.loc[buy_signal, 'Action'] = 'Buy'
    df.loc[sell_signal, 'Action'] = 'Sell'
    # Return only the required columns
    return df[['date', 'open', 'high', 'low', 'close', 'volume', 'Action']]

def mean_reversion_strategy(df):
    df = df.copy()
    window = 20  # look-back period for mean and std deviation
    # Compute Bollinger Bands (20-period SMA ± 2 standard deviations)
    df['mid'] = df['close'].rolling(window=window).mean()
    df['std'] = df['close'].rolling(window=window).std()
    df['upper'] = df['mid'] + 2 * df['std']
    df['lower'] = df['mid'] - 2 * df['std']
    df['Action'] = 'Hold'
    # Buy signal: price crosses below lower band (oversold); Sell: price crosses above upper band (overbought)
    buy_signal = (df['close'] < df['lower']) & (df['close'].shift(1) >= df['lower'].shift(1))
    sell_signal = (df['close'] > df['upper']) & (df['close'].shift(1) <= df['upper'].shift(1))
    df.loc[buy_signal, 'Action'] = 'Buy'
    df.loc[sell_signal, 'Action'] = 'Sell'
    return df[['date', 'open', 'high', 'low', 'close', 'volume', 'Action']]

def breakout_strategy(df):
    df = df.copy()
    window = 60  # look-back window (e.g. past 60 minutes)
    # Calculate recent high and low (previous window's max high and min low)
    df['recent_high'] = df['high'].rolling(window=window, min_periods=window).max().shift(1)
    df['recent_low'] = df['low'].rolling(window=window, min_periods=window).min().shift(1)
    df['Action'] = 'Hold'
    # Buy if current close breaks above the recent high; Sell if breaks below recent low
    df.loc[df['close'] > df['recent_high'], 'Action'] = 'Buy'
    df.loc[df['close'] < df['recent_low'], 'Action'] = 'Sell'
    return df[['date', 'open', 'high', 'low', 'close', 'volume', 'Action']]


def hft_pattern_strategy(df):
    df = df.copy()
    # Calculate one-period (1-minute) returns
    df['return'] = df['close'].pct_change()
    # Estimate short-term volatility (rolling standard deviation of returns)
    window = 10  # e.g., 10-minute window for volatility
    df['volatility'] = df['return'].rolling(window=window).std()
    df['Action'] = 'Hold'
    # If the return is greater than +3σ (a rare upward spike) => Sell signal (price likely to mean-revert down)
    # If return is less than -3σ (sharp drop) => Buy signal (mean-revert up expected)
    df.loc[df['return'] > 3 * df['volatility'], 'Action'] = 'Sell'
    df.loc[df['return'] < -3 * df['volatility'], 'Action'] = 'Buy'
    return df[['date', 'open', 'high', 'low', 'close', 'volume', 'Action']]

def candlestick_pattern_strategy(df):
    df = df.copy()
    # Previous period's open and close for pattern comparison
    prev_open = df['open'].shift(1)
    prev_close = df['close'].shift(1)
    # Identify bullish engulfing: prior candle red, current candle green, and current body engulfs prior body
    bullish_engulf = (prev_close < prev_open) & (df['close'] > df['open']) & \
                     (df['close'] >= prev_open) & (df['open'] <= prev_close)
    # Identify bearish engulfing: prior candle green, current candle red, and current body engulfs prior body
    bearish_engulf = (prev_close > prev_open) & (df['close'] < df['open']) & \
                     (df['close'] <= prev_open) & (df['open'] >= prev_close)
    df['Action'] = 'Hold'
    df.loc[bullish_engulf, 'Action'] = 'Buy'
    df.loc[bearish_engulf, 'Action'] = 'Sell'
    return df[['date', 'open', 'high', 'low', 'close', 'volume', 'Action']]





# Example usage:
# result_df = macd_vwap_strategy("path_to_your_1min_data.csv")
# print(result_df.head())

db_url = 'postgresql://postgres@localhost:5432/stock-data' 
engine = create_engine(db_url)  

query = text(f'SELECT * FROM "{("1min_BTC")}"') # add each asset of that type
df_datacore = (pd.read_sql(query, engine))
df_datacore['date'] = pd.to_datetime(df_datacore['date'])
df_datacore = df_datacore.sort_values(by='date', ascending=True)

df = df_datacore

df = bollinger_bands_minute_strategy(df)
df.to_csv("result1.csv", index=False)

# Example usage
# result_df = bollinger_bands_minute_strategy("path_to_your_1min_data.csv")
# print(result_df.head())


# Example usage:
### pivot_atr_algorithm("testalgo1.csv", "results1.csv")
