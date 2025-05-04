import pandas as pd

def calculate_rsi(prices, n=14):
    """
    Calculate the Relative Strength Index (RSI) for a given price series. Based on Wilder's SMMA method.

    Args:
        prices (pd.Series): A pandas Series of prices (e.g., closing prices).
        n (int): The lookback period for RSI calculation (default is 14).

    Returns:
        pd.Series: A pandas Series containing the RSI values.

    Reference:
        wiki: https://en.wikipedia.org/wiki/Relative_strength_index
    """
    
    if prices.shape[0] < n:
        raise ValueError(f"Not enough data points to calculate RSI. Minimum required: {n}, Provided: {prices.shape[0]}")

    delta = prices.diff()
    prev_gain = delta[1:n].where(delta > 0, 0).mean()
    prev_loss = -delta[1:n].where(delta < 0, 0).mean()
    
    rsi_func = lambda gain, loss: 100 - (100 / (1 + (gain/ loss)))
    rsis = [pd.NA] * (n-1) + [rsi_func(prev_gain, prev_loss)]
    for delta_data in delta[n:].values:
        prev_gain = (prev_gain * (n-1) + max(delta_data, 0)) / n
        prev_loss = (prev_loss * (n-1) + -min(delta_data, 0)) / n
        
        rsi = rsi_func(prev_gain, prev_loss)
        rsis.append(rsi)
    return rsis

def calculate_sma(prices, n=30):
    """
    Calculate the Simple Moving Average (SMA) for a given price series.

    Args:
        prices (pd.Series): A pandas Series of prices (e.g., closing prices).
        n (int): The lookback period for SMA calculation (default is 30).

    Returns:
        pd.Series: A pandas Series containing the SMA values.
    """
    return prices.rolling(window=n).mean()