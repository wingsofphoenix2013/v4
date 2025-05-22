import logging
import pandas as pd

# Получение и логирование входных данных для расчёта EMA
def ema_pandas(prices, period):
    """
    Расчёт EMA по массиву цен через pandas.Series.ewm (совпадает с TradingView).
    prices: list[float] — массив close-цен.
    period: int — период EMA.
    Возвращает последний ema_value (float).
    """
    if len(prices) < period:
        return None
    closes = pd.Series(prices)
    ema_series = closes.ewm(span=period, adjust=False).mean()
    return ema_series.iloc[-1]