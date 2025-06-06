# indicators/bb.py
import pandas as pd

def compute(df: pd.DataFrame, params: dict) -> dict[str, float]:
    """
    Расчёт полос Боллинджера (Bollinger Bands) с использованием SMA.

    Параметры:
        length: окно скользящей средней (по умолчанию 20)
        std: множитель стандартного отклонения (по умолчанию 2.0)

    Возвращает:
        center (SMA), upper, lower
    """
    length = int(params.get("length", 20))
    std_mult = float(params.get("std", 2.0))

    close = df["c"].astype(float)

    sma = close.rolling(length).mean()
    std = close.rolling(length).std()

    center = sma.iloc[-1]
    upper = (sma + std_mult * std).iloc[-1]
    lower = (sma - std_mult * std).iloc[-1]

    return {
        "center": float(center),
        "upper": float(upper),
        "lower": float(lower)
    }